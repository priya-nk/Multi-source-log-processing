#pragma once

#include <atomic>
#include <condition_variable>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>

// A bounded MPMC ring buffer based on per-slot sequence numbers. The queue's
// data path is lock-free; condition variables are used only when a caller must
// sleep because the queue is full or empty.
//
// close() rejects later pushes, wakes blocked callers, and lets pop() drain
// already queued values before returning std::nullopt. Do not destroy the
// queue while other threads are using it.6
template <class T>
class BoundedLockFreeQueue {
    static_assert(std::is_nothrow_move_constructible_v<T>,
                  "T must be nothrow move constructible");
    static_assert(std::is_nothrow_destructible_v<T>,
                  "T must be nothrow destructible");

    struct Cell {
        std::atomic<std::size_t> sequence{0};
        alignas(T) unsigned char storage[sizeof(T)];

        T* value() noexcept {
            return std::launder(reinterpret_cast<T*>(storage));
        }
    };

public:
    explicit BoundedLockFreeQueue(std::size_t capacity)
        : capacity_(capacity), cells_(capacity ? new Cell[capacity] : nullptr) {
        if (capacity_ == 0) {
            throw std::invalid_argument("queue capacity must be greater than zero");
        }
        for (std::size_t i = 0; i < capacity_; ++i) {
            cells_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    BoundedLockFreeQueue(const BoundedLockFreeQueue&) = delete;
    BoundedLockFreeQueue& operator=(const BoundedLockFreeQueue&) = delete;

    ~BoundedLockFreeQueue() {
        // Destruction requires external quiescence.
        while (try_pop_discard()) {}
    }

    // Blocks while full. Returns false if close() has begun before this push
    // can claim a slot. The by-value parameter also supports move-only T.
    bool push(T value) {
        if (closed_.load(std::memory_order_acquire)) return false;
        if (try_push(std::move(value))) {
            notify_data_waiter();
            return true;
        }

        std::unique_lock<std::mutex> lock(space_mutex_);
        space_waiters_.fetch_add(1, std::memory_order_release);
        for (;;) {
            if (closed_.load(std::memory_order_acquire)) {
                space_waiters_.fetch_sub(1, std::memory_order_release);
                return false;
            }
            if (try_push(std::move(value))) {
                space_waiters_.fetch_sub(1, std::memory_order_release);
                lock.unlock();
                notify_data_waiter();
                return true;
            }
            space_cv_.wait(lock);
        }
    }

    // Blocks while empty. After close(), queued items remain available; once
    // drained, pop() returns std::nullopt.
    std::optional<T> pop() {
        if (auto value = try_pop()) {
            notify_space_waiter();
            return value;
        }
        if (closed_.load(std::memory_order_acquire)) return std::nullopt;

        std::unique_lock<std::mutex> lock(data_mutex_);
        data_waiters_.fetch_add(1, std::memory_order_release);
        for (;;) {
            if (auto value = try_pop()) {
                data_waiters_.fetch_sub(1, std::memory_order_release);
                lock.unlock();
                notify_space_waiter();
                return value;
            }
            if (closed_.load(std::memory_order_acquire)) {
                data_waiters_.fetch_sub(1, std::memory_order_release);
                return std::nullopt;
            }
            data_cv_.wait(lock);
        }
    }

    template <class Rep, class Period>
    std::optional<T> pop_for(const std::chrono::duration<Rep, Period>& timeout) {
        if (auto value = try_pop()) {
            notify_space_waiter();
            return value;
        }
        if (closed_.load(std::memory_order_acquire)) return std::nullopt;

        std::unique_lock<std::mutex> lock(data_mutex_);
        data_waiters_.fetch_add(1, std::memory_order_release);
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        for (;;) {
            if (auto value = try_pop()) {
                data_waiters_.fetch_sub(1, std::memory_order_release);
                lock.unlock();
                notify_space_waiter();
                return value;
            }
            if (closed_.load(std::memory_order_acquire)) {
                data_waiters_.fetch_sub(1, std::memory_order_release);
                return std::nullopt;
            }
            if (data_cv_.wait_until(lock, deadline) == std::cv_status::timeout &&
                std::chrono::steady_clock::now() >= deadline) {
                // One final queue check closes the timeout/producer race.
                if (auto value = try_pop()) {
                    data_waiters_.fetch_sub(1, std::memory_order_release);
                    lock.unlock();
                    notify_space_waiter();
                    return value;
                }
                data_waiters_.fetch_sub(1, std::memory_order_release);
                return std::nullopt;
            }
        }
    }

    void close() noexcept {
        closed_.store(true, std::memory_order_release);
        // Pair with the waiter's mutex/recheck sequence to avoid lost wakeups.
        {
            std::lock_guard<std::mutex> lock(data_mutex_);
            data_cv_.notify_all();
        }
        {
            std::lock_guard<std::mutex> lock(space_mutex_);
            space_cv_.notify_all();
        }
    }

    bool is_closed() const noexcept {
        return closed_.load(std::memory_order_acquire);
    }

    std::size_t capacity() const noexcept { return capacity_; }

private:
    bool try_push(T&& value) noexcept {
        std::size_t pos = enqueue_pos_.load(std::memory_order_relaxed);
        Cell* cell;
        for (;;) {
            cell = &cells_[pos % capacity_];
            const std::size_t seq = cell->sequence.load(std::memory_order_acquire);
            const auto diff = static_cast<std::intptr_t>(seq) -
                              static_cast<std::intptr_t>(pos);
            if (diff == 0) {
                if (enqueue_pos_.compare_exchange_weak(
                        pos, pos + 1, std::memory_order_relaxed)) break;
            } else if (diff < 0) {
                return false;
            } else {
                pos = enqueue_pos_.load(std::memory_order_relaxed);
            }
        }
        ::new (static_cast<void*>(cell->storage)) T(std::move(value));
        cell->sequence.store(pos + 1, std::memory_order_release);
        return true;
    }

    std::optional<T> try_pop() noexcept {
        std::size_t pos = dequeue_pos_.load(std::memory_order_relaxed);
        Cell* cell;
        for (;;) {
            cell = &cells_[pos % capacity_];
            const std::size_t seq = cell->sequence.load(std::memory_order_acquire);
            const auto diff = static_cast<std::intptr_t>(seq) -
                              static_cast<std::intptr_t>(pos + 1);
            if (diff == 0) {
                if (dequeue_pos_.compare_exchange_weak(
                        pos, pos + 1, std::memory_order_relaxed)) break;
            } else if (diff < 0) {
                return std::nullopt;
            } else {
                pos = dequeue_pos_.load(std::memory_order_relaxed);
            }
        }
        std::optional<T> result(std::in_place, std::move(*cell->value()));
        cell->value()->~T();
        cell->sequence.store(pos + capacity_, std::memory_order_release);
        return result;
    }

    bool try_pop_discard() noexcept {
        auto value = try_pop();
        return value.has_value();
    }

    void notify_data_waiter() noexcept {
        if (data_waiters_.load(std::memory_order_acquire) == 0) return;
        std::lock_guard<std::mutex> lock(data_mutex_);
        data_cv_.notify_one();
    }

    void notify_space_waiter() noexcept {
        if (space_waiters_.load(std::memory_order_acquire) == 0) return;
        std::lock_guard<std::mutex> lock(space_mutex_);
        space_cv_.notify_one();
    }

    const std::size_t capacity_;
    std::unique_ptr<Cell[]> cells_;
    alignas(64) std::atomic<std::size_t> enqueue_pos_{0};
    alignas(64) std::atomic<std::size_t> dequeue_pos_{0};
    std::atomic<bool> closed_{false};

    std::mutex data_mutex_;
    std::condition_variable data_cv_;
    std::atomic<std::size_t> data_waiters_{0};
    std::mutex space_mutex_;
    std::condition_variable space_cv_;
    std::atomic<std::size_t> space_waiters_{0};
};

