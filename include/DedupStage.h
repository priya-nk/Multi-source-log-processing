#pragma once

#include "BoundedLockFreeQueue.h"
#include "LogEntry.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

class DedupStage {
public:
    static constexpr std::size_t kShardCount = 16;
    using Clock = std::chrono::steady_clock;

    DedupStage(BoundedLockFreeQueue<LogEntry>& queue2,
               BoundedLockFreeQueue<LogEntry>& queue3,
               std::size_t worker_count,
               std::chrono::milliseconds active_window,
               std::size_t max_entries_per_shard = 100000);
    ~DedupStage();

    DedupStage(const DedupStage&) = delete;
    DedupStage& operator=(const DedupStage&) = delete;

    void start();
    // Stop upstream producers first. stop() closes Queue 2, drains its queued
    // records, joins workers, then the final worker closes Queue 3.
    void stop();

    std::uint64_t duplicates_dropped() const noexcept {
        return duplicates_dropped_.load(std::memory_order_relaxed);
    }

    static std::uint64_t fingerprint(const LogEntry& entry) noexcept;

private:
    struct Shard {
        std::mutex mutex;
        std::unordered_set<std::uint64_t> fingerprints;
        std::deque<std::pair<std::uint64_t, Clock::time_point>> insertion_order;
    };

    bool first_seen(std::uint64_t fingerprint);
    void worker_loop();

    BoundedLockFreeQueue<LogEntry>& queue2_;
    BoundedLockFreeQueue<LogEntry>& queue3_;
    const std::size_t worker_count_;
    const std::chrono::milliseconds active_window_;
    const std::size_t max_entries_per_shard_;
    std::array<Shard, kShardCount> shards_;
    std::atomic<std::uint64_t> duplicates_dropped_{0};
    std::atomic<std::size_t> remaining_workers_{0};
    std::atomic<bool> started_{false};
    std::vector<std::thread> workers_;
};

