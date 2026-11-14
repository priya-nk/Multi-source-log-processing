#include "DedupStage.h"

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <thread>

namespace {
constexpr std::uint64_t kMurmurC1 = 0x87c37b91114253d5ULL;
constexpr std::uint64_t kMurmurC2 = 0x4cf5ad432745937fULL;

std::uint64_t rotl64(std::uint64_t value, int shift) noexcept {
    return (value << shift) | (value >> (64 - shift));
}

// Produces avalanche effect
std::uint64_t fmix64(std::uint64_t value) noexcept {
    value ^= value >> 33;
    value *= 0xff51afd7ed558ccdULL;
    value ^= value >> 33;
    value *= 0xc4ceb9fe1a85ec53ULL;
    value ^= value >> 33;
    return value;
}

std::uint64_t load_le64(const unsigned char* p) noexcept {
    std::uint64_t value = 0;
    for (unsigned i = 0; i < 8; ++i) value |= std::uint64_t(p[i]) << (8 * i);
    return value;
}

// MurmurHash3 x64 128-bit variant; the pipeline uses h1 as the 64-bit
// fingerprint. Length prefixes make the composite key unambiguous.
std::uint64_t murmur3_64(const unsigned char* data, std::size_t length,
                         std::uint32_t seed = 0) noexcept {
    const std::size_t blocks = length / 16;
    std::uint64_t h1 = seed, h2 = seed;
    for (std::size_t i = 0; i < blocks; ++i) {
        std::uint64_t k1 = load_le64(data + i * 16);
        std::uint64_t k2 = load_le64(data + i * 16 + 8);
        k1 *= kMurmurC1; 
        k1 = rotl64(k1, 31); 
        k1 *= kMurmurC2; 
        h1 ^= k1;
        h1 = rotl64(h1, 27); 
        h1 += h2; 
        h1 = h1 * 5 + 0x52dce729;
        
        k2 *= kMurmurC2; 
        k2 = rotl64(k2, 33); 
        k2 *= kMurmurC1; 
        h2 ^= k2;
        h2 = rotl64(h2, 31); 
        h2 += h1; 
        h2 = h2 * 5 + 0x38495ab5;
    }

    const unsigned char* tail = data + blocks * 16;
    const std::size_t remaining = length & 15;
    std::uint64_t k1 = 0, k2 = 0;
    if (remaining > 8) {
        for (std::size_t i = 8; i < remaining; ++i) k2 |= std::uint64_t(tail[i]) << (8 * (i - 8));
        k2 *= kMurmurC2; k2 = rotl64(k2, 33); k2 *= kMurmurC1; h2 ^= k2;
    }
    if (remaining > 0) {
        const std::size_t first_part = std::min<std::size_t>(remaining, 8);
        for (std::size_t i = 0; i < first_part; ++i) k1 |= std::uint64_t(tail[i]) << (8 * i);
        k1 *= kMurmurC1; k1 = rotl64(k1, 31); k1 *= kMurmurC2; h1 ^= k1;
    }

    h1 ^= length; h2 ^= length;
    h1 += h2; h2 += h1;
    h1 = fmix64(h1); h2 = fmix64(h2);
    h1 += h2;
    return h1;
}

template <std::size_t N>
std::size_t field_length(const char (&field)[N]) noexcept {
    const auto* end = static_cast<const char*>(std::memchr(field, '\0', N));
    return end ? static_cast<std::size_t>(end - field) : N;
}
} // namespace

DedupStage::DedupStage(BoundedLockFreeQueue<LogEntry>& queue2,
                       BoundedLockFreeQueue<LogEntry>& queue3,
                       std::size_t worker_count,
                       std::chrono::milliseconds active_window,
                       std::size_t max_entries_per_shard)
    : queue2_(queue2), queue3_(queue3), worker_count_(worker_count),
      active_window_(active_window), max_entries_per_shard_(max_entries_per_shard) {
    if (worker_count_ == 0) throw std::invalid_argument("worker_count must be greater than zero");
    if (active_window_.count() <= 0) throw std::invalid_argument("active_window must be positive");
    if (max_entries_per_shard_ == 0) throw std::invalid_argument("max_entries_per_shard must be greater than zero");
}

DedupStage::~DedupStage() { stop(); }

void DedupStage::start() {
    bool expected = false;
    if (!started_.compare_exchange_strong(expected, true)) throw std::logic_error("DedupStage can only be started once");
    remaining_workers_.store(worker_count_, std::memory_order_release);
    try {
        workers_.reserve(worker_count_);
        for (std::size_t i = 0; i < worker_count_; ++i) workers_.emplace_back(&DedupStage::worker_loop, this);
    } catch (...) {
        queue2_.close();
        for (auto& worker : workers_) if (worker.joinable()) worker.join();
        queue3_.close();
        started_.store(false, std::memory_order_release);
        throw;
    }
}

void DedupStage::stop() {
    if (!started_.exchange(false, std::memory_order_acq_rel)) return;
    queue2_.close();
    for (auto& worker : workers_) if (worker.joinable()) worker.join();
    workers_.clear();
}

std::uint64_t DedupStage::fingerprint(const LogEntry& entry) noexcept {
    // [len32][timestamp][len32][host][len32][message]
    unsigned char key[4 + sizeof(entry.timestamp) + 4 + sizeof(entry.host_source) +
                      4 + sizeof(entry.message)];
    std::size_t offset = 0;
    auto append = [&](const char* value, std::size_t length) {
        const auto n = static_cast<std::uint32_t>(length);
        key[offset++] = static_cast<unsigned char>(n);
        key[offset++] = static_cast<unsigned char>(n >> 8);
        key[offset++] = static_cast<unsigned char>(n >> 16);
        key[offset++] = static_cast<unsigned char>(n >> 24);
        std::memcpy(key + offset, value, length);
        offset += length;
    };
    append(entry.timestamp, field_length(entry.timestamp));
    append(entry.host_source, field_length(entry.host_source));
    append(entry.message, field_length(entry.message));
    return murmur3_64(key, offset);
}

bool DedupStage::first_seen(std::uint64_t value) {
    Shard& shard = shards_[value % kShardCount];
    for (;;) {
        const auto now = Clock::now();
        std::unique_lock<std::mutex> lock(shard.mutex);
        while (!shard.insertion_order.empty() &&
               shard.insertion_order.front().second + active_window_ <= now) {
            const auto expired = shard.insertion_order.front().first;
            shard.insertion_order.pop_front();
            shard.fingerprints.erase(expired);
        }

        if (shard.fingerprints.find(value) != shard.fingerprints.end()) return false;
        if (shard.fingerprints.size() < max_entries_per_shard_) {
            shard.fingerprints.insert(value);
            shard.insertion_order.emplace_back(value, now);
            return true;
        }

        // Keep every fingerprint for the full requested window. If this shard
        // reaches its configured bound, apply backpressure until its oldest
        // entry expires instead of evicting a still-active fingerprint.
        const auto available = shard.insertion_order.front().second + active_window_;
        lock.unlock();
        std::this_thread::sleep_until(available);
    }
}

void DedupStage::worker_loop() {
    while (auto record = queue2_.pop()) {
        const auto value = fingerprint(*record);
        if (!first_seen(value)) {
            duplicates_dropped_.fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        if (!queue3_.push(std::move(*record))) break;
    }
    if (remaining_workers_.fetch_sub(1, std::memory_order_acq_rel) == 1) queue3_.close();
}

