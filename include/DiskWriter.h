#pragma once

#include "BoundedLockFreeQueue.h"
#include "LogEntry.h"
#include "PipelineCounters.h"

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

// Queue consumer batches records into alternating buffers. A single I/O thread
// writes and flushes each published group, so output rows cannot interleave.
class DiskWriter {
public:
    DiskWriter(BoundedLockFreeQueue<LogEntry>& queue3,
               std::string output_path,
               std::size_t buffer_bytes = 1024 * 1024,
               PipelineCounters* counters = nullptr,
               std::chrono::milliseconds flush_interval = std::chrono::milliseconds(250),
               std::size_t group_records = 256);
    ~DiskWriter();

    DiskWriter(const DiskWriter&) = delete;
    DiskWriter& operator=(const DiskWriter&) = delete;

    void start();
    void stop();

    bool has_error() const;
    std::string error_message() const;

private:
    void producer_loop() noexcept;
    void writer_loop() noexcept;
    bool publish_active_buffer();
    void append_record(const LogEntry& entry, std::string& buffer);
    void append_csv_field(std::string_view field, std::string& buffer);
    void set_error(std::string message) noexcept;

    BoundedLockFreeQueue<LogEntry>& queue3_;
    const std::string output_path_;
    const std::size_t buffer_bytes_;
    PipelineCounters* counters_;
    const std::chrono::milliseconds flush_interval_;
    const std::size_t group_records_;

    std::ofstream output_;
    bool needs_header_ = false;
    std::array<std::string, 2> buffers_;
    std::array<std::uint64_t, 2> record_counts_{{0, 0}};
    std::size_t active_buffer_ = 0;
    int ready_buffer_ = -1; // -1 when no group is waiting for/under I/O.
    bool producer_done_ = false;
    bool io_failed_ = false;

    std::thread producer_thread_;
    std::thread writer_thread_;
    std::atomic<bool> started_{false};
    std::mutex batch_mutex_;
    std::condition_variable batch_ready_;
    std::condition_variable buffer_available_;

    mutable std::mutex error_mutex_;
    std::string error_message_;
};

