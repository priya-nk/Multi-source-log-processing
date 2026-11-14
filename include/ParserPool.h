#pragma once

#include "LogEntry.h"
#include "PipelineCounters.h"
#include "TcpLogReceiver.h"

#include <atomic>
#include <cstddef>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// Consumes Queue 1 until it is closed and drained. Stop the receiver before
// stop() for orderly shutdown; stop() closes Queue 1 and joins all workers.
class ParserPool {
public:
    ParserPool(BoundedLockFreeQueue<RawLogLine>& queue1,
               BoundedLockFreeQueue<LogEntry>& queue2,
               std::size_t worker_count,
               std::string quarantine_path,
               PipelineCounters* counters = nullptr,
               QuarantineLineHandler quarantine_handler = {});
    ~ParserPool();

    ParserPool(const ParserPool&) = delete;
    ParserPool& operator=(const ParserPool&) = delete;

    void start();
    void stop();

private:
    void worker_loop();
    bool parse_line(const RawLogLine& raw, LogEntry& output, std::string& reason) const;
    void append_quarantine(const RawLogLine& raw, const std::string& reason);

    BoundedLockFreeQueue<RawLogLine>& queue1_;
    BoundedLockFreeQueue<LogEntry>& queue2_;
    const std::size_t worker_count_;
    const std::string quarantine_path_;
    std::ofstream quarantine_file_;
    PipelineCounters* counters_;
    QuarantineLineHandler quarantine_handler_;
    std::mutex quarantine_mutex_;
    std::vector<std::thread> workers_;
    std::atomic<std::size_t> remaining_workers_{0};
    std::atomic<bool> started_{false};
};

