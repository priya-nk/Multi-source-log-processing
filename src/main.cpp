#include "DedupStage.h"
#include "DiskWriter.h"
#include "ParserPool.h"
#include "PipelineCounters.h"
#include "TcpLogReceiver.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

#ifdef _WIN32
#include <windows.h>
#endif

namespace {
volatile std::sig_atomic_t g_stop_requested = 0;

void request_stop(int) { g_stop_requested = 1; }

struct Config {
    unsigned short port = 18080;
    std::string output_path = "normalized_output.csv";
    std::string quarantine_path = "quarantine.log";
    std::size_t queue_capacity = 16384;
    std::size_t parser_threads = std::max(1u, std::thread::hardware_concurrency() / 2);
    std::size_t dedup_threads = std::max(1u, std::thread::hardware_concurrency() / 2);
    std::uint64_t window_ms = 5 * 60 * 1000;
    std::uint64_t telemetry_ms = 1000;
};

std::uint64_t number_arg(const std::string& flag, const std::string& value) {
    try {
        std::size_t used = 0;
        const auto result = std::stoull(value, &used);
        if (used != value.size()) throw std::invalid_argument("trailing characters");
        return result;
    } catch (...) {
        throw std::invalid_argument("invalid value for " + flag + ": " + value);
    }
}

Config parse_args(int argc, char** argv) {
    Config config;
    for (int i = 1; i < argc; ++i) {
        const std::string flag = argv[i];
        if (flag == "--help" || flag == "-h") {
            std::cout << "Usage: log_processor [--port N] [--output PATH] [--quarantine PATH] "
                         "[--queue-capacity N] [--parser-threads N] [--dedup-threads N] "
                         "[--window-ms N] [--telemetry-ms N]\n";
            std::exit(0);
        }
        if (i + 1 >= argc) throw std::invalid_argument("missing value for " + flag);
        const std::string value = argv[++i];
        if (flag == "--port") {
            const auto n = number_arg(flag, value);
            if (n == 0 || n > 65535) throw std::invalid_argument("port must be in [1, 65535]");
            config.port = static_cast<unsigned short>(n);
        } else if (flag == "--output") config.output_path = value;
        else if (flag == "--quarantine") config.quarantine_path = value;
        else if (flag == "--queue-capacity") config.queue_capacity = number_arg(flag, value);
        else if (flag == "--parser-threads") config.parser_threads = number_arg(flag, value);
        else if (flag == "--dedup-threads") config.dedup_threads = number_arg(flag, value);
        else if (flag == "--window-ms") config.window_ms = number_arg(flag, value);
        else if (flag == "--telemetry-ms") config.telemetry_ms = number_arg(flag, value);
        else throw std::invalid_argument("unknown option: " + flag);
    }
    if (config.queue_capacity == 0 || config.parser_threads == 0 || config.dedup_threads == 0 ||
        config.window_ms == 0 || config.telemetry_ms == 0) {
        throw std::invalid_argument("queue capacity, worker counts and intervals must be greater than zero");
    }
    if (config.output_path == config.quarantine_path) {
        throw std::invalid_argument("output and quarantine paths must be different");
    }
    return config;
}

class QuarantineSink {
public:
    explicit QuarantineSink(const std::string& path) : file_(path, std::ios::out | std::ios::app | std::ios::binary) {
        if (!file_) throw std::runtime_error("cannot open quarantine file: " + path);
    }

    void append(const RawLogLine& line, std::string_view reason) noexcept {
        try {
            std::string record;
            record.reserve(reason.size() + line.source.size() + line.text.size() + 32);
            record.append("reason="); record.append(reason);
            record.append("\tsource="); append_safe(line.source, record);
            record.append("\traw="); append_safe(line.text, record);
            record.push_back('\n');
            std::lock_guard<std::mutex> lock(mutex_);
            file_.write(record.data(), static_cast<std::streamsize>(record.size()));
            file_.flush();
        } catch (...) {
            // Keep quarantine I/O failures from terminating receiver/parser threads.
        }
    }

private:
    static void append_safe(std::string_view text, std::string& output) {
        for (char c : text) {
            if (c == '\n') output.append("\\n");
            else if (c == '\r') output.append("\\r");
            else if (c == '\t') output.append("\\t");
            else output.push_back(c);
        }
    }

    std::ofstream file_;
    std::mutex mutex_;
};

std::int64_t invariant_delta(std::uint64_t received, std::uint64_t written,
                             std::uint64_t duplicates, std::uint64_t quarantined) {
    return static_cast<std::int64_t>(received) - static_cast<std::int64_t>(written) -
           static_cast<std::int64_t>(duplicates) - static_cast<std::int64_t>(quarantined);
}

void print_telemetry(const PipelineCounters& counters, std::uint64_t duplicates,
                     double lines_per_second) {
    const auto received = counters.received.load(std::memory_order_relaxed);
    const auto written = counters.written.load(std::memory_order_relaxed);
    const auto quarantined = counters.quarantined.load(std::memory_order_relaxed);
    const auto delta = invariant_delta(received, written, duplicates, quarantined);
    std::cout << "received=" << received << " written=" << written
              << " duplicates_dropped=" << duplicates << " quarantined=" << quarantined
              << " invariant_delta=" << delta << " input_lines_per_sec="
              << static_cast<std::uint64_t>(lines_per_second) << '\n';
}
} // namespace

int main(int argc, char** argv) {
    try {
        const Config config = parse_args(argc, argv);
        std::signal(SIGINT, request_stop);
        std::signal(SIGTERM, request_stop);
#ifdef SIGBREAK
        std::signal(SIGBREAK, request_stop);
#endif

        BoundedLockFreeQueue<RawLogLine> queue1(config.queue_capacity);
        BoundedLockFreeQueue<LogEntry> queue2(config.queue_capacity);
        BoundedLockFreeQueue<LogEntry> queue3(config.queue_capacity);
        PipelineCounters counters;
        QuarantineSink quarantine(config.quarantine_path);
        const QuarantineLineHandler quarantine_handler =
            [&quarantine](const RawLogLine& line, std::string_view reason) {
                quarantine.append(line, reason);
            };

        TcpLogReceiver receiver(queue1, config.port, quarantine_handler,
                                1024 * 1024, &counters);
        ParserPool parser(queue1, queue2, config.parser_threads,
                          config.quarantine_path, &counters, quarantine_handler);
        DedupStage dedup(queue2, queue3, config.dedup_threads,
                         std::chrono::milliseconds(config.window_ms));
        DiskWriter writer(queue3, config.output_path, 1024 * 1024, &counters);

        writer.start();
        dedup.start();
        parser.start();
        receiver.start();

        std::cout << "Log processor listening on port " << config.port
                  << " (Ctrl+C to stop)\n"
                  << "Output: " << config.output_path << "\n"
                  << "Quarantine: " << config.quarantine_path << "\n";

        auto last_time = std::chrono::steady_clock::now();
        std::uint64_t last_received = 0;
        while (!g_stop_requested) {
            const auto delay = std::chrono::milliseconds(100);
            std::this_thread::sleep_for(delay);
            const auto now = std::chrono::steady_clock::now();
            if (now - last_time < std::chrono::milliseconds(config.telemetry_ms)) continue;
            const auto received = counters.received.load(std::memory_order_relaxed);
            const double seconds = std::chrono::duration<double>(now - last_time).count();
            const double rate = seconds > 0 ? (received - last_received) / seconds : 0.0;
            print_telemetry(counters, dedup.duplicates_dropped(), rate);
            last_received = received;
            last_time = now;
        }

        std::cout << "Stopping receiver and draining pipeline...\n";
        receiver.stop();
        parser.stop();
        dedup.stop();
        writer.stop();

        print_telemetry(counters, dedup.duplicates_dropped(), 0.0);
        if (writer.has_error()) {
            std::cerr << "Disk writer error: " << writer.error_message() << '\n';
            return 2;
        }
        const auto final_delta = invariant_delta(
            counters.received.load(), counters.written.load(),
            dedup.duplicates_dropped(), counters.quarantined.load());
        if (final_delta != 0) {
            std::cerr << "Pipeline conservation check failed: delta=" << final_delta << '\n';
            return 3;
        }
        std::cout << "Graceful shutdown complete; all received lines accounted for.\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Fatal error: " << error.what() << '\n';
        return 1;
    }
}

