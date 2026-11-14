#include "DiskWriter.h"

#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace {
template <std::size_t N>
std::string_view bounded_field(const char (&field)[N]) noexcept {
    const auto* end = static_cast<const char*>(std::memchr(field, '\0', N));
    return std::string_view(field, end ? static_cast<std::size_t>(end - field) : N);
}
} // namespace

DiskWriter::DiskWriter(BoundedLockFreeQueue<LogEntry>& queue3,
                       std::string output_path,
                       std::size_t buffer_bytes,
                       PipelineCounters* counters,
                       std::chrono::milliseconds flush_interval,
                       std::size_t group_records)
    : queue3_(queue3), output_path_(std::move(output_path)), buffer_bytes_(buffer_bytes),
      counters_(counters), flush_interval_(flush_interval), group_records_(group_records) {
    if (output_path_.empty()) throw std::invalid_argument("output_path must not be empty");
    if (buffer_bytes_ == 0) throw std::invalid_argument("buffer_bytes must be greater than zero");
    if (flush_interval_.count() <= 0) throw std::invalid_argument("flush_interval must be positive");
    if (group_records_ == 0) throw std::invalid_argument("group_records must be greater than zero");
}

DiskWriter::~DiskWriter() { stop(); }

void DiskWriter::start() {
    bool expected = false;
    if (!started_.compare_exchange_strong(expected, true)) {
        throw std::logic_error("DiskWriter can only be started once");
    }

    std::error_code ec;
    const bool exists = std::filesystem::exists(output_path_, ec);
    if (ec) {
        started_.store(false, std::memory_order_release);
        throw std::runtime_error("cannot inspect output file: " + ec.message());
    }
    const auto size = exists ? std::filesystem::file_size(output_path_, ec) : 0;
    if (ec) {
        started_.store(false, std::memory_order_release);
        throw std::runtime_error("cannot read output file size: " + ec.message());
    }
    needs_header_ = size == 0;

    output_.open(output_path_, std::ios::out | std::ios::app | std::ios::binary);
    if (!output_) {
        started_.store(false, std::memory_order_release);
        throw std::runtime_error("cannot open output file: " + output_path_);
    }
    output_.exceptions(std::ios::badbit | std::ios::failbit);
    try {
        buffers_[0].reserve(buffer_bytes_ + 1024);
        buffers_[1].reserve(buffer_bytes_ + 1024);
        writer_thread_ = std::thread(&DiskWriter::writer_loop, this);
        producer_thread_ = std::thread(&DiskWriter::producer_loop, this);
    } catch (...) {
        {
            std::lock_guard<std::mutex> lock(batch_mutex_);
            io_failed_ = true;
            producer_done_ = true;
        }
        batch_ready_.notify_all();
        buffer_available_.notify_all();
        if (producer_thread_.joinable()) producer_thread_.join();
        if (writer_thread_.joinable()) writer_thread_.join();
        started_.store(false, std::memory_order_release);
        if (output_.is_open()) output_.close();
        throw;
    }
}

void DiskWriter::stop() {
    if (!started_.exchange(false, std::memory_order_acq_rel)) return;
    // Upstream should be stopped first. Closing Queue 3 wakes the consumer and
    // lets it drain queued records before it publishes its final partial group.
    queue3_.close();
    if (producer_thread_.joinable()) producer_thread_.join();
    if (writer_thread_.joinable()) writer_thread_.join();
}

bool DiskWriter::has_error() const {
    std::lock_guard<std::mutex> lock(error_mutex_);
    return !error_message_.empty();
}

std::string DiskWriter::error_message() const {
    std::lock_guard<std::mutex> lock(error_mutex_);
    return error_message_;
}

bool DiskWriter::publish_active_buffer() {
    std::unique_lock<std::mutex> lock(batch_mutex_);
    buffer_available_.wait(lock, [this] { return ready_buffer_ == -1 || io_failed_; });
    if (io_failed_) return false;
    if (buffers_[active_buffer_].empty() && record_counts_[active_buffer_] == 0) return true;

    ready_buffer_ = static_cast<int>(active_buffer_);
    active_buffer_ = 1 - active_buffer_;
    lock.unlock();
    batch_ready_.notify_one();
    return true;
}

void DiskWriter::producer_loop() noexcept {
    try {
        if (needs_header_) buffers_[active_buffer_] = "timestamp,log_level,host_source,message\n";
        auto last_commit = std::chrono::steady_clock::now();

        for (;;) {
            if (io_failed_) break;
            auto record = queue3_.pop_for(flush_interval_);
            const bool input_closed = !record && queue3_.is_closed();
            if (record) {
                append_record(*record, buffers_[active_buffer_]);
                ++record_counts_[active_buffer_];
            }

            const auto now = std::chrono::steady_clock::now();
            const bool pending = !buffers_[active_buffer_].empty();
            const bool group_ready = pending &&
                (buffers_[active_buffer_].size() >= buffer_bytes_ ||
                 record_counts_[active_buffer_] >= group_records_ ||
                 now - last_commit >= flush_interval_);
            if (group_ready) {
                if (!publish_active_buffer()) break;
                last_commit = now;
            }
            if (input_closed) break;
        }

        if (!io_failed_ && (!buffers_[active_buffer_].empty() || record_counts_[active_buffer_] != 0)) {
            publish_active_buffer();
        }
    } catch (const std::exception& error) {
        set_error(error.what());
        queue3_.close();
        {
            std::lock_guard<std::mutex> lock(batch_mutex_);
            io_failed_ = true;
        }
        buffer_available_.notify_all();
    } catch (...) {
        set_error("unknown disk writer batch error");
        queue3_.close();
        {
            std::lock_guard<std::mutex> lock(batch_mutex_);
            io_failed_ = true;
        }
        buffer_available_.notify_all();
    }

    {
        std::lock_guard<std::mutex> lock(batch_mutex_);
        producer_done_ = true;
    }
    batch_ready_.notify_all();
}

void DiskWriter::writer_loop() noexcept {
    try {
        for (;;) {
            int index;
            std::uint64_t committed_records;
            {
                std::unique_lock<std::mutex> lock(batch_mutex_);
                batch_ready_.wait(lock, [this] { return ready_buffer_ != -1 || producer_done_ || io_failed_; });
                if (io_failed_) break;
                if (ready_buffer_ == -1 && producer_done_) break;
                index = ready_buffer_;
                committed_records = record_counts_[static_cast<std::size_t>(index)];
            }

            const auto& group = buffers_[static_cast<std::size_t>(index)];
            output_.write(group.data(), static_cast<std::streamsize>(group.size()));
            output_.flush(); // One flush per group commit, not per record.

            {
                std::lock_guard<std::mutex> lock(batch_mutex_);
                buffers_[static_cast<std::size_t>(index)].clear();
                record_counts_[static_cast<std::size_t>(index)] = 0;
                ready_buffer_ = -1;
            }
            if (counters_ && committed_records != 0) {
                counters_->written.fetch_add(committed_records, std::memory_order_relaxed);
            }
            buffer_available_.notify_one();
        }
        output_.flush();
        output_.close();
    } catch (const std::exception& error) {
        set_error(error.what());
        queue3_.close();
        {
            std::lock_guard<std::mutex> lock(batch_mutex_);
            io_failed_ = true;
            ready_buffer_ = -1;
        }
        batch_ready_.notify_all();
        buffer_available_.notify_all();
        try { output_.close(); } catch (...) {}
    } catch (...) {
        set_error("unknown disk writer I/O error");
        queue3_.close();
        {
            std::lock_guard<std::mutex> lock(batch_mutex_);
            io_failed_ = true;
            ready_buffer_ = -1;
        }
        batch_ready_.notify_all();
        buffer_available_.notify_all();
        try { output_.close(); } catch (...) {}
    }
}

void DiskWriter::append_record(const LogEntry& entry, std::string& buffer) {
    append_csv_field(bounded_field(entry.timestamp), buffer);
    buffer.push_back(',');
    append_csv_field(bounded_field(entry.log_level), buffer);
    buffer.push_back(',');
    append_csv_field(bounded_field(entry.host_source), buffer);
    buffer.push_back(',');
    append_csv_field(bounded_field(entry.message), buffer);
    buffer.push_back('\n');
}

void DiskWriter::append_csv_field(std::string_view field, std::string& buffer) {
    buffer.push_back('"');
    for (const char c : field) {
        switch (c) {
            case '"': buffer.append("\"\""); break;
            case '\n': buffer.append("\\n"); break;
            case '\r': buffer.append("\\r"); break;
            case '\0': buffer.append("\\0"); break;
            default: buffer.push_back(c); break;
        }
    }
    buffer.push_back('"');
}

void DiskWriter::set_error(std::string message) noexcept {
    try {
        std::lock_guard<std::mutex> lock(error_mutex_);
        error_message_ = std::move(message);
    } catch (...) {}
}

