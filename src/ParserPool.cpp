#include "ParserPool.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstring>
#include <initializer_list>
#include <iterator>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace {
enum class Format { Json, Syslog, ApacheAccess, ApacheError, Hdfs, Unknown };

std::string_view trim(std::string_view s) {
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) s.remove_prefix(1);
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) s.remove_suffix(1);
    return s;
}

bool copy_field(char* destination, std::size_t capacity, std::string_view value,
                const char* field, std::string& reason) {
    if (value.size() >= capacity) {
        reason = std::string("field_too_long:") + field;
        return false;
    }
    std::memcpy(destination, value.data(), value.size());
    destination[value.size()] = '\0';
    return true;
}

bool json_string(std::string_view input, std::size_t& pos, std::string& out) {
    if (pos >= input.size() || input[pos++] != '"') return false;
    while (pos < input.size()) {
        const char c = input[pos++];
        if (c == '"') return true;
        if (static_cast<unsigned char>(c) < 0x20) return false;
        if (c != '\\') { out.push_back(c); continue; }
        if (pos >= input.size()) return false;
        const char escaped = input[pos++];
        switch (escaped) {
            case '"': out.push_back('"'); break;
            case '\\': out.push_back('\\'); break;
            case '/': out.push_back('/'); break;
            case 'b': out.push_back('\b'); break;
            case 'f': out.push_back('\f'); break;
            case 'n': out.push_back('\n'); break;
            case 'r': out.push_back('\r'); break;
            case 't': out.push_back('\t'); break;
            default: return false; // Unsupported/invalid escape, including \u.
        }
    }
    return false;
}

bool parse_json_flat(std::string_view text, std::vector<std::pair<std::string, std::string>>& fields) {
    std::size_t pos = 0;
    auto whitespace = [&] { while (pos < text.size() && std::isspace(static_cast<unsigned char>(text[pos]))) ++pos; };
    whitespace();
    if (pos == text.size() || text[pos++] != '{') return false;
    whitespace();
    if (pos < text.size() && text[pos] == '}') { ++pos; whitespace(); return pos == text.size(); }
    for (;;) {
        whitespace();
        std::string key, value;
        if (!json_string(text, pos, key)) return false;
        whitespace();
        if (pos == text.size() || text[pos++] != ':') return false;
        whitespace();
        if (pos < text.size() && text[pos] == '"') {
            if (!json_string(text, pos, value)) return false;
        } else {
            const std::size_t start = pos;
            while (pos < text.size() && text[pos] != ',' && text[pos] != '}' &&
                   !std::isspace(static_cast<unsigned char>(text[pos]))) ++pos;
            if (start == pos) return false;
            value.assign(text.substr(start, pos - start));
            if (value != "null" && value != "true" && value != "false" &&
                !std::all_of(value.begin(), value.end(), [](unsigned char c) {
                    return std::isdigit(c) || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E';
                })) return false;
        }
        fields.emplace_back(std::move(key), std::move(value));
        whitespace();
        if (pos >= text.size()) return false;
        if (text[pos] == '}') { ++pos; whitespace(); return pos == text.size(); }
        if (text[pos++] != ',') return false;
    }
}

std::string_view find_field(const std::vector<std::pair<std::string, std::string>>& fields,
                            std::initializer_list<std::string_view> names) {
    for (const auto& field : fields) {
        for (auto name : names) if (field.first == name) return field.second;
    }
    return {};
}

bool parse_json(std::string_view text, const RawLogLine& raw, LogEntry& out, std::string& reason) {
    std::vector<std::pair<std::string, std::string>> fields;
    if (!parse_json_flat(text, fields)) { reason = "malformed_json"; return false; }
    const auto message = find_field(fields, {"message", "msg"});
    if (message.empty()) { reason = "json_missing_message"; return false; }
    const auto timestamp = find_field(fields, {"timestamp", "time", "@timestamp"});
    const auto level = find_field(fields, {"level", "severity"});
    const auto host = find_field(fields, {"host", "hostname"});
    return copy_field(out.timestamp, sizeof(out.timestamp), timestamp, "timestamp", reason) &&
           copy_field(out.log_level, sizeof(out.log_level), level.empty() ? "INFO" : level, "log_level", reason) &&
           copy_field(out.host_source, sizeof(out.host_source), host.empty() ? raw.source : host, "host_source", reason) &&
           copy_field(out.message, sizeof(out.message), message, "message", reason);
}

bool parse_syslog(std::string_view text, const RawLogLine& raw, LogEntry& out, std::string& reason) {
    std::string_view remaining = text;
    std::string_view level = "INFO";
    if (!remaining.empty() && remaining.front() == '<') {
        const auto end = remaining.find('>');
        if (end == std::string_view::npos || end == 1) { reason = "malformed_syslog_priority"; return false; }
        int priority = 0;
        for (std::size_t i = 1; i < end; ++i) {
            if (!std::isdigit(static_cast<unsigned char>(remaining[i]))) { reason = "malformed_syslog_priority"; return false; }
            priority = priority * 10 + (remaining[i] - '0');
        }
        static constexpr const char* levels[] = {"EMERG", "ALERT", "CRITICAL", "ERROR", "WARN", "NOTICE", "INFO", "DEBUG"};
        level = levels[priority % 8];
        remaining.remove_prefix(end + 1);
    }
    auto token = [&remaining]() -> std::string_view {
        remaining = trim(remaining);
        const auto end = remaining.find_first_of(" \t");
        if (end == std::string_view::npos) { auto value = remaining; remaining = {}; return value; }
        auto value = remaining.substr(0, end);
        remaining.remove_prefix(end);
        return value;
    };
    const auto timestamp = token();
    const auto day_or_time = token();
    const auto time_or_host = token();
    if (timestamp.empty() || day_or_time.empty() || time_or_host.empty()) {
        reason = "malformed_syslog_header"; return false;
    }
    std::string normalized_time;
    if (timestamp.size() == 3 && std::isalpha(static_cast<unsigned char>(timestamp[0]))) {
        normalized_time = std::string(timestamp) + " " + std::string(day_or_time) + " " + std::string(time_or_host);
    } else {
        normalized_time = std::string(timestamp);
    }
    auto host = token();
    auto message = trim(remaining);
    // RFC3164: Month Day Time Host TAG: message. The alternate form accepts
    // timestamp Host TAG: message.
    if (timestamp.size() != 3 || !std::isalpha(static_cast<unsigned char>(timestamp[0]))) {
        message = trim(std::string_view(text).substr(text.find(timestamp) + timestamp.size()));
        const auto split = message.find_first_of(" \t");
        if (split == std::string_view::npos) { reason = "malformed_syslog_header"; return false; }
        host = message.substr(0, split);
        message = trim(message.substr(split));
    }
    const auto colon = message.find(": ");
    if (colon != std::string_view::npos) message.remove_prefix(colon + 2);
    if (message.empty()) { reason = "syslog_missing_message"; return false; }
    return copy_field(out.timestamp, sizeof(out.timestamp), normalized_time, "timestamp", reason) &&
           copy_field(out.log_level, sizeof(out.log_level), level, "log_level", reason) &&
           copy_field(out.host_source, sizeof(out.host_source), host.empty() ? raw.source : host, "host_source", reason) &&
           copy_field(out.message, sizeof(out.message), message, "message", reason);
}

bool next_quoted(std::string_view line, std::size_t& pos, std::string_view& value) {
    while (pos < line.size() && line[pos] == ' ') ++pos;
    if (pos >= line.size() || line[pos++] != '"') return false;
    const auto end = line.find('"', pos);
    if (end == std::string_view::npos) return false;
    value = line.substr(pos, end - pos);
    pos = end + 1;
    return true;
}

bool parse_apache(std::string_view line, LogEntry& out, std::string& reason) {
    std::size_t pos = 0;
    const auto host_end = line.find(' ');
    if (host_end == std::string_view::npos || host_end == 0) { reason = "malformed_apache_client"; return false; }
    const auto host = line.substr(0, host_end);
    const auto date_open = line.find('[', host_end);
    const auto date_close = date_open == std::string_view::npos ? date_open : line.find(']', date_open + 1);
    if (date_open == std::string_view::npos || date_close == std::string_view::npos) { reason = "malformed_apache_timestamp"; return false; }
    const auto timestamp = line.substr(date_open + 1, date_close - date_open - 1);
    pos = date_close + 1;
    std::string_view request;
    if (!next_quoted(line, pos, request) || request.empty()) { reason = "malformed_apache_request"; return false; }
    while (pos < line.size() && line[pos] == ' ') ++pos;
    const auto status_end = line.find(' ', pos);
    if (status_end == std::string_view::npos) { reason = "malformed_apache_status"; return false; }
    const auto status_text = line.substr(pos, status_end - pos);
    if (status_text.size() != 3 || !std::all_of(status_text.begin(), status_text.end(), [](unsigned char c){ return std::isdigit(c); })) {
        reason = "malformed_apache_status"; return false;
    }
    const int status = (status_text[0]-'0')*100 + (status_text[1]-'0')*10 + (status_text[2]-'0');
    const std::string_view level = status >= 500 ? "ERROR" : status >= 400 ? "WARN" : "INFO";
    std::string message(request);
    message.append(" | status=");
    message.append(status_text);
    auto bytes = trim(line.substr(status_end + 1));
    if (!bytes.empty()) { message.append(" | bytes="); message.append(bytes.substr(0, bytes.find(' '))); }
    return copy_field(out.timestamp, sizeof(out.timestamp), timestamp, "timestamp", reason) &&
           copy_field(out.log_level, sizeof(out.log_level), level, "log_level", reason) &&
           copy_field(out.host_source, sizeof(out.host_source), host, "host_source", reason) &&
           copy_field(out.message, sizeof(out.message), message, "message", reason);
}

bool parse_apache_error(std::string_view line, const RawLogLine& raw,
                        LogEntry& out, std::string& reason) {
    const auto timestamp_end = line.find(']');
    if (line.empty() || line.front() != '[' || timestamp_end == std::string_view::npos) {
        reason = "malformed_apache_error_timestamp";
        return false;
    }
    const auto timestamp = line.substr(1, timestamp_end - 1);
    std::size_t pos = timestamp_end + 1;
    while (pos < line.size() && line[pos] == ' ') ++pos;
    if (pos >= line.size() || line[pos] != '[') {
        reason = "malformed_apache_error_level";
        return false;
    }
    const auto level_end = line.find(']', pos + 1);
    if (level_end == std::string_view::npos) {
        reason = "malformed_apache_error_level";
        return false;
    }
    auto level = line.substr(pos + 1, level_end - pos - 1);
    std::string normalized_level;
    normalized_level.reserve(level.size());
    for (unsigned char c : level) normalized_level.push_back(static_cast<char>(std::toupper(c)));
    if (normalized_level == "WARNING") normalized_level = "WARN";
    else if (normalized_level == "CRIT" || normalized_level == "EMERG" || normalized_level == "ALERT") normalized_level = "CRITICAL";
    else if (normalized_level == "NOTICE") normalized_level = "INFO";

    pos = level_end + 1;
    while (pos < line.size() && line[pos] == ' ') ++pos;
    std::string_view host = "apache";
    if (line.substr(pos, 8) == "[client ") {
        const auto client_end = line.find(']', pos + 8);
        if (client_end == std::string_view::npos) {
            reason = "malformed_apache_error_client";
            return false;
        }
        host = line.substr(pos + 8, client_end - pos - 8);
        pos = client_end + 1;
        while (pos < line.size() && line[pos] == ' ') ++pos;
    }
    const auto message = trim(line.substr(pos));
    if (message.empty()) {
        reason = "apache_error_missing_message";
        return false;
    }
    return copy_field(out.timestamp, sizeof(out.timestamp), timestamp, "timestamp", reason) &&
           copy_field(out.log_level, sizeof(out.log_level), normalized_level, "log_level", reason) &&
           copy_field(out.host_source, sizeof(out.host_source), host.empty() ? raw.source : host, "host_source", reason) &&
           copy_field(out.message, sizeof(out.message), message, "message", reason);
}

bool parse_hdfs(std::string_view line, LogEntry& out, std::string& reason) {
    auto next_token = [&line]() -> std::string_view {
        line = trim(line);
        const auto end = line.find_first_of(" \t");
        if (end == std::string_view::npos) {
            const auto value = line;
            line = {};
            return value;
        }
        const auto value = line.substr(0, end);
        line.remove_prefix(end);
        return value;
    };
    const auto date = next_token();
    const auto time = next_token();
    const auto pid = next_token();
    const auto level = next_token();
    const auto body = trim(line);
    auto digits = [](std::string_view s) {
        return !s.empty() && std::all_of(s.begin(), s.end(), [](unsigned char c) { return std::isdigit(c); });
    };
    if (date.size() != 6 || time.size() != 6 || !digits(date) || !digits(time) || !digits(pid) || body.empty()) {
        reason = "malformed_hdfs_header";
        return false;
    }
    static constexpr const char* accepted_levels[] = {"INFO", "WARN", "ERROR", "DEBUG", "FATAL", "TRACE"};
    if (std::find(std::begin(accepted_levels), std::end(accepted_levels), level) == std::end(accepted_levels)) {
        reason = "malformed_hdfs_level";
        return false;
    }
    // HDFS lines continue with a Java logger/component name followed by ': '
    // and the actual payload, e.g. dfs.DataNode$PacketResponder: PacketResponder...
    const auto component_end = body.find(": ");
    if (component_end == std::string_view::npos || component_end == 0 || component_end + 2 >= body.size()) {
        reason = "malformed_hdfs_component";
        return false;
    }
    const auto component = body.substr(0, component_end);
    const auto message = trim(body.substr(component_end + 2));
    const int short_year = (date[0] - '0') * 10 + (date[1] - '0');
    const int year = short_year < 70 ? 2000 + short_year : 1900 + short_year;
    std::string timestamp = std::to_string(year) + "-" + std::string(date.substr(2, 2)) + "-" +
                            std::string(date.substr(4, 2)) + "T" + std::string(time.substr(0, 2)) + ":" +
                            std::string(time.substr(2, 2)) + ":" + std::string(time.substr(4, 2)) + "Z";
    return copy_field(out.timestamp, sizeof(out.timestamp), timestamp, "timestamp", reason) &&
           copy_field(out.log_level, sizeof(out.log_level), level, "log_level", reason) &&
           copy_field(out.host_source, sizeof(out.host_source), component, "host_source", reason) &&
           copy_field(out.message, sizeof(out.message), message, "message", reason);
}

bool is_month(std::string_view value) {
    static constexpr std::string_view months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
    return std::find(std::begin(months), std::end(months), value) != std::end(months);
}

bool looks_like_hdfs(std::string_view text) {
    std::array<std::string_view, 4> tokens{};
    for (auto& token : tokens) {
        text = trim(text);
        const auto end = text.find_first_of(" \t");
        if (end == std::string_view::npos) return false;
        token = text.substr(0, end);
        text.remove_prefix(end);
    }
    const auto digits = [](std::string_view s) {
        return !s.empty() && std::all_of(s.begin(), s.end(), [](unsigned char c) { return std::isdigit(c); });
    };
    return tokens[0].size() == 6 && tokens[1].size() == 6 && digits(tokens[0]) && digits(tokens[1]) &&
           digits(tokens[2]) && (tokens[3] == "INFO" || tokens[3] == "WARN" || tokens[3] == "ERROR" ||
                                  tokens[3] == "DEBUG" || tokens[3] == "FATAL" || tokens[3] == "TRACE");
}

bool looks_like_syslog(std::string_view text) {
    if (!text.empty() && text.front() == '<') return true;
    std::array<std::string_view, 3> tokens{};
    for (auto& token : tokens) {
        text = trim(text);
        const auto end = text.find_first_of(" \t");
        if (end == std::string_view::npos) return false;
        token = text.substr(0, end);
        text.remove_prefix(end);
    }
    if (is_month(tokens[0])) return tokens[1].size() <= 2 && tokens[2].find(':') != std::string_view::npos;
    return tokens[0].find('T') != std::string_view::npos && tokens[1].find(':') != std::string_view::npos;
}

Format detect_format(std::string_view text) {
    text = trim(text);
    if (!text.empty() && text.front() == '{') return Format::Json;
    if (looks_like_hdfs(text)) return Format::Hdfs;
    if (!text.empty() && text.front() == '[' && text.find("] [") != std::string_view::npos) return Format::ApacheError;
    const auto bracket = text.find('[');
    const auto quote = text.find('"');
    const auto first_space = text.find(' ');
    if (bracket != std::string_view::npos && quote != std::string_view::npos && first_space < bracket && bracket < quote)
        return Format::ApacheAccess;
    if (looks_like_syslog(text)) return Format::Syslog;
    return Format::Unknown;
}
} // namespace

ParserPool::ParserPool(BoundedLockFreeQueue<RawLogLine>& queue1,
                       BoundedLockFreeQueue<LogEntry>& queue2,
                       std::size_t worker_count,
                       std::string quarantine_path,
                       PipelineCounters* counters,
                       QuarantineLineHandler quarantine_handler)
    : queue1_(queue1), queue2_(queue2), worker_count_(worker_count),
      quarantine_path_(std::move(quarantine_path)), counters_(counters),
      quarantine_handler_(std::move(quarantine_handler)) {
    if (worker_count_ == 0) throw std::invalid_argument("worker_count must be greater than zero");
    if (!quarantine_handler_) {
        quarantine_file_.open(quarantine_path_, std::ios::out | std::ios::app);
        if (!quarantine_file_) throw std::runtime_error("cannot open quarantine file: " + quarantine_path_);
    }
}

ParserPool::~ParserPool() { stop(); }

void ParserPool::start() {
    bool expected = false;
    if (!started_.compare_exchange_strong(expected, true)) throw std::logic_error("ParserPool can only be started once");
    remaining_workers_.store(worker_count_, std::memory_order_release);
    try {
        workers_.reserve(worker_count_);
        for (std::size_t i = 0; i < worker_count_; ++i) workers_.emplace_back(&ParserPool::worker_loop, this);
    } catch (...) {
        queue1_.close();
        for (auto& worker : workers_) if (worker.joinable()) worker.join();
        queue2_.close();
        started_.store(false);
        throw;
    }
}

void ParserPool::stop() {
    if (!started_.exchange(false, std::memory_order_acq_rel)) return;
    // The receiver should already be stopped. Closing here also wakes workers
    // if the coordinator stops the pool directly; queued input is still drained.
    queue1_.close();
    for (auto& worker : workers_) if (worker.joinable()) worker.join();
    workers_.clear();
    quarantine_file_.flush();
}

void ParserPool::worker_loop() {
    while (auto raw = queue1_.pop()) {
        LogEntry entry{};
        std::string reason;
        if (!parse_line(*raw, entry, reason)) {
            append_quarantine(*raw, reason);
            continue;
        }
        if (!queue2_.push(std::move(entry))) break;
    }
    if (remaining_workers_.fetch_sub(1, std::memory_order_acq_rel) == 1) queue2_.close();
}

bool ParserPool::parse_line(const RawLogLine& raw, LogEntry& output, std::string& reason) const {
    const std::string_view text = trim(raw.text);
    if (text.empty()) { reason = "empty_line"; return false; }
    switch (detect_format(text)) {
        case Format::Json: return parse_json(text, raw, output, reason);
        case Format::Syslog: return parse_syslog(text, raw, output, reason);
        case Format::ApacheAccess:
            if (!parse_apache(text, output, reason)) return false;
            // Apache's first field is the remote client; retain the receiver's
            // source identity in the message to keep both identities available.
            return true;
        case Format::ApacheError: return parse_apache_error(text, raw, output, reason);
        case Format::Hdfs: return parse_hdfs(text, output, reason);
        case Format::Unknown: reason = "unknown_format"; return false;
    }
    reason = "unknown_format";
    return false;
}

void ParserPool::append_quarantine(const RawLogLine& raw, const std::string& reason) {
    if (counters_) counters_->quarantined.fetch_add(1, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock(quarantine_mutex_);
    if (quarantine_handler_) {
        quarantine_handler_(raw, reason);
    } else {
        quarantine_file_ << "reason=" << reason << "\tsource=" << raw.source
                         << "\traw=" << raw.text << '\n';
        quarantine_file_.flush();
    }
}

