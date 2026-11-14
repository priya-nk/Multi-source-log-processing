#include "TcpLogReceiver.h"

#include <algorithm>
#include <array>
#include <stdexcept>
#include <utility>

#ifdef _WIN32
using SocketLength = int;
static constexpr TcpNativeSocket kInvalidSocket = INVALID_SOCKET;
static int last_socket_error() { return WSAGetLastError(); }
static void close_socket(TcpNativeSocket s) { closesocket(s); }
static void shutdown_socket(TcpNativeSocket s) { shutdown(s, SD_BOTH); }
#else
#include <arpa/inet.h>
#include <cerrno>
#include <netinet/in.h>
#include <unistd.h>
using SocketLength = socklen_t;
static constexpr TcpNativeSocket kInvalidSocket = -1;
static int last_socket_error() { return errno; }
static void close_socket(TcpNativeSocket s) { close(s); }
static void shutdown_socket(TcpNativeSocket s) { shutdown(s, SHUT_RDWR); }
#endif

TcpLogReceiver::TcpLogReceiver(BoundedLockFreeQueue<RawLogLine>& queue1,
                               unsigned short port,
                               QuarantineLineHandler quarantine,
                               std::size_t max_line_bytes,
                               PipelineCounters* counters)
    : queue1_(queue1),
      port_(port),
      max_line_bytes_(max_line_bytes),
      quarantine_handler_(std::move(quarantine)),
      counters_(counters),
      listen_socket_(kInvalidSocket) {
    if (max_line_bytes_ == 0) {
        throw std::invalid_argument("max_line_bytes must be greater than zero");
    }
}

TcpLogReceiver::~TcpLogReceiver() { stop(); }

void TcpLogReceiver::start() {
    bool expected = false;
    if (!started_.compare_exchange_strong(expected, true)) {
        throw std::logic_error("TcpLogReceiver can only be started once");
    }

#ifdef _WIN32
    WSADATA data{};
    const int startup_result = WSAStartup(MAKEWORD(2, 2), &data);
    if (startup_result != 0) {
        started_.store(false);
        throw std::runtime_error("WSAStartup failed: " + std::to_string(startup_result));
    }
#endif

    listen_socket_ = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listen_socket_ == kInvalidSocket) {
#ifdef _WIN32
        WSACleanup();
#endif
        started_.store(false);
        throw std::runtime_error("socket() failed: " + std::to_string(last_socket_error()));
    }

#ifdef _WIN32
    int exclusive = 1;
    if (setsockopt(listen_socket_, SOL_SOCKET, SO_EXCLUSIVEADDRUSE,
                   reinterpret_cast<const char*>(&exclusive), sizeof(exclusive)) != 0) {
        const int error = last_socket_error();
        close_socket(listen_socket_);
        listen_socket_ = kInvalidSocket;
        WSACleanup();
        started_.store(false);
        throw std::runtime_error("setsockopt(SO_EXCLUSIVEADDRUSE) failed: " + std::to_string(error));
    }
#else
    int reuse = 1;
    setsockopt(listen_socket_, SOL_SOCKET, SO_REUSEADDR, &reuse,
               sizeof(reuse));
#endif

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(port_);
    if (bind(listen_socket_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
        const int error = last_socket_error();
        close_socket(listen_socket_);
        listen_socket_ = kInvalidSocket;
#ifdef _WIN32
        WSACleanup();
#endif
        started_.store(false);
        throw std::runtime_error("bind() failed: " + std::to_string(error));
    }
    if (listen(listen_socket_, SOMAXCONN) != 0) {
        const int error = last_socket_error();
        close_socket(listen_socket_);
        listen_socket_ = kInvalidSocket;
#ifdef _WIN32
        WSACleanup();
#endif
        started_.store(false);
        throw std::runtime_error("listen() failed: " + std::to_string(error));
    }

    stopping_.store(false, std::memory_order_release);
    accept_thread_ = std::thread(&TcpLogReceiver::accept_loop, this);
}

void TcpLogReceiver::stop() noexcept {
    if (!started_.exchange(false, std::memory_order_acq_rel)) return;
    stopping_.store(true, std::memory_order_release);

    if (listen_socket_ != kInvalidSocket) shutdown_socket(listen_socket_);
    if (accept_thread_.joinable()) accept_thread_.join();

    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        for (const auto socket : active_clients_) shutdown_socket(socket);
    }
    for (auto& worker : client_threads_) {
        if (worker.joinable()) worker.join();
    }
    client_threads_.clear();

    if (listen_socket_ != kInvalidSocket) {
        close_socket(listen_socket_);
        listen_socket_ = kInvalidSocket;
    }
    queue1_.close();
#ifdef _WIN32
    WSACleanup();
#endif
}

void TcpLogReceiver::accept_loop() {
    while (!stopping_.load(std::memory_order_acquire)) {
        sockaddr_in peer{};
        SocketLength peer_length = sizeof(peer);
        const TcpNativeSocket client = accept(
            listen_socket_, reinterpret_cast<sockaddr*>(&peer), &peer_length);
        if (client == kInvalidSocket) {
            if (stopping_.load(std::memory_order_acquire)) break;
            continue;
        }

        char ip[INET_ADDRSTRLEN]{};
        if (inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof(ip)) == nullptr) {
            std::copy_n("unknown", sizeof("unknown"), ip);
        }
        const std::string source = std::string(ip) + ":" +
                                   std::to_string(ntohs(peer.sin_port));
        register_client(client);
        try {
            client_threads_.emplace_back(&TcpLogReceiver::client_loop, this, client, source);
        } catch (...) {
            unregister_client(client);
            close_socket(client);
            stopping_.store(true, std::memory_order_release);
            break;
        }
    }
}

void TcpLogReceiver::client_loop(TcpNativeSocket socket, std::string source) {
    std::string sliding_buffer;
    sliding_buffer.reserve(std::min<std::size_t>(max_line_bytes_, 64 * 1024));
    bool discarding_oversized_frame = false;
    bool stop_reading = false;
    std::array<char, 16 * 1024> chunk{};

    for (;;) {
        const auto received = recv(socket, chunk.data(), static_cast<int>(chunk.size()), 0);
        if (received <= 0) break;

        stop_reading = false;
        for (int i = 0; i < received; ++i) {
            const char byte = chunk[static_cast<std::size_t>(i)];
            if (byte == '\n') {
                if (discarding_oversized_frame) {
                    quarantine(RawLogLine{source, std::move(sliding_buffer)}, "frame_too_large");
                    sliding_buffer.clear();
                    discarding_oversized_frame = false;
                    continue;
                }
                if (!sliding_buffer.empty() && sliding_buffer.back() == '\r') {
                    sliding_buffer.pop_back();
                }
                RawLogLine line{source, std::move(sliding_buffer)};
                sliding_buffer.clear();
                if (counters_) counters_->received.fetch_add(1, std::memory_order_relaxed);
                if (!queue1_.push(std::move(line))) {
                    if (counters_) counters_->received.fetch_sub(1, std::memory_order_relaxed);
                    stop_reading = true;
                    break;
                }
                continue;
            }

            if (discarding_oversized_frame) continue;
            if (sliding_buffer.size() == max_line_bytes_) {
                discarding_oversized_frame = true;
            } else {
                sliding_buffer.push_back(byte);
            }
        }
        if (stop_reading) break;
    }

    if (discarding_oversized_frame) {
        quarantine(RawLogLine{source, std::move(sliding_buffer)}, "frame_too_large_connection_closed");
    } else if (!sliding_buffer.empty()) {
        quarantine(RawLogLine{source, std::move(sliding_buffer)},
                   "incomplete_frame_connection_closed");
    }

    unregister_client(socket);
    close_socket(socket);
}

void TcpLogReceiver::quarantine(const RawLogLine& line, std::string_view reason) noexcept {
    if (counters_) {
        counters_->received.fetch_add(1, std::memory_order_relaxed);
        counters_->quarantined.fetch_add(1, std::memory_order_relaxed);
    }
    if (!quarantine_handler_) return;
    try {
        std::lock_guard<std::mutex> lock(quarantine_mutex_);
        quarantine_handler_(line, reason);
    } catch (...) {
        // A quarantine sink failure must not terminate a client worker thread.
    }
}

void TcpLogReceiver::register_client(TcpNativeSocket socket) {
    std::lock_guard<std::mutex> lock(clients_mutex_);
    active_clients_.push_back(socket);
}

void TcpLogReceiver::unregister_client(TcpNativeSocket socket) noexcept {
    std::lock_guard<std::mutex> lock(clients_mutex_);
    const auto it = std::find(active_clients_.begin(), active_clients_.end(), socket);
    if (it != active_clients_.end()) active_clients_.erase(it);
}

