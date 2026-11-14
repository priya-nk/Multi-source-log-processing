#pragma once

#include "BoundedLockFreeQueue.h"
#include "PipelineCounters.h"

#include <atomic>
#include <cstddef>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
using TcpNativeSocket = SOCKET;
#else
#include <sys/socket.h>
using TcpNativeSocket = int;
#endif

struct RawLogLine {
    std::string source;
    std::string text;
};

// Quarantine callbacks are serialized by TcpLogReceiver. The callback should
// append the raw line and reason to the application's quarantine destination.
using QuarantineLineHandler =
    std::function<void(const RawLogLine&, std::string_view reason)>;

// One thread accepts connections and one worker reads each connected source.
// Each stream is framed independently on '\n'; Queue 1 provides bounded
// backpressure. stop() closes Queue 1 to wake workers blocked in push().
class TcpLogReceiver {
public:
    TcpLogReceiver(BoundedLockFreeQueue<RawLogLine>& queue1,
                   unsigned short port,
                   QuarantineLineHandler quarantine,
                   std::size_t max_line_bytes = 1024 * 1024,
                   PipelineCounters* counters = nullptr);
    ~TcpLogReceiver();

    TcpLogReceiver(const TcpLogReceiver&) = delete;
    TcpLogReceiver& operator=(const TcpLogReceiver&) = delete;

    // Starts listening on all local IPv4 interfaces. May be called once.
    void start();
    // Stops accepting, shuts down active clients, joins workers and closes
    // Queue 1 so its consumers can drain and then observe end-of-stream.
    void stop() noexcept;

private:
    void accept_loop();
    void client_loop(TcpNativeSocket socket, std::string source);
    void quarantine(const RawLogLine& line, std::string_view reason) noexcept;
    void register_client(TcpNativeSocket socket);
    void unregister_client(TcpNativeSocket socket) noexcept;

    BoundedLockFreeQueue<RawLogLine>& queue1_;
    const unsigned short port_;
    const std::size_t max_line_bytes_;
    QuarantineLineHandler quarantine_handler_;
    PipelineCounters* counters_;
    std::atomic<bool> stopping_{false};
    std::atomic<bool> started_{false};
    TcpNativeSocket listen_socket_;
    std::thread accept_thread_;
    std::mutex clients_mutex_;
    std::vector<TcpNativeSocket> active_clients_;
    std::vector<std::thread> client_threads_;
    std::mutex quarantine_mutex_;
};

