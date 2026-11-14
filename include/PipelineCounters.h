#pragma once

#include <atomic>
#include <cstdint>

struct PipelineCounters {
    std::atomic<std::uint64_t> received{0};
    std::atomic<std::uint64_t> written{0};
    std::atomic<std::uint64_t> quarantined{0};
};
