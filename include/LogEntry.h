#pragma once

#include <cstddef>
#include <type_traits>

// Fixed-size normalized record. Capacity includes the trailing NUL byte.
// The representation is contiguous and allocation-free for queue transfers.
struct LogEntry {
    char timestamp[32]{};
    char log_level[12]{};
    char host_source[64]{};
    char message[512]{};
};

static_assert(std::is_standard_layout_v<LogEntry>);
static_assert(std::is_trivially_copyable_v<LogEntry>);

