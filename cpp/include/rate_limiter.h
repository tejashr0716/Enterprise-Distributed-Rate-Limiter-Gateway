#ifndef RATE_LIMITER_H
#define RATE_LIMITER_H

#include <cstdint>
#include <cstddef>

#ifdef __cplusplus
extern "C" {
#endif

#pragma pack(push, 8)
typedef struct {
    int32_t  allowed;              // 1 = allowed, 0 = rejected (429)
    double   remaining_tokens;     // Tokens left in bucket
    double   capacity;             // Max bucket size
    uint64_t reset_after_ms;       // Time until bucket fully refilled (ms)
    uint64_t latency_ns;           // Internal evaluation duration (ns)
    uint64_t current_timestamp_ns; // Nanosecond timestamp of evaluation
} RateLimitResult;

typedef struct {
    uint64_t total_requests_evaluated;
    uint64_t total_requests_allowed;
    uint64_t total_requests_throttled;
    double   avg_latency_ns;
    uint64_t min_latency_ns;
    uint64_t max_latency_ns;
    uint64_t active_buckets_count;
} LimiterMetrics;
#pragma pack(pop)

typedef void* RateLimiterHandle;

RateLimiterHandle create_rate_limiter();
void destroy_rate_limiter(RateLimiterHandle handle);
void reset_rate_limiter(RateLimiterHandle handle);

void configure_policy(
    RateLimiterHandle handle,
    const char* client_id,
    double capacity,
    double refill_rate_per_sec
);

RateLimitResult check_and_consume(
    RateLimiterHandle handle,
    const char* client_id,
    double tokens_requested
);

void get_limiter_metrics(
    RateLimiterHandle handle,
    LimiterMetrics* out_metrics
);

#ifdef __cplusplus
}
#endif

#endif // RATE_LIMITER_H
