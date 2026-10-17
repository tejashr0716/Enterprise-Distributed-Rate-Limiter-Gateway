#include "rate_limiter.h"
#include <string>
#include <unordered_map>
#include <chrono>
#include <mutex>
#include <atomic>
#include <algorithm>
#include <cstring>

namespace {

inline uint64_t get_now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()
    ).count();
}

struct TokenBucket {
    double capacity;
    double refill_rate_per_sec; // Tokens added per second
    double tokens;
    uint64_t last_update_ns;
    std::mutex mtx;

    TokenBucket(double cap = 100.0, double rate = 10.0, uint64_t now_ns = 0)
        : capacity(cap), refill_rate_per_sec(rate), tokens(cap), last_update_ns(now_ns) {}

    RateLimitResult consume(double requested, uint64_t now_ns, uint64_t start_time) {
        std::lock_guard<std::mutex> lock(mtx);
        
        // Calculate refilled tokens based on elapsed nanoseconds
        double elapsed_sec = (now_ns > last_update_ns) ? 
            (static_cast<double>(now_ns - last_update_ns) / 1e9) : 0.0;
        tokens = std::min(capacity, tokens + (elapsed_sec * refill_rate_per_sec));
        last_update_ns = now_ns;

        RateLimitResult res;
        res.capacity = capacity;
        res.current_timestamp_ns = now_ns;

        if (tokens >= requested) {
            tokens -= requested;
            res.allowed = 1;
            res.remaining_tokens = tokens;
            double needed_to_fill = capacity - tokens;
            res.reset_after_ms = (refill_rate_per_sec > 0) ? 
                static_cast<uint64_t>((needed_to_fill / refill_rate_per_sec) * 1000.0) : 0;
        } else {
            res.allowed = 0;
            res.remaining_tokens = tokens;
            double tokens_deficit = requested - tokens;
            res.reset_after_ms = (refill_rate_per_sec > 0) ? 
                static_cast<uint64_t>((tokens_deficit / refill_rate_per_sec) * 1000.0) : 1000;
        }

        uint64_t end_time = get_now_ns();
        res.latency_ns = end_time - start_time;
        return res;
    }
};

class RateLimiterCore {
private:
    std::unordered_map<std::string, TokenBucket*> buckets_;
    std::mutex map_mutex_;

    // Default parameters for unconfigured clients
    double default_capacity_{100.0};
    double default_refill_rate_{20.0};

    // Telemetry
    std::atomic<uint64_t> total_requests_{0};
    std::atomic<uint64_t> total_allowed_{0};
    std::atomic<uint64_t> total_throttled_{0};
    std::atomic<uint64_t> latency_sum_ns_{0};
    std::atomic<uint64_t> min_latency_ns_{UINT64_MAX};
    std::atomic<uint64_t> max_latency_ns_{0};

public:
    RateLimiterCore() = default;

    ~RateLimiterCore() {
        reset();
    }

    void reset() {
        std::lock_guard<std::mutex> lock(map_mutex_);
        for (auto& pair : buckets_) {
            delete pair.second;
        }
        buckets_.clear();
        total_requests_ = 0;
        total_allowed_ = 0;
        total_throttled_ = 0;
        latency_sum_ns_ = 0;
        min_latency_ns_ = UINT64_MAX;
        max_latency_ns_ = 0;
    }

    void configure_policy(const std::string& client_id, double capacity, double refill_rate) {
        std::lock_guard<std::mutex> lock(map_mutex_);
        auto it = buckets_.find(client_id);
        if (it != buckets_.end()) {
            std::lock_guard<std::mutex> b_lock(it->second->mtx);
            it->second->capacity = capacity;
            it->second->refill_rate_per_sec = refill_rate;
            it->second->tokens = std::min(it->second->tokens, capacity);
            it->second->last_update_ns = get_now_ns();
        } else {
            buckets_[client_id] = new TokenBucket(capacity, refill_rate, get_now_ns());
        }
    }

    RateLimitResult check_and_consume(const std::string& client_id, double requested) {
        uint64_t start_time = get_now_ns();
        total_requests_.fetch_add(1, std::memory_order_relaxed);

        TokenBucket* bucket = nullptr;
        {
            std::lock_guard<std::mutex> lock(map_mutex_);
            auto it = buckets_.find(client_id);
            if (it != buckets_.end()) {
                bucket = it->second;
            } else {
                bucket = new TokenBucket(default_capacity_, default_refill_rate_, start_time);
                buckets_[client_id] = bucket;
            }
        }

        RateLimitResult res = bucket->consume(requested, get_now_ns(), start_time);

        if (res.allowed) {
            total_allowed_.fetch_add(1, std::memory_order_relaxed);
        } else {
            total_throttled_.fetch_add(1, std::memory_order_relaxed);
        }

        latency_sum_ns_.fetch_add(res.latency_ns, std::memory_order_relaxed);
        
        // Update min/max latency
        uint64_t current_min = min_latency_ns_.load(std::memory_order_relaxed);
        while (res.latency_ns < current_min && 
               !min_latency_ns_.compare_exchange_weak(current_min, res.latency_ns, std::memory_order_relaxed)) {}

        uint64_t current_max = max_latency_ns_.load(std::memory_order_relaxed);
        while (res.latency_ns > current_max && 
               !max_latency_ns_.compare_exchange_weak(current_max, res.latency_ns, std::memory_order_relaxed)) {}

        return res;
    }

    void get_metrics(LimiterMetrics* out) {
        if (!out) return;
        uint64_t total = total_requests_.load(std::memory_order_relaxed);
        out->total_requests_evaluated = total;
        out->total_requests_allowed = total_allowed_.load(std::memory_order_relaxed);
        out->total_requests_throttled = total_throttled_.load(std::memory_order_relaxed);
        out->avg_latency_ns = (total > 0) ? (static_cast<double>(latency_sum_ns_.load(std::memory_order_relaxed)) / total) : 0.0;
        uint64_t min_lat = min_latency_ns_.load(std::memory_order_relaxed);
        out->min_latency_ns = (min_lat == UINT64_MAX) ? 0 : min_lat;
        out->max_latency_ns = max_latency_ns_.load(std::memory_order_relaxed);

        std::lock_guard<std::mutex> lock(map_mutex_);
        out->active_buckets_count = buckets_.size();
    }
};

} // namespace

extern "C" {

RateLimiterHandle create_rate_limiter() {
    return reinterpret_cast<RateLimiterHandle>(new RateLimiterCore());
}

void destroy_rate_limiter(RateLimiterHandle handle) {
    if (handle) {
        delete reinterpret_cast<RateLimiterCore*>(handle);
    }
}

void reset_rate_limiter(RateLimiterHandle handle) {
    if (handle) {
        reinterpret_cast<RateLimiterCore*>(handle)->reset();
    }
}

void configure_policy(
    RateLimiterHandle handle,
    const char* client_id,
    double capacity,
    double refill_rate_per_sec
) {
    if (handle && client_id) {
        reinterpret_cast<RateLimiterCore*>(handle)->configure_policy(client_id, capacity, refill_rate_per_sec);
    }
}

RateLimitResult check_and_consume(
    RateLimiterHandle handle,
    const char* client_id,
    double tokens_requested
) {
    if (!handle || !client_id) {
        RateLimitResult err;
        std::memset(&err, 0, sizeof(RateLimitResult));
        err.allowed = 0;
        return err;
    }
    return reinterpret_cast<RateLimiterCore*>(handle)->check_and_consume(client_id, tokens_requested);
}

void get_limiter_metrics(RateLimiterHandle handle, LimiterMetrics* out_metrics) {
    if (handle && out_metrics) {
        reinterpret_cast<RateLimiterCore*>(handle)->get_metrics(out_metrics);
    }
}

}
