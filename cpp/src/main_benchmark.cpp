#include "rate_limiter.h"
#include <iostream>
#include <vector>
#include <numeric>
#include <algorithm>
#include <chrono>
#include <iomanip>

int main() {
    std::cout << "=========================================================\n";
    std::cout << "  GATEKEEPER C++ TOKEN BUCKET RATE LIMITER BENCHMARK     \n";
    std::cout << "=========================================================\n";

    RateLimiterHandle limiter = create_rate_limiter();

    // Configure policies for different client tiers
    configure_policy(limiter, "CLIENT_ENTERPRISE", 500000.0, 50000.0);
    configure_policy(limiter, "CLIENT_PRO", 50000.0, 5000.0);
    configure_policy(limiter, "CLIENT_FREE", 1000.0, 100.0);

    const int TOTAL_REQUESTS = 1000000;
    std::cout << "[*] Evaluating " << TOTAL_REQUESTS << " token-bucket checks across multiple client tiers...\n";

    std::vector<uint64_t> latencies;
    latencies.reserve(TOTAL_REQUESTS);

    const char* clients[] = {"CLIENT_ENTERPRISE", "CLIENT_PRO", "CLIENT_FREE", "CLIENT_ANONYMOUS"};
    int num_clients = 4;

    auto start_wall = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < TOTAL_REQUESTS; ++i) {
        const char* client = clients[i % num_clients];
        RateLimitResult res = check_and_consume(limiter, client, 1.0);
        latencies.push_back(res.latency_ns);
    }

    auto end_wall = std::chrono::high_resolution_clock::now();
    double total_sec = std::chrono::duration<double>(end_wall - start_wall).count();
    double throughput = TOTAL_REQUESTS / total_sec;

    std::sort(latencies.begin(), latencies.end());

    double sum = std::accumulate(latencies.begin(), latencies.end(), 0.0);
    double avg = sum / latencies.size();
    uint64_t p50 = latencies[latencies.size() * 0.50];
    uint64_t p90 = latencies[latencies.size() * 0.90];
    uint64_t p99 = latencies[latencies.size() * 0.99];
    uint64_t p999 = latencies[latencies.size() * 0.999];
    uint64_t min_lat = latencies.front();
    uint64_t max_lat = latencies.back();

    LimiterMetrics metrics;
    get_limiter_metrics(limiter, &metrics);

    std::cout << "\n================= BENCHMARK RESULTS =================\n";
    std::cout << std::fixed << std::setprecision(2);
    std::cout << " Total Evaluations     : " << metrics.total_requests_evaluated << "\n";
    std::cout << " Requests Allowed      : " << metrics.total_requests_allowed << "\n";
    std::cout << " Requests Throttled    : " << metrics.total_requests_throttled << "\n";
    std::cout << " Active Buckets        : " << metrics.active_buckets_count << "\n";
    std::cout << " Total Time            : " << total_sec << " seconds\n";
    std::cout << " Limiter Throughput    : " << throughput << " req/sec (" << (throughput / 1e6) << " M ops/s)\n";
    std::cout << "-----------------------------------------------------\n";
    std::cout << " Min Latency           : " << min_lat << " ns (" << (min_lat / 1000.0) << " µs)\n";
    std::cout << " Mean Latency          : " << avg << " ns (" << (avg / 1000.0) << " µs)\n";
    std::cout << " p50 Latency           : " << p50 << " ns (" << (p50 / 1000.0) << " µs)\n";
    std::cout << " p90 Latency           : " << p90 << " ns (" << (p90 / 1000.0) << " µs)\n";
    std::cout << " p99 Latency           : " << p99 << " ns (" << (p99 / 1000.0) << " µs)\n";
    std::cout << " p99.9 Latency         : " << p999 << " ns (" << (p999 / 1000.0) << " µs)\n";
    std::cout << " Max Latency           : " << max_lat << " ns (" << (max_lat / 1000.0) << " µs)\n";
    std::cout << "=====================================================\n";

    destroy_rate_limiter(limiter);
    return 0;
}
