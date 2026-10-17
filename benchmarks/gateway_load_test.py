import asyncio
import time
import statistics
from httpx import AsyncClient, ASGITransport
from backend.app.main import app
from backend.app.services.database_service import db_service
from backend.app.services.redis_limiter import redis_limiter
from backend.app.services.rabbitmq_service import rabbitmq_service

TOTAL_REQUESTS = 2500
CONCURRENT_WORKERS = 50

async def worker_task(worker_id: int, requests_per_worker: int, latencies: list, status_codes: dict):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test", timeout=30.0) as client:
        clients = ["CLIENT_ALICE", "CLIENT_BOB", "CLIENT_ACME_CORP", "CLIENT_ABUSER"]
        client_id = clients[worker_id % len(clients)]

        for _ in range(requests_per_worker):
            t0 = time.perf_counter()
            try:
                res = await client.get(
                    "/api/v1/gateway/proxy/resource",
                    headers={"X-Client-ID": client_id}
                )
                t1 = time.perf_counter()
                dur_ms = (t1 - t0) * 1000.0
                latencies.append(dur_ms)
                status_codes[res.status_code] = status_codes.get(res.status_code, 0) + 1
            except Exception:
                pass

async def run_benchmark():
    print("=====================================================================")
    print("  GATEKEEPER DISTRIBUTED API GATEWAY CONCURRENT LOAD TEST             ")
    print("=====================================================================")
    print(f"[*] Target Requests    : {TOTAL_REQUESTS}")
    print(f"[*] Concurrent Workers : {CONCURRENT_WORKERS}")
    print("[*] Connecting services (Postgres, Redis, RabbitMQ, C++ Core)...")

    await db_service.connect()
    await redis_limiter.connect()
    await rabbitmq_service.connect()

    latencies = []
    status_codes = {}
    requests_per_worker = TOTAL_REQUESTS // CONCURRENT_WORKERS

    start_wall = time.perf_counter()
    tasks = [
        asyncio.create_task(worker_task(w_id, requests_per_worker, latencies, status_codes))
        for w_id in range(CONCURRENT_WORKERS)
    ]
    await asyncio.gather(*tasks)
    end_wall = time.perf_counter()

    total_sec = end_wall - start_wall
    throughput = len(latencies) / total_sec

    latencies.sort()
    p50 = statistics.median(latencies)
    p90 = latencies[int(len(latencies) * 0.90)]
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]
    mean_lat = statistics.mean(latencies)

    print("\n======================= BENCHMARK RESULTS =======================")
    print(f" Total Requests Ingested : {len(latencies):,}")
    print(f" Status 200 OK (Allowed) : {status_codes.get(200, 0):,}")
    print(f" Status 429 (Throttled)  : {status_codes.get(429, 0):,}")
    print(f" Total Elapsed Time      : {total_sec:.3f} seconds")
    print(f" Gateway Throughput      : {throughput:,.2f} req/sec")
    print("-----------------------------------------------------------------")
    print(f" Mean Latency            : {mean_lat:.2f} ms")
    print(f" p50 Latency             : {p50:.2f} ms")
    print(f" p90 Latency             : {p90:.2f} ms")
    print(f" p95 Latency             : {p95:.2f} ms")
    print(f" p99 Latency             : {p99:.2f} ms")
    print("=================================================================\n")

    await db_service.disconnect()
    await redis_limiter.disconnect()
    await rabbitmq_service.disconnect()

if __name__ == "__main__":
    asyncio.run(run_benchmark())
