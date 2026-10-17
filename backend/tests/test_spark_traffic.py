import pytest
from datetime import datetime
from spark.rate_limit_traffic_analytics import create_spark_session, analyze_gateway_traffic

def test_spark_traffic_analytics():
    spark = create_spark_session("GateKeeper-Test-Spark")
    try:
        now = datetime.utcnow()
        sample = [
            (101, "CLIENT_ATTACKER", "/login", 429, 85, now),
            (102, "CLIENT_ATTACKER", "/login", 429, 90, now),
            (103, "CLIENT_LEGIT", "/profile", 200, 95, now),
        ]
        results = analyze_gateway_traffic(spark, sample)
        client_rows = results["clients"].collect()
        assert len(client_rows) == 2
        
        attacker = [r for r in client_rows if r["client_id"] == "CLIENT_ATTACKER"][0]
        assert attacker["throttled_429"] == 2
        assert attacker["throttle_rate_pct"] == 100.0

        legit = [r for r in client_rows if r["client_id"] == "CLIENT_LEGIT"][0]
        assert legit["allowed_200"] == 1
        assert legit["throttle_rate_pct"] == 0.0
    finally:
        spark.stop()
