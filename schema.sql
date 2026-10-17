-- GateKeeper Distributed Rate Limiter & API Gateway Schema
-- PostgreSQL / Azure SQL Compatible

DROP TABLE IF EXISTS gateway_traffic_metrics CASCADE;
DROP TABLE IF EXISTS violation_audits CASCADE;
DROP TABLE IF EXISTS rate_limit_policies CASCADE;
DROP TABLE IF EXISTS api_keys CASCADE;
DROP TABLE IF EXISTS client_tiers CASCADE;

CREATE TABLE client_tiers (
    tier_id VARCHAR(32) PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    default_capacity NUMERIC(12, 2) NOT NULL,
    default_refill_rate NUMERIC(12, 2) NOT NULL, -- Tokens per second
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE api_keys (
    api_key VARCHAR(128) PRIMARY KEY,
    client_id VARCHAR(64) NOT NULL,
    tier_id VARCHAR(32) NOT NULL REFERENCES client_tiers(tier_id),
    status VARCHAR(16) DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_api_keys_client ON api_keys(client_id);

CREATE TABLE rate_limit_policies (
    policy_id BIGSERIAL PRIMARY KEY,
    client_id VARCHAR(64) NOT NULL,
    endpoint_pattern VARCHAR(128) NOT NULL DEFAULT '*',
    capacity NUMERIC(12, 2) NOT NULL,
    refill_rate NUMERIC(12, 2) NOT NULL,
    window_seconds INT NOT NULL DEFAULT 60,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT unique_client_endpoint UNIQUE (client_id, endpoint_pattern)
);

CREATE TABLE violation_audits (
    audit_id BIGSERIAL PRIMARY KEY,
    client_id VARCHAR(64) NOT NULL,
    endpoint VARCHAR(128) NOT NULL,
    ip_address VARCHAR(45) NOT NULL,
    requested_tokens NUMERIC(8, 2) NOT NULL,
    remaining_tokens NUMERIC(12, 2) NOT NULL,
    latency_ns BIGINT NOT NULL,
    violation_time TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_violation_client_time ON violation_audits(client_id, violation_time DESC);

CREATE TABLE gateway_traffic_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    client_id VARCHAR(64) NOT NULL,
    endpoint VARCHAR(128) NOT NULL,
    status_code INT NOT NULL,
    latency_ns BIGINT NOT NULL,
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed Default Tiers and Client Keys
INSERT INTO client_tiers (tier_id, name, default_capacity, default_refill_rate)
VALUES
    ('FREE', 'Free Tier Sandbox', 60.0, 1.0),            -- 60 req burst, 1 req/s refill
    ('PRO', 'Professional Developer', 1000.0, 50.0),     -- 1,000 req burst, 50 req/s refill
    ('ENTERPRISE', 'High Volume Enterprise', 50000.0, 2000.0) -- 50,000 req burst, 2,000 req/s refill
ON CONFLICT (tier_id) DO NOTHING;

INSERT INTO api_keys (api_key, client_id, tier_id, status)
VALUES
    ('gk_live_alice_free_123', 'CLIENT_ALICE', 'FREE', 'ACTIVE'),
    ('gk_live_bob_pro_456', 'CLIENT_BOB', 'PRO', 'ACTIVE'),
    ('gk_live_corp_ent_789', 'CLIENT_ACME_CORP', 'ENTERPRISE', 'ACTIVE'),
    ('gk_live_attacker_000', 'CLIENT_ABUSER', 'FREE', 'ACTIVE')
ON CONFLICT (api_key) DO NOTHING;

INSERT INTO rate_limit_policies (client_id, endpoint_pattern, capacity, refill_rate, window_seconds)
VALUES
    ('CLIENT_ALICE', '*', 60.0, 1.0, 60),
    ('CLIENT_BOB', '*', 1000.0, 50.0, 60),
    ('CLIENT_ACME_CORP', '*', 50000.0, 2000.0, 60),
    ('CLIENT_ABUSER', '*', 10.0, 0.2, 60)
ON CONFLICT (client_id, endpoint_pattern) DO NOTHING;
