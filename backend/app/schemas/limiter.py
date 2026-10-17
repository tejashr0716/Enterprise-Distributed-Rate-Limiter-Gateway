from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime

class RateLimitCheckRequest(BaseModel):
    client_id: str = Field(..., example="CLIENT_ALICE")
    endpoint: str = Field(default="/api/v1/resource", example="/api/v1/payments")
    tokens_requested: float = Field(default=1.0, gt=0, example=1.0)
    ip_address: Optional[str] = Field(default="127.0.0.1", example="192.168.1.100")

class RateLimitCheckResponse(BaseModel):
    allowed: bool
    client_id: str
    remaining_tokens: float
    capacity: float
    reset_after_ms: int
    latency_ns: int
    algorithm: str = "C++ Token Bucket with Nanosecond Precision"
    evaluated_at: datetime

class PolicyCreate(BaseModel):
    client_id: str
    endpoint_pattern: str = "*"
    capacity: float = Field(..., gt=0, example=100.0)
    refill_rate: float = Field(..., gt=0, example=10.0)
    window_seconds: int = Field(default=60, gt=0)

class PolicyResponse(BaseModel):
    policy_id: Optional[int] = None
    client_id: str
    endpoint_pattern: str
    capacity: float
    refill_rate: float
    window_seconds: int
    updated_at: Optional[datetime] = None

class ViolationAuditRecord(BaseModel):
    audit_id: Optional[int] = None
    client_id: str
    endpoint: str
    ip_address: str
    requested_tokens: float
    remaining_tokens: float
    latency_ns: int
    violation_time: datetime
