from fastapi import APIRouter, HTTPException
from ..schemas.limiter import PolicyCreate, PolicyResponse
from ..services.database_service import db_service
from ..services.cpp_limiter import cpp_limiter

router = APIRouter(prefix="/policies", tags=["Policies"])

@router.get("/{client_id}", response_model=PolicyResponse)
async def get_policy(client_id: str, endpoint: str = "*"):
    pol = await db_service.get_client_policy(client_id, endpoint)
    return PolicyResponse(**pol)

@router.post("", response_model=PolicyResponse)
async def create_or_update_policy(policy_req: PolicyCreate):
    await db_service.upsert_policy(
        client_id=policy_req.client_id,
        endpoint_pattern=policy_req.endpoint_pattern,
        capacity=policy_req.capacity,
        refill_rate=policy_req.refill_rate,
        window_seconds=policy_req.window_seconds
    )
    cpp_limiter.configure_policy(
        client_id=policy_req.client_id,
        capacity=policy_req.capacity,
        refill_rate=policy_req.refill_rate
    )
    return PolicyResponse(
        client_id=policy_req.client_id,
        endpoint_pattern=policy_req.endpoint_pattern,
        capacity=policy_req.capacity,
        refill_rate=policy_req.refill_rate,
        window_seconds=policy_req.window_seconds
    )
