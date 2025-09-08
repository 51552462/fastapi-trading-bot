import os
from typing import Optional, Dict, Any
from fastapi import APIRouter, HTTPException

try:
    from admin_runtime import get_params as _get_params, set_params as _set_params
except Exception:
    _CACHE: Dict[str, Any] = {}
    def _get_params(): return dict(_CACHE)
    def _set_params(patch: Dict[str, Any]):
        _CACHE.update(patch or {}); return patch or {}

router = APIRouter()
_ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")

def _auth(token: Optional[str]):
    if _ADMIN_TOKEN and token != _ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="bad admin token")

@router.get("/admin/params")
def admin_get_params(token: Optional[str] = None):
    _auth(token)
    return {"ok": True, "params": _get_params()}

@router.post("/admin/params")
def admin_set_params(patch: Dict[str, Any] = None, token: Optional[str] = None):
    _auth(token)
    changed = _set_params(patch or {})
    # 트레이더/정책에 런타임 반영(존재할 때만)
    try:
        import trader
        if hasattr(trader, "apply_runtime_overrides"):
            trader.apply_runtime_overrides(changed)
    except Exception:
        pass
    try:
        import tf_policy
        if hasattr(tf_policy, "apply_runtime_overrides"):
            tf_policy.apply_runtime_overrides(changed)
    except Exception:
        pass
    return {"ok": True, "changed": changed}
