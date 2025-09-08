import os
def policy_mode() -> str:
    return str(os.getenv("POLICY_MODE", "full")).strip().lower()
def minimal_guard_active() -> bool:
    return policy_mode() == "minimal"
def skip_policy_cut() -> bool:
    return minimal_guard_active()
