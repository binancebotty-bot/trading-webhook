import importlib.util
import json
import pathlib
import sys

root = pathlib.Path(r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER")
p = root / "HL_Copy_App_SSOT.py"
spec = importlib.util.spec_from_file_location("hl_app_rebuild", p)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)
state = mod.build_model_state()
errs = mod.validate_render_contract(state)
print(json.dumps({
    "updated_at": state.get("updated_at"),
    "wallet_rows": len(state.get("wallet_rows") or []),
    "errors": len(errs),
    "first_errors": errs[:20],
}, indent=2))
