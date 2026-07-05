# PE_FIX_V1 HYBRID GRAFT PLAN

## Donor
Path: `C:/Users/wigmore/trading_stack/Hyperliquid scanner/_CLEANUP_QUARANTINE_/tier4_deadcode/CUNT_FILES/HL_Copy_App_SSOT.py`
SHA256: 860b130d1a4be5ac0a41d094661f346f737419b0fc82107bfe705af5a5f0e183
Size: 423988 bytes

## Symbols to graft
- `load_ui_state` — donor lines 652..680
- `save_ui_state` — donor lines 683..705

## Insertion point in target
Append grafted symbols at end of target file (Python top-level — safe if functions are not
duplicate names).

## Constants to verify
UI_STATE_FILE: UI_STATE_FILE = BASE_DIR / "ui_state.json"

## Idempotency
Graft script checks AST for each donor symbol in target before copying.