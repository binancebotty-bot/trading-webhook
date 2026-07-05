# SSOT Council Summary

Question: How do we restore HL_Copy_App_SSOT.py so visible equity, realised/unrealised, allocation, drawdown, maxDD, header, row, API, and warning values are produced from one canonical model/aggregate instead of duplicated formulas?

## Consensus
- Current live file has been restored to the Phase 1 recovered baseline and should be treated as the safety baseline.
- The incomplete Phase 2 broke the app by splitting ownership between build_model_state and render_home. That approach must not continue.
- render_home() must become presentation-only. It must not call selected_aggregate(), rebuild portfolio_history, recompute DD/maxDD, construct a local portfolio object, or mutate USER row display data as accounting truth.
- Any correct copy allocation and unified DD logic must move into the canonical model build path, then render/API/warnings must read that same state object.

## Canonical Owners
- _compute_copy_alloc(...): copy allocation sizing, using UI sizing fields.
- WalletModel.sync_equity(): per-wallet lead/copy alloc, realised, unrealised, equity, peak, current DD, maxDD.
- build_model_state(): constructs canonical wallet rows, selected rows, selected aggregate, portfolio, portfolio_history, USER aggregate row.
- selected_aggregate(...): should be promoted to own the complete selected portfolio bundle, not only scalar trade sums.
- build_portfolio_history(...) or a private helper under selected aggregate: owns unified selected portfolio curves and DD/maxDD.
- render_home(): reads state["portfolio"], state["portfolio_history"], state["wallet_rows"]. It formats only.
- /api/state: exports the same canonical state, or a bounded lightweight canonical export for proof.
- validate_render_contract(): validates canonical state consistency; it must not become another formula engine.

## Immediate Recovery Done
- Broken Phase 2 live file was frozen.
- HL_Copy_App_SSOT.py was restored from proofs\RECOVERED_BASELINE_8014_20260630_171220\HL_Copy_App_SSOT.py.recovered_baseline.
- 8014 was restarted with python -B HL_Copy_App_SSOT.py.
- / and /?sync_copy=1 return HTTP 200 and no longer stay on Building model state.

## Next Safe Patch Shape
1. Add/adjust canonical selected portfolio builder in model path only.
2. Make state["portfolio"], state["portfolio_history"], and USER row the only sources for header/table/API/warnings.
3. Remove/bypass render-side aggregate/DD/accounting formulas.
4. Add a lightweight bounded proof/export that reads the cached canonical model without triggering a full rebuild.
5. Only then fix copy allocation and unified DD in the canonical builder.

## Blocked Actions
- Do not commit.
- Do not resume incomplete Phase 2 patch.
- Do not run PE_FIX scripts.
- Do not touch LIVE WALLET TRADING.
