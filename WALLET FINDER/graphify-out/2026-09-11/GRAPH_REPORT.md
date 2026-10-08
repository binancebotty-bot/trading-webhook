# Graph Report - WALLET FINDER  (2026-09-11)

## Corpus Check
- 49 files · ~233,586 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1223 nodes · 3626 edges · 60 communities (54 shown, 6 thin omitted)
- Extraction: 99% EXTRACTED · 1% INFERRED · 0% AMBIGUOUS · INFERRED: 27 edges (avg confidence: 0.54)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `509d3580`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- EngineSSOT
- fnum
- IpWeightBudget
- get_portfolio_data
- hl_rate_guard.py
- 3_mtm_filter.py
- get_table_rows
- universe_builder.py
- Any
- effective_wallet_ui
- get_table_rows
- app.py
- 4_useful_wallet_scanner.py
- 1WalletFinder.py
- JSONResponse
- get
- load_universe
- HL_Copy_App_SSOT.py
- _run_harness.py
- update_last_trade_times
- test_import_copy_candidates.py
- get
- get_portfolio_data
- _table_context
- test_universe_builder.py
- _save_state
- test_pipeline_policy.py
- 5_reconstructed_drawdown.py
- load_universe
- wallet_talent_scout_8012.py
- _w2_w3_avh_probe.py
- test_ssot_backoff.py
- test_true_dd_windowing.py
- get
- stop_all.sh
- verify_active_wallet_proof_ui.py
- __init__.py
- start_all.sh
- _load_state
- post_with_weight_budget_preacquired
- test_talent_scout_global_sort.py
- test_sizing_normalisation.py
- load_json
- wallet_proof_engine_8014.py
- poll_wallet_forward_adaptive
- start_wallet_finder.ps1
- get_adaptive_window_ms
- hl_ip_budget.py
- _on_startup
- _Recorder
- _load_state
- test_talent_scout_selection_responsiveness.py
- apply_live_config_wallet_model
- SharedBudgetUnavailable

## God Nodes (most connected - your core abstractions)
1. `fnum()` - 84 edges
2. `build_model_state()` - 46 edges
3. `EngineSSOT` - 44 edges
4. `_live_audit_summary()` - 41 edges
5. `inum()` - 35 edges
6. `load_json()` - 30 edges
7. `load_ui_state()` - 29 edges
8. `render_home()` - 25 edges
9. `save_ui_state()` - 24 edges
10. `render_row()` - 24 edges

## Surprising Connections (you probably didn't know these)
- `_last_trade_ms_from_engine_truth()` --calls--> `iso_to_ms()`  [INFERRED]
  wallet_proof_engine_8014.py → HL_Copy_Engine_SSOT.py
- `monitor_start_ms()` --calls--> `iso_to_ms()`  [INFERRED]
  wallet_proof_engine_8014.py → HL_Copy_Engine_SSOT.py
- `RateGuard` --uses--> `SharedBudgetUnavailable`  [INFERRED]
  hl_rate_guard.py → hl_ip_budget.py
- `test_scout_metric_filters_ignore_copy_engine_wallet_exclusions()` --calls--> `_apply_filters()`  [EXTRACTED]
  tests/test_talent_scout_global_sort.py → wallet_talent_scout_8012.py
- `test_portfolio_remove_queues_refresh_instead_of_computing_inline()` --calls--> `unselect_wallet()`  [EXTRACTED]
  tests/test_talent_scout_selection_responsiveness.py → wallet_talent_scout_8012.py

## Import Cycles
- None detected.

## Communities (60 total, 6 thin omitted)

### Community 0 - "EngineSSOT"
Cohesion: 0.06
Nodes (38): datetime, acquire_lock(), atomic_write_json(), bval(), chunked(), CsvLedger, EngineSSOT, ensure_dirs() (+30 more)

### Community 1 - "fnum"
Cohesion: 0.06
Nodes (77): active_wallet(), active_wallet_row(), age_ms_label(), _append_exchange_history(), apply_user_true_drawdown_rollup(), block(), build_computed_true_drawdown_history(), build_portfolio_history() (+69 more)

### Community 2 - "IpWeightBudget"
Cohesion: 0.19
Nodes (9): IpWeightBudget, A rolling weight window in a file, shared by every participant. ``reserve`` is…, Hold the machine-wide lock, or raise inside the caller's bound. The lock lives…, Read the committed window and say whether it can be believed. Returns…, Publish the window atomically. Write-then-replace, never truncate-then-write. A…, Weight reserved by every participant in the current window., Atomically reserve ``weight`` if the window can carry it. ``floor`` is weight…, Readable state for the HEALTH display and for tests. (+1 more)

### Community 3 - "get_portfolio_data"
Cohesion: 0.24
Nodes (19): _apply_date_range_to_series(), _curve_metrics(), _display_ssot_dd(), _ensure_trade_replay_series(), get_portfolio_data(), _get_wallet_pnl_series(), _load_curve(), _load_trade_replay_pnl_series() (+11 more)

### Community 4 - "hl_rate_guard.py"
Cohesion: 0.18
Nodes (12): guard(), RateGuard, Per-process Hyperliquid request budget for the WALLET FINDER products. Three…, Try to claim budget for one request. False means skip this one. ``timeout_s``…, A rolling-window ceiling for one process, plus the shared IP budget., reset_for_tests(), shared_budget(), weight_for() (+4 more)

### Community 5 - "3_mtm_filter.py"
Cohesion: 0.07
Nodes (51): ClientSession, Path, Path, Semaphore, fetch_mtm(), gate_mtm(), load_progress(), load_wallets() (+43 more)

### Community 6 - "get_table_rows"
Cohesion: 0.15
Nodes (17): _activity_bucket(), _build_sparkline(), _fmt(), get_table_rows(), _is_dormant(), _is_slow(), _is_stale(), _last_trade_age_fmt() (+9 more)

### Community 7 - "universe_builder.py"
Cohesion: 0.13
Nodes (29): build_universe_rows(), compute_wallet_metrics(), dedup_trades_file(), flush_universe(), is_garbage(), load_purged_wallets(), load_trades_index(), load_valid_wallets() (+21 more)

### Community 8 - "Any"
Cohesion: 0.08
Nodes (60): Any, _active_live_copy_wallet_count(), _age_label_from_ms(), _build_account_reconciliation(), _build_execution_quality_freshness(), _build_execution_quality_rows(), _build_execution_quality_summary(), _build_live_leader_performance() (+52 more)

### Community 9 - "effective_wallet_ui"
Cohesion: 0.16
Nodes (17): _account_value_curve(), api_wallet_equity_curve(), copy_price_for_fill(), _copyable_model_fill(), _current_leader_equity_from_cache(), effective_wallet_ui(), fill_can_measure_execution_delta(), model_copy_notional() (+9 more)

### Community 10 - "get_table_rows"
Cohesion: 0.11
Nodes (21): _activity_bucket(), _build_sparkline(), _compute_compound_score(), _fmt(), get_table_rows(), _is_dormant(), _is_slow(), _is_stale() (+13 more)

### Community 11 - "app.py"
Cohesion: 0.16
Nodes (19): data_status(), export_selected(), _load_mtm_account_value_series(), _mtm_period_candidates(), _mtm_period_for_days(), _parse_filter(), proof_wallets_txt(), _prune_selection_to_universe() (+11 more)

### Community 12 - "4_useful_wallet_scanner.py"
Cohesion: 0.06
Nodes (37): Path, cache_path(), extract_wallet_stats(), fetch_portfolio(), load_source_wallets(), main(), stage1_simple.py â€” The ONLY filter stage before the deep-dive trade count chec, Fetch portfolio data for one wallet with retries. Returns dict or None. (+29 more)

### Community 13 - "1WalletFinder.py"
Cohesion: 0.22
Nodes (21): append_csv(), get_coins(), init_csv(), inject_leaderboard_wallets(), load_existing_wallets(), main(), poll_worker(), post() (+13 more)

### Community 14 - "JSONResponse"
Cohesion: 0.12
Nodes (57): add_live_config_wallet(), admin_purge_wallet(), api_get_ui_state(), api_import_copy_candidates(), api_norm(), api_set_ui_state(), api_state(), api_wallet_meta() (+49 more)

### Community 15 - "get"
Cohesion: 0.11
Nodes (28): _compute_compound_score(), data_status(), export_selected(), healthz(), last_trade_events(), _martingale_flag_reliable(), _penalty_flags(), _penalty_flags_title() (+20 more)

### Community 16 - "load_universe"
Cohesion: 0.10
Nodes (28): _apply_filters(), _current_table_days(), data_status_card(), _find_all_trades_path(), _get_visible_universe(), _load_last_trade_times(), _load_purged_wallets(), _load_summary_kpis() (+20 more)

### Community 18 - "_run_harness.py"
Cohesion: 0.19
Nodes (19): audit_for(), dd_from_pts(), fetch_ch(), fetch_funding_72h(), fetch_ledger_72h(), fetch_portfolio(), fetch_user_fills_72h(), http_post() (+11 more)

### Community 19 - "update_last_trade_times"
Cohesion: 0.25
Nodes (15): _file_identity(), _normalise_baseline(), Path, Incremental last-trade timestamp indexing for the 8012 dashboard.  The trade led, Return latest wallet timestamps plus a resumable ledger cursor.      A previous, update_last_trade_times(), Path, _row() (+7 more)

### Community 21 - "test_import_copy_candidates.py"
Cohesion: 0.50
Nodes (3): Regression coverage for the 8012 -> 8014 candidate import boundary., test_import_does_not_consult_true_dd_or_model_state(), _wallet()

### Community 22 - "get"
Cohesion: 0.24
Nodes (29): apply_filter(), clear_all(), clear_filter(), data_status_card(), _get_sort(), portfolio_panel(), get, HTMLResponse (+21 more)

### Community 23 - "get_portfolio_data"
Cohesion: 0.17
Nodes (24): _apply_date_range_to_series(), _curve_metrics(), _display_ssot_dd(), _ensure_trade_replay_series(), get_portfolio_data(), _get_wallet_pnl_series(), _install_replay_payload(), _load_best_replay_snapshot() (+16 more)

### Community 24 - "_table_context"
Cohesion: 0.15
Nodes (16): _active_filter_count(), _activity_counts(), index(), Return active (<=3d), slow (3d-1w), stale (1w-15d), and dormant (>15d) counts., render(), _render_filter_bar(), _table_context(), Regression test: Table count fix for WALLET FINDER.  Verifies: 1. per_page in st (+8 more)

### Community 25 - "test_universe_builder.py"
Cohesion: 0.11
Nodes (28): _fake_adaptive_window(), _FakePost, _mock_response(), _mock_session(), _noop_budget(), Unit tests for universe_builder.py fixes: 1. WeightBudget.release — adds weight…, On 429: re-acquire on attempts 0..(retries-2), skip on last attempt., On exception: re-acquire on non-last attempts, skip on last. (+20 more)

### Community 26 - "_save_state"
Cohesion: 0.25
Nodes (26): apply_filter(), clear_all(), clear_filter(), _get_sort(), portfolio_panel(), HTMLResponse, post, Request (+18 more)

### Community 27 - "test_pipeline_policy.py"
Cohesion: 0.12
Nodes (13): load_script(), 8012 candidates must enter 8014 before TRUE DD/readiness evaluation., 8012 keeps its original scout filters separate from 8014 promotion gates., Restored 8012 displays the original Real-DD scout column, not TRUE/SCOUT DD., 8012 has no 8014 TRUE DD promotion gate or reconstructed TRUE column pollution., test_8012_filters_are_invariant_to_normalization(), test_8012_realised_dd_display_is_not_mtm_capped(), test_8012_sparse_mtm_is_floored_by_realised_dd_for_risk() (+5 more)

### Community 28 - "5_reconstructed_drawdown.py"
Cohesion: 0.19
Nodes (14): Path, assess_status(), compute_reconstructed_curve(), _empty_candidate(), load_copy_ready_wallets(), load_trades_for_wallets(), main(), 5_reconstructed_drawdown.py — WALLET FINDER EDITION Stage 5: Reconstructed Draw (+6 more)

### Community 29 - "load_universe"
Cohesion: 0.11
Nodes (23): _apply_filters(), _current_table_days(), _find_all_trades_path(), _get_visible_universe(), _load_last_trade_times(), _load_purged_wallets(), _load_summary_kpis(), _load_trade_stats() (+15 more)

### Community 30 - "wallet_talent_scout_8012.py"
Cohesion: 0.13
Nodes (22): Lock, test_global_sort_happens_before_pagination_and_preserves_global_rank(), _active_filter_count(), _activity_counts(), _get_last_trade_lock(), _get_portfolio_data_async(), index(), _load_mtm_account_value_series() (+14 more)

### Community 31 - "_w2_w3_avh_probe.py"
Cohesion: 0.57
Nodes (7): dd_from_pts(), fetch_avh(), fetch_ledger(), local_equity_curve_full(), local_summary_for(), main(), post()

### Community 32 - "test_ssot_backoff.py"
Cohesion: 0.22
Nodes (14): _drifting_snapshot(), _engine(), The tracker must not re-ask the exchange a question it already answered. This…, Backing off is not the same as hiding it., Backoff is about absence of evidence, not about elapsed time., Under a ceiling a sweep may not finish; it must not always give up at the same…, The whole incident in one assertion., test_a_clean_wallet_is_never_investigated_at_all() (+6 more)

### Community 33 - "test_true_dd_windowing.py"
Cohesion: 0.23
Nodes (6): _point(), Regression coverage for 8014 proof-window TRUE DD and dashboard caching., test_portfolio_true_dd_cannot_admit_a_zero_fill_wallets_old_history(), test_true_dd_carries_a_quiet_baselined_wallet_forward_as_valid_zero(), test_true_dd_never_falls_back_to_full_history_without_a_proof_window(), test_true_dd_uses_only_the_supplied_proof_window()

### Community 34 - "get"
Cohesion: 0.06
Nodes (58): Response, api_equity(), api_metrics(), api_metrics_csv(), api_trades(), apply_wallet_filters_to_state(), block_num(), dd_current() (+50 more)

### Community 35 - "stop_all.sh"
Cohesion: 0.83
Nodes (3): stop_all.sh script, stop_by_pid_file(), stop_by_port()

### Community 44 - "_load_state"
Cohesion: 0.33
Nodes (7): _latest_state_with_selection(), _load_state(), on_event, Recover Talent Scout-owned selection keys from the latest rotating backup., Load state from disk. Falls back to .bak if main file is corrupt. Migration: if…, _selected_true_count(), startup()

### Community 45 - "post_with_weight_budget_preacquired"
Cohesion: 0.15
Nodes (13): release() must add weight once, NOT double-count., release() must not exceed burst limit., Multiple concurrent releases must sum correctly (no double-count)., test_release_adds_weight_exactly_once(), test_release_caps_at_burst(), test_release_concurrent_safety(), _parse_retry_after(), post() (+5 more)

### Community 46 - "test_talent_scout_global_sort.py"
Cohesion: 0.18
Nodes (7): _FakeKernel32, _FakeWinFunction, isolated_table_state(), fixture, parametrize, test_displayed_metric_sort_uses_full_wallet_universe(), test_scout_metric_filters_ignore_copy_engine_wallet_exclusions()

### Community 48 - "test_sizing_normalisation.py"
Cohesion: 0.24
Nodes (6): RawFill, _fill(), Regression coverage for fill-time proportional sizing and USER refresh., test_explicit_wallet_equity_override_remains_fixed(), test_proportional_sizing_uses_leader_equity_at_each_fill(), test_unlocked_saved_equity_value_does_not_freeze_sizing()

### Community 49 - "load_json"
Cohesion: 0.09
Nodes (39): _account_orphan_signed_size(), _account_reconciliation_baseline_timestamp(), api_cache_health(), api_engine_health(), api_model_cache_status(), _apply_ownership_truth_to_integrity(), _build_account_orphan_registry(), cancel_recon_repair_request() (+31 more)

### Community 51 - "wallet_proof_engine_8014.py"
Cohesion: 0.06
Nodes (66): alignment_status_for_key(), _apply_price_model(), atomic_write_csv(), atomic_write_json(), _audit_summary_background_rebuild(), avg(), backup_purge_files(), bps_fee() (+58 more)

### Community 52 - "poll_wallet_forward_adaptive"
Cohesion: 0.31
Nodes (10): append_all_trades(), estimate_request_weight(), log(), poll_wallet_forward(), poll_wallet_forward_adaptive(), post_with_extended_recovery(), Estimate API weight for a response with N items., 64-bit hash of the full 7-field canonical key. Hashing keeps the per-wallet… (+2 more)

### Community 53 - "start_wallet_finder.ps1"
Cohesion: 0.60
Nodes (5): Get-LogPaths(), Start-Dash(), Start-Feeder(), Test-PortUp(), Test-ScriptRunning()

### Community 54 - "get_adaptive_window_ms"
Cohesion: 0.33
Nodes (6): calculate_optimal_window(), get_adaptive_window_ms(), get_peak_24h_trades(), Get actual peak trades in any 24h window from local trade data (last 30d)., Calculate optimal POLL_WINDOW_MS for a wallet based on peak 24h trades. Target:…, Calculate adaptive window using actual peak 24h trades from local data.

### Community 55 - "hl_ip_budget.py"
Cohesion: 0.22
Nodes (7): default_budget_path(), One weight budget shared by every local Hyperliquid consumer on this IP.…, The process-wide handle to the shared budget, or None if not configured. Absent…, A machine-wide default location, used by the launcher wiring., One non-blocking attempt at the exclusive lock. Deliberately non-blocking on…, shared_budget(), _try_lock_file()

### Community 59 - "_load_state"
Cohesion: 0.29
Nodes (7): _latest_state_with_selection(), _load_state(), on_event, Recover Talent Scout-owned selection keys from the latest rotating backup., Load state from disk. Falls back to .bak if main file is corrupt. Migration: if…, _selected_true_count(), startup()

### Community 60 - "test_talent_scout_selection_responsiveness.py"
Cohesion: 0.20
Nodes (7): _FakeKernel32, _FakeWinFunction, isolated_selection_state(), fixture, parametrize, test_checkbox_selection_returns_without_inline_portfolio_compute(), test_portfolio_remove_queues_refresh_instead_of_computing_inline()

### Community 61 - "apply_live_config_wallet_model"
Cohesion: 0.50
Nodes (4): apply_live_config_wallet_model(), _clean_wallet_config(), Sanitise optional per-wallet model overrides. Empty/missing wallet config…, Let the 8014 live wallet controls drive app-model replay sizing. The engine…

### Community 63 - "SharedBudgetUnavailable"
Cohesion: 0.40
Nodes (5): The shared budget could not be consulted within its bound. Deliberately…, SharedBudgetUnavailable, RuntimeError, legacy_main_pre_intake_repair(), Disabled legacy entrypoint; retained only for import compatibility.

## Knowledge Gaps
- **5 isolated node(s):** `Path`, `start_all.sh script`, `_FakeKernel32`, `_FakeKernel32`, `Path`
  These have ≤1 connection - possible missing edges or undocumented components.
- **6 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Any` connect `Any` to `EngineSSOT`, `fnum`, `get`, `effective_wallet_ui`, `1WalletFinder.py`, `JSONResponse`, `load_json`, `update_last_trade_times`, `wallet_proof_engine_8014.py`, `apply_live_config_wallet_model`?**
  _High betweenness centrality (0.127) - this node is a cross-community bridge._
- **Why does `_summarise()` connect `3_mtm_filter.py` to `app.py`, `4_useful_wallet_scanner.py`, `load_universe`, `load_universe`, `wallet_talent_scout_8012.py`?**
  _High betweenness centrality (0.071) - this node is a cross-community bridge._
- **Why does `update_last_trade_times()` connect `update_last_trade_times` to `Any`, `load_universe`, `wallet_talent_scout_8012.py`?**
  _High betweenness centrality (0.054) - this node is a cross-community bridge._
- **What connects `Path`, `start_all.sh script`, `_FakeKernel32` to the rest of the system?**
  _5 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `EngineSSOT` be split into smaller, more focused modules?**
  _Cohesion score 0.06422466422466422 - nodes in this community are weakly interconnected._
- **Should `fnum` be split into smaller, more focused modules?**
  _Cohesion score 0.056049213943950786 - nodes in this community are weakly interconnected._
- **Should `3_mtm_filter.py` be split into smaller, more focused modules?**
  _Cohesion score 0.06801346801346801 - nodes in this community are weakly interconnected._