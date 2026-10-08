# Graph Report - WALLET FINDER  (2026-09-13)

## Corpus Check
- 49 files · ~233,377 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1235 nodes · 3686 edges · 65 communities (61 shown, 4 thin omitted)
- Extraction: 99% EXTRACTED · 1% INFERRED · 0% AMBIGUOUS · INFERRED: 30 edges (avg confidence: 0.54)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `509d3580`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- EngineSSOT
- Any
- IpWeightBudget
- get_portfolio_data
- hl_rate_guard.py
- 3_mtm_filter.py
- get_table_rows
- universe_builder.py
- get
- block_num
- get_table_rows
- app.py
- 4_useful_wallet_scanner.py
- 1WalletFinder.py
- load_ui_state
- get
- load_universe
- get_live_audit_summary
- _run_harness.py
- update_last_trade_times
- atomic_write_json
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
- fnum
- stop_all.sh
- verify_active_wallet_proof_ui.py
- __init__.py
- start_all.sh
- _load_state
- post_with_weight_budget_preacquired
- test_talent_scout_global_sort.py
- _load_live_copy_config
- test_sizing_normalisation.py
- wallet_proof_engine_8014.py
- build_model_state
- poll_wallet_forward_adaptive
- start_wallet_finder.ps1
- get_adaptive_window_ms
- hl_ip_budget.py
- _Recorder
- _fetch_exchange_account_snapshot
- _load_state
- test_talent_scout_selection_responsiveness.py
- JSONResponse
- _model_dashboard_response
- SharedBudgetUnavailable
- inum
- last_trade_events

## God Nodes (most connected - your core abstractions)
1. `fnum()` - 86 edges
2. `build_model_state()` - 47 edges
3. `EngineSSOT` - 44 edges
4. `_live_audit_summary()` - 41 edges
5. `inum()` - 34 edges
6. `load_json()` - 30 edges
7. `load_ui_state()` - 29 edges
8. `render_home()` - 25 edges
9. `save_ui_state()` - 24 edges
10. `parse_bool()` - 24 edges

## Surprising Connections (you probably didn't know these)
- `_last_trade_ms_from_engine_truth()` --calls--> `iso_to_ms()`  [INFERRED]
  wallet_proof_engine_8014.py → HL_Copy_Engine_SSOT.py
- `monitor_start_ms()` --calls--> `iso_to_ms()`  [INFERRED]
  wallet_proof_engine_8014.py → HL_Copy_Engine_SSOT.py
- `RateGuard` --uses--> `SharedBudgetUnavailable`  [INFERRED]
  hl_rate_guard.py → hl_ip_budget.py
- `test_wallet_config_endpoint_persists_lock_and_starts_rebuild()` --calls--> `set_wallet_config()`  [EXTRACTED]
  tests/test_sizing_normalisation.py → wallet_proof_engine_8014.py
- `test_dashboard_request_queues_refresh_when_polled_equity_changed()` --calls--> `_model_dashboard_response()`  [EXTRACTED]
  tests/test_sizing_normalisation.py → wallet_proof_engine_8014.py

## Import Cycles
- None detected.

## Communities (65 total, 4 thin omitted)

### Community 0 - "EngineSSOT"
Cohesion: 0.06
Nodes (38): datetime, acquire_lock(), atomic_write_json(), bval(), chunked(), CsvLedger, EngineSSOT, ensure_dirs() (+30 more)

### Community 1 - "Any"
Cohesion: 0.09
Nodes (32): Any, _account_orphan_signed_size(), active_wallet(), active_wallet_row(), age_ms_label(), apply_wallet_filters_to_state(), _build_account_orphan_registry(), contract_money_equal() (+24 more)

### Community 2 - "IpWeightBudget"
Cohesion: 0.19
Nodes (9): IpWeightBudget, A rolling weight window in a file, shared by every participant. ``reserve`` is…, Hold the machine-wide lock, or raise inside the caller's bound. The lock lives…, Read the committed window and say whether it can be believed. Returns…, Publish the window atomically. Write-then-replace, never truncate-then-write. A…, Weight reserved by every participant in the current window., Atomically reserve ``weight`` if the window can carry it. ``floor`` is weight…, Readable state for the HEALTH display and for tests. (+1 more)

### Community 3 - "get_portfolio_data"
Cohesion: 0.24
Nodes (19): _apply_date_range_to_series(), _curve_metrics(), _display_ssot_dd(), _ensure_trade_replay_series(), get_portfolio_data(), _get_wallet_pnl_series(), _load_curve(), _load_trade_replay_pnl_series() (+11 more)

### Community 4 - "hl_rate_guard.py"
Cohesion: 0.14
Nodes (15): guard(), RateGuard, Per-process Hyperliquid request budget for the WALLET FINDER products. Three…, Try to claim budget for one request. False means skip this one. ``timeout_s``…, A rolling-window ceiling for one process, plus the shared IP budget., reset_for_tests(), shared_budget(), weight_for() (+7 more)

### Community 5 - "3_mtm_filter.py"
Cohesion: 0.07
Nodes (51): ClientSession, Path, Path, Semaphore, fetch_mtm(), gate_mtm(), load_progress(), load_wallets() (+43 more)

### Community 6 - "get_table_rows"
Cohesion: 0.15
Nodes (17): _activity_bucket(), _build_sparkline(), _fmt(), get_table_rows(), _is_dormant(), _is_slow(), _is_stale(), _last_trade_age_fmt() (+9 more)

### Community 7 - "universe_builder.py"
Cohesion: 0.13
Nodes (29): build_universe_rows(), compute_wallet_metrics(), dedup_trades_file(), flush_universe(), is_garbage(), load_purged_wallets(), load_trades_index(), load_valid_wallets() (+21 more)

### Community 8 - "get"
Cohesion: 0.09
Nodes (45): _account_value_curve(), api_wallet_equity_curve(), _build_account_reconciliation(), _build_live_leader_performance(), _build_live_wallet_derived(), _build_live_wallet_rows(), _build_manual_reconciliation_rows(), _build_real_copy_positions() (+37 more)

### Community 9 - "block_num"
Cohesion: 0.43
Nodes (7): block_num(), dd_current(), dd_max(), latest_history_block_dd(), max_history_block_dd(), Read numeric metric fields with legacy alias fallback., Return latest combined curve DD when the rendered block lacks it. Used only as…

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

### Community 14 - "load_ui_state"
Cohesion: 0.14
Nodes (37): admin_purge_wallet(), api_import_copy_candidates(), api_norm(), api_set_ui_state(), api_wallet_equity_refresh(), api_wallet_meta(), _archive_manual_reconciliation_ledger_row(), _clean_wallet_filters() (+29 more)

### Community 15 - "get"
Cohesion: 0.11
Nodes (28): _compute_compound_score(), data_status(), export_selected(), healthz(), last_trade_events(), _martingale_flag_reliable(), _penalty_flags(), _penalty_flags_title() (+20 more)

### Community 16 - "load_universe"
Cohesion: 0.10
Nodes (28): _apply_filters(), _current_table_days(), data_status_card(), _find_all_trades_path(), _get_visible_universe(), _load_last_trade_times(), _load_purged_wallets(), _load_summary_kpis() (+20 more)

### Community 17 - "get_live_audit_summary"
Cohesion: 0.40
Nodes (6): _audit_summary_background_rebuild(), get_live_audit_summary(), Persist the last enriched live summary so App restarts do not blind /live-copy., Background rebuild of live audit summary cache — never blocks the request…, Return live audit summary with non-blocking stale-serve + background rebuild. -…, _save_live_audit_summary_last_good()

### Community 18 - "_run_harness.py"
Cohesion: 0.19
Nodes (19): audit_for(), dd_from_pts(), fetch_ch(), fetch_funding_72h(), fetch_ledger_72h(), fetch_portfolio(), fetch_user_fills_72h(), http_post() (+11 more)

### Community 19 - "update_last_trade_times"
Cohesion: 0.25
Nodes (15): _file_identity(), _normalise_baseline(), Path, Incremental last-trade timestamp indexing for the 8012 dashboard.  The trade led, Return latest wallet timestamps plus a resumable ledger cursor.      A previous, update_last_trade_times(), Path, _row() (+7 more)

### Community 20 - "atomic_write_json"
Cohesion: 0.19
Nodes (23): atomic_write_csv(), atomic_write_json(), backup_purge_files(), _candidate_wallet_file(), import_copy_candidates_to_manual_wallets(), _last_trade_ms_from_engine_truth(), load_engine_truth(), load_local_env_file() (+15 more)

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
Cohesion: 0.21
Nodes (11): _point(), Regression coverage for 8014 proof-window TRUE DD and dashboard caching., test_curve_loader_keeps_only_window_seed_and_window_points(), test_dashboard_cold_start_serves_last_good_html_before_json_recovery(), test_dashboard_serves_fresh_cached_html_without_a_model_rebuild(), test_portfolio_true_dd_cannot_admit_a_zero_fill_wallets_old_history(), test_rendered_wallet_row_keeps_distinct_drawdowns_and_omits_exact_aliases(), test_true_dd_carries_a_quiet_baselined_wallet_forward_as_valid_zero() (+3 more)

### Community 34 - "fnum"
Cohesion: 0.10
Nodes (44): api_metrics(), _append_exchange_history(), avg(), css_class(), dash_td(), _dd_abs(), dual(), dual_or_dash() (+36 more)

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

### Community 47 - "_load_live_copy_config"
Cohesion: 0.22
Nodes (24): _active_live_copy_wallet_count(), add_live_config_wallet(), api_state(), _enforce_live_copy_cap(), get_live_config(), _global_controls_for_ui(), invalidate_live_audit_summary_cache(), invalidate_model_cache() (+16 more)

### Community 48 - "test_sizing_normalisation.py"
Cohesion: 0.12
Nodes (20): RawFill, _fill(), Regression coverage for fill-time proportional sizing and USER refresh., test_dashboard_request_queues_refresh_when_polled_equity_changed(), test_explicit_dashboard_refresh_rebuilds_from_latest_poll(), test_explicit_wallet_equity_override_remains_fixed(), test_one_nanosecond_source_change_is_detected(), test_polled_leader_equity_source_change_is_detected() (+12 more)

### Community 49 - "wallet_proof_engine_8014.py"
Cohesion: 0.08
Nodes (46): Compatibility wrapper for the renamed Wallet Proof Engine 8014 module., _age_label_from_ms(), _apply_ownership_truth_to_integrity(), _build_execution_quality_freshness(), _build_execution_quality_rows(), _build_execution_quality_summary(), _build_live_top_status(), _build_ownership_truth_execution_quality_rows() (+38 more)

### Community 51 - "build_model_state"
Cohesion: 0.07
Nodes (39): alignment_status_for_key(), apply_live_config_wallet_model(), _apply_price_model(), bps_fee(), build_model_state(), calc_unrealized(), close_positions_fifo(), _copy_cost_bps() (+31 more)

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

### Community 58 - "_fetch_exchange_account_snapshot"
Cohesion: 0.24
Nodes (10): _account_reconciliation_baseline_timestamp(), _fetch_exchange_account_snapshot(), _fetch_user_fills_by_time(), _fetch_user_realized_pnl_snapshot(), _fetch_user_realized_pnl_snapshot_uncached(), _local_env_value(), _public_account_address(), Realized PnL over days, cached for the operator refresh interval. The uncached… (+2 more)

### Community 59 - "_load_state"
Cohesion: 0.29
Nodes (7): _latest_state_with_selection(), _load_state(), on_event, Recover Talent Scout-owned selection keys from the latest rotating backup., Load state from disk. Falls back to .bak if main file is corrupt. Migration: if…, _selected_true_count(), startup()

### Community 60 - "test_talent_scout_selection_responsiveness.py"
Cohesion: 0.20
Nodes (7): _FakeKernel32, _FakeWinFunction, isolated_selection_state(), fixture, parametrize, test_checkbox_selection_returns_without_inline_portfolio_compute(), test_portfolio_remove_queues_refresh_instead_of_computing_inline()

### Community 61 - "JSONResponse"
Cohesion: 0.16
Nodes (21): Response, api_cache_health(), api_engine_health(), api_equity(), api_get_ui_state(), api_metrics_csv(), api_model_cache_status(), api_trades() (+13 more)

### Community 62 - "_model_dashboard_response"
Cohesion: 0.17
Nodes (12): home(), load_manual_wallets(), model_dashboard(), _model_dashboard_html_cache_store(), _model_dashboard_response(), Dashboard home route with non-blocking fallback., Explicit refresh: rebuild from the latest polled sizing equity., Legacy SSOT model dashboard, always accessible at /model or /legacy-model. (+4 more)

### Community 63 - "SharedBudgetUnavailable"
Cohesion: 0.40
Nodes (5): The shared budget could not be consulted within its bound. Deliberately…, SharedBudgetUnavailable, RuntimeError, legacy_main_pre_intake_repair(), Disabled legacy entrypoint; retained only for import compatibility.

### Community 64 - "inum"
Cohesion: 0.09
Nodes (30): test_render_refresh_rebuilds_user_row_from_current_selection(), apply_user_true_drawdown_rollup(), block(), build_computed_true_drawdown_history(), build_portfolio_history(), _build_recent_send_warning_groups(), _clean_wallet_filter_last_result(), compact_history() (+22 more)

### Community 66 - "last_trade_events"
Cohesion: 0.67
Nodes (3): last_trade_events(), StreamingResponse, Push LAST TRADE updates to the browser as engine truth advances. Server-sent…

## Knowledge Gaps
- **5 isolated node(s):** `Path`, `start_all.sh script`, `_FakeKernel32`, `_FakeKernel32`, `Path`
  These have ≤1 connection - possible missing edges or undocumented components.
- **4 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Any` connect `Any` to `EngineSSOT`, `inum`, `fnum`, `hl_rate_guard.py`, `get`, `block_num`, `1WalletFinder.py`, `load_ui_state`, `_load_live_copy_config`, `test_sizing_normalisation.py`, `wallet_proof_engine_8014.py`, `get_live_audit_summary`, `update_last_trade_times`, `build_model_state`, `atomic_write_json`, `_fetch_exchange_account_snapshot`, `JSONResponse`, `_model_dashboard_response`?**
  _High betweenness centrality (0.134) - this node is a cross-community bridge._
- **Why does `_summarise()` connect `3_mtm_filter.py` to `app.py`, `4_useful_wallet_scanner.py`, `load_universe`, `load_universe`, `wallet_talent_scout_8012.py`?**
  _High betweenness centrality (0.072) - this node is a cross-community bridge._
- **Why does `update_last_trade_times()` connect `update_last_trade_times` to `load_universe`, `Any`, `wallet_talent_scout_8012.py`?**
  _High betweenness centrality (0.056) - this node is a cross-community bridge._
- **What connects `Path`, `start_all.sh script`, `_FakeKernel32` to the rest of the system?**
  _5 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `EngineSSOT` be split into smaller, more focused modules?**
  _Cohesion score 0.06422466422466422 - nodes in this community are weakly interconnected._
- **Should `Any` be split into smaller, more focused modules?**
  _Cohesion score 0.09274193548387097 - nodes in this community are weakly interconnected._
- **Should `hl_rate_guard.py` be split into smaller, more focused modules?**
  _Cohesion score 0.14210526315789473 - nodes in this community are weakly interconnected._