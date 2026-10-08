# Graph Report - WALLET FINDER  (2026-07-27)

## Corpus Check
- 41 files · ~183,828 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1040 nodes · 3106 edges · 51 communities (48 shown, 3 thin omitted)
- Extraction: 99% EXTRACTED · 1% INFERRED · 0% AMBIGUOUS · INFERRED: 20 edges (avg confidence: 0.54)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `790a5f37`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- EngineSSOT
- render_home
- _fetch_user_realized_pnl_snapshot
- load_universe
- Any
- 3_mtm_filter.py
- wallet_talent_scout_8012.py
- universe_builder.py
- fnum
- wallet_proof_engine_8014.py
- app.py
- get
- 4_useful_wallet_scanner.py
- 1WalletFinder.py
- JSONResponse
- get
- load_universe
- hl_mtm_lookup.py
- _run_harness.py
- _model_dashboard_response
- Path
- test_import_copy_candidates.py
- _save_state
- get_portfolio_data
- test_table_count_fix.py
- _num
- _save_state
- test_pipeline_policy.py
- 5_reconstructed_drawdown.py
- load_universe
- _table_context
- _w2_w3_avh_probe.py
- _load_state
- test_true_dd_windowing.py
- build_computed_true_drawdown_history
- stop_all.sh
- verify_active_wallet_proof_ui.py
- __init__.py
- start_all.sh
- _load_state
- run_context.py
- block_num
- _account_value_curve
- test_sizing_normalisation.py
- _save_live_audit_summary_last_good

## God Nodes (most connected - your core abstractions)
1. `fnum()` - 84 edges
2. `build_model_state()` - 45 edges
3. `EngineSSOT` - 44 edges
4. `_live_audit_summary()` - 40 edges
5. `inum()` - 34 edges
6. `load_json()` - 30 edges
7. `load_ui_state()` - 27 edges
8. `render_home()` - 24 edges
9. `save_ui_state()` - 23 edges
10. `parse_bool()` - 23 edges

## Surprising Connections (you probably didn't know these)
- `monitor_start_ms()` --calls--> `iso_to_ms()`  [INFERRED]
  wallet_proof_engine_8014.py → HL_Copy_Engine_SSOT.py
- `test_table_context_returns_total_rows()` --calls--> `_table_context()`  [INFERRED]
  tests/test_table_count_fix.py → wallet_talent_scout_8012.py
- `test_header_pagination_agree()` --calls--> `_table_context()`  [INFERRED]
  tests/test_table_count_fix.py → wallet_talent_scout_8012.py
- `load_source_wallets()` --calls--> `load_intake_rows()`  [EXTRACTED]
  2simplefilter.py → wallet_intake.py
- `extract_wallet_stats()` --calls--> `_summarise()`  [EXTRACTED]
  2simplefilter.py → hl_mtm_lookup.py

## Import Cycles
- 1-file cycle: `HL_Copy_Engine_SSOT.py -> HL_Copy_Engine_SSOT.py`

## Communities (51 total, 3 thin omitted)

### Community 0 - "EngineSSOT"
Cohesion: 0.07
Nodes (37): datetime, Any, Path, acquire_lock(), atomic_write_json(), bval(), chunked(), CsvLedger (+29 more)

### Community 1 - "render_home"
Cohesion: 0.08
Nodes (58): Response, active_wallet(), active_wallet_row(), api_equity(), api_metrics(), api_metrics_csv(), api_trades(), apply_user_true_drawdown_rollup() (+50 more)

### Community 2 - "_fetch_user_realized_pnl_snapshot"
Cohesion: 0.40
Nodes (5): _live_leader_performance_from_last_good(), _load_live_audit_summary_last_good(), _perf_has_alltime(), Pre-build live audit summary at startup so first page load hits cache., _startup_warm_audit_cache()

### Community 3 - "load_universe"
Cohesion: 0.24
Nodes (18): _apply_date_range_to_series(), _current_table_days(), _curve_metrics(), _ensure_trade_replay_series(), get_portfolio_data(), _get_wallet_pnl_series(), _load_curve(), _load_trade_replay_pnl_series() (+10 more)

### Community 4 - "Any"
Cohesion: 0.16
Nodes (18): Path, cache_path(), extract_wallet_stats(), fetch_portfolio(), legacy_main_pre_intake_repair(), load_source_wallets(), main(), stage1_simple.py â€” The ONLY filter stage before the deep-dive trade count chec (+10 more)

### Community 5 - "3_mtm_filter.py"
Cohesion: 0.09
Nodes (36): ClientSession, Path, Semaphore, fetch_mtm(), gate_mtm(), load_progress(), load_wallets(), log() (+28 more)

### Community 6 - "wallet_talent_scout_8012.py"
Cohesion: 0.12
Nodes (25): _activity_bucket(), _build_sparkline(), _fmt(), get_table_rows(), _install_replay_payload(), _is_dormant(), _is_slow(), _is_stale() (+17 more)

### Community 7 - "universe_builder.py"
Cohesion: 0.17
Nodes (25): append_all_trades(), compute_wallet_metrics(), dedup_trades_file(), is_garbage(), load_purged_wallets(), load_trades_index(), load_valid_wallets(), log() (+17 more)

### Community 8 - "fnum"
Cohesion: 0.06
Nodes (68): _account_value_curve(), _active_live_copy_wallet_count(), _age_label_from_ms(), age_ms_label(), api_wallet_equity_curve(), _build_execution_quality_freshness(), _build_execution_quality_rows(), _build_execution_quality_summary() (+60 more)

### Community 9 - "wallet_proof_engine_8014.py"
Cohesion: 0.07
Nodes (55): alignment_status_for_key(), apply_live_config_wallet_model(), _apply_price_model(), avg(), block(), bps_fee(), build_model_state(), calc_unrealized() (+47 more)

### Community 10 - "app.py"
Cohesion: 0.10
Nodes (25): _activity_bucket(), _build_sparkline(), _compute_compound_score(), _display_ssot_dd(), _fmt(), get_table_rows(), _is_dormant(), _is_slow() (+17 more)

### Community 11 - "get"
Cohesion: 0.21
Nodes (17): data_status(), export_selected(), healthz(), proof_wallets_txt(), _prune_selection_to_universe(), get, JSONResponse, PlainTextResponse (+9 more)

### Community 12 - "4_useful_wallet_scanner.py"
Cohesion: 0.12
Nodes (15): append_summary(), compute_kpis(), compute_trade_kpis(), _ensure_summary_header(), fetch_all_fills(), fetch_recent_fills_probe(), load_progress(), log() (+7 more)

### Community 13 - "1WalletFinder.py"
Cohesion: 0.21
Nodes (22): append_csv(), get_coins(), init_csv(), inject_leaderboard_wallets(), load_existing_wallets(), main(), poll_worker(), post() (+14 more)

### Community 14 - "JSONResponse"
Cohesion: 0.07
Nodes (73): add_live_config_wallet(), admin_purge_wallet(), api_cache_health(), api_get_ui_state(), api_import_copy_candidates(), api_norm(), api_set_ui_state(), api_wallet_meta() (+65 more)

### Community 15 - "get"
Cohesion: 0.13
Nodes (26): _compute_compound_score(), _current_table_days(), data_status(), export_selected(), healthz(), _martingale_flag_reliable(), _penalty_flags(), _penalty_flags_title() (+18 more)

### Community 16 - "load_universe"
Cohesion: 0.11
Nodes (24): _apply_filters(), data_status_card(), _find_all_trades_path(), _get_visible_universe(), _load_last_trade_times(), _load_purged_wallets(), _load_summary_kpis(), _load_trade_stats() (+16 more)

### Community 17 - "hl_mtm_lookup.py"
Cohesion: 0.22
Nodes (16): Compatibility wrapper for the renamed Wallet Proof Engine 8014 module., Path, Debug MTM DD values for problem wallets., _cache_path(), _empty_stats(), get_mtm_stats(), get_mtm_stats_async(), hl_mtm_lookup.py — Shared HL portfolio (MTM) lookup utility. WALLET FINDER editi (+8 more)

### Community 18 - "_run_harness.py"
Cohesion: 0.19
Nodes (19): audit_for(), dd_from_pts(), fetch_ch(), fetch_funding_72h(), fetch_ledger_72h(), fetch_portfolio(), fetch_user_fills_72h(), http_post() (+11 more)

### Community 19 - "_model_dashboard_response"
Cohesion: 0.24
Nodes (16): _file_identity(), _normalise_baseline(), Any, Path, Incremental last-trade timestamp indexing for the 8012 dashboard.  The trade led, Return latest wallet timestamps plus a resumable ledger cursor.      A previous, update_last_trade_times(), Path (+8 more)

### Community 20 - "Path"
Cohesion: 0.13
Nodes (30): _account_orphan_signed_size(), atomic_write_csv(), atomic_write_json(), backup_purge_files(), _build_account_orphan_registry(), _candidate_wallet_file(), _file_age_payload(), import_copy_candidates_to_manual_wallets() (+22 more)

### Community 21 - "test_import_copy_candidates.py"
Cohesion: 0.50
Nodes (3): Regression coverage for the 8012 -> 8014 candidate import boundary., test_import_does_not_consult_true_dd_or_model_state(), _wallet()

### Community 22 - "_save_state"
Cohesion: 0.26
Nodes (25): apply_filter(), clear_all(), clear_filter(), _get_sort(), portfolio_panel(), HTMLResponse, post, Request (+17 more)

### Community 23 - "get_portfolio_data"
Cohesion: 0.27
Nodes (18): _apply_date_range_to_series(), _curve_metrics(), _display_ssot_dd(), _ensure_trade_replay_series(), get_portfolio_data(), _get_wallet_pnl_series(), _load_curve(), _load_trade_replay_pnl_series() (+10 more)

### Community 24 - "test_table_count_fix.py"
Cohesion: 0.20
Nodes (9): Regression test: Table count fix for WALLET FINDER.  Verifies: 1. per_page in st, per_page in wallet_finder_state.json must not be 3., Header must use total_rows, not rows|length., _table_context must include total_rows in its return dict., Header count and pagination count must agree., test_header_pagination_agree(), test_header_uses_total_rows(), test_per_page_not_three() (+1 more)

### Community 25 - "_num"
Cohesion: 0.21
Nodes (12): _active_filter_count(), _activity_counts(), _get_portfolio_data_async(), index(), portfolio_panel_initial(), Return active (<=3d), slow (3d-1w), stale (1w-15d), and dormant (>15d) counts., Keep the multi-gigabyte period scan off the web server's event loop., render() (+4 more)

### Community 26 - "_save_state"
Cohesion: 0.26
Nodes (25): apply_filter(), clear_all(), clear_filter(), _get_sort(), portfolio_panel(), HTMLResponse, post, Request (+17 more)

### Community 27 - "test_pipeline_policy.py"
Cohesion: 0.12
Nodes (13): load_script(), 8012 candidates must enter 8014 before TRUE DD/readiness evaluation., 8012 keeps its original scout filters separate from 8014 promotion gates., Restored 8012 displays the original Real-DD scout column, not TRUE/SCOUT DD., 8012 has no 8014 TRUE DD promotion gate or reconstructed TRUE column pollution., test_8012_filters_are_invariant_to_normalization(), test_8012_realised_dd_display_is_not_mtm_capped(), test_8012_sparse_mtm_is_floored_by_realised_dd_for_risk() (+5 more)

### Community 28 - "5_reconstructed_drawdown.py"
Cohesion: 0.19
Nodes (14): Path, assess_status(), compute_reconstructed_curve(), _empty_candidate(), load_copy_ready_wallets(), load_trades_for_wallets(), main(), 5_reconstructed_drawdown.py — WALLET FINDER EDITION Stage 5: Reconstructed Draw (+6 more)

### Community 29 - "load_universe"
Cohesion: 0.09
Nodes (32): data_status_card(), _find_all_trades_path(), _install_replay_payload(), _latest_state_with_selection(), _load_best_replay_snapshot(), _load_last_trade_times(), _load_mtm_account_value_series(), _load_purged_wallets() (+24 more)

### Community 30 - "_table_context"
Cohesion: 0.21
Nodes (12): _active_filter_count(), _activity_counts(), _get_portfolio_data_async(), index(), portfolio_panel_initial(), Return active (<=3d), slow (3d-1w), stale (1w-15d), and dormant (>15d) counts., Keep the multi-gigabyte period scan off the web server's event loop., render() (+4 more)

### Community 31 - "_w2_w3_avh_probe.py"
Cohesion: 0.57
Nodes (7): dd_from_pts(), fetch_avh(), fetch_ledger(), local_equity_curve_full(), local_summary_for(), main(), post()

### Community 32 - "_load_state"
Cohesion: 0.32
Nodes (8): _apply_filters(), _get_visible_universe(), _purge_losing_wallets_from_df(), DataFrame, Count losing proof rows without mutating purge files or hiding wallets., _sort_universe(), _sort_universe_pinned(), _table_execution_required()

### Community 33 - "test_true_dd_windowing.py"
Cohesion: 0.23
Nodes (6): _point(), Regression coverage for 8014 proof-window TRUE DD and dashboard caching., test_portfolio_true_dd_cannot_admit_a_zero_fill_wallets_old_history(), test_true_dd_carries_a_quiet_baselined_wallet_forward_as_valid_zero(), test_true_dd_never_falls_back_to_full_history_without_a_proof_window(), test_true_dd_uses_only_the_supplied_proof_window()

### Community 34 - "build_computed_true_drawdown_history"
Cohesion: 0.24
Nodes (10): build_computed_true_drawdown_history(), compact_history(), _computed_equity_curve(), _computed_true_drawdown_summary(), _drawdown_from_curve_points(), Load a wallet's proving curve, optionally limited to an 8014 proof window., Build portfolio TRUE DD from each wallet's own active 8014 proof window.      Th, Return proof-window true equity points, carrying sparse curves to the window end (+2 more)

### Community 35 - "stop_all.sh"
Cohesion: 0.83
Nodes (3): stop_all.sh script, stop_by_pid_file(), stop_by_port()

### Community 44 - "_load_state"
Cohesion: 0.33
Nodes (7): _latest_state_with_selection(), _load_state(), on_event, Recover Talent Scout-owned selection keys from the latest rotating backup., Load state from disk. Falls back to .bak if main file is corrupt.      Migrati, _selected_true_count(), startup()

### Community 45 - "run_context.py"
Cohesion: 0.52
Nodes (6): get_current_run_id(), latest_run_filter(), _new_run_id(), Run identity helpers for Wallet Finder rebuilds., start_new_run(), write_current_run()

### Community 46 - "block_num"
Cohesion: 0.43
Nodes (7): block_num(), dd_current(), dd_max(), latest_history_block_dd(), max_history_block_dd(), Read numeric metric fields with legacy alias fallback., Return latest combined curve DD when the rendered block lacks it.      Used on

### Community 47 - "_account_value_curve"
Cohesion: 0.08
Nodes (53): _account_reconciliation_baseline_timestamp(), api_model_cache_status(), api_state(), _append_exchange_history(), _apply_ownership_truth_to_integrity(), _build_account_reconciliation(), _build_live_top_status(), _build_live_wallet_derived() (+45 more)

### Community 48 - "test_sizing_normalisation.py"
Cohesion: 0.24
Nodes (6): RawFill, _fill(), Regression coverage for fill-time proportional sizing and USER refresh., test_explicit_wallet_equity_override_remains_fixed(), test_proportional_sizing_uses_leader_equity_at_each_fill(), test_unlocked_saved_equity_value_does_not_freeze_sizing()

### Community 49 - "_save_live_audit_summary_last_good"
Cohesion: 0.40
Nodes (6): _audit_summary_background_rebuild(), get_live_audit_summary(), Persist the last enriched live summary so App restarts do not blind /live-copy., Background rebuild of live audit summary cache — never blocks the request thread, Return live audit summary with non-blocking stale-serve + background rebuild., _save_live_audit_summary_last_good()

## Knowledge Gaps
- **3 isolated node(s):** `Path`, `start_all.sh script`, `Path`
  These have ≤1 connection - possible missing edges or undocumented components.
- **3 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `datetime` connect `EngineSSOT` to `wallet_talent_scout_8012.py`, `universe_builder.py`, `wallet_proof_engine_8014.py`, `4_useful_wallet_scanner.py`, `run_context.py`, `5_reconstructed_drawdown.py`, `load_universe`?**
  _High betweenness centrality (0.460) - this node is a cross-community bridge._
- **Why does `_summarise()` connect `hl_mtm_lookup.py` to `Any`, `wallet_talent_scout_8012.py`, `app.py`, `load_universe`, `load_universe`?**
  _High betweenness centrality (0.061) - this node is a cross-community bridge._
- **What connects `Path`, `start_all.sh script`, `Path` to the rest of the system?**
  _3 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `EngineSSOT` be split into smaller, more focused modules?**
  _Cohesion score 0.07175689479060265 - nodes in this community are weakly interconnected._
- **Should `render_home` be split into smaller, more focused modules?**
  _Cohesion score 0.07562008469449485 - nodes in this community are weakly interconnected._
- **Should `3_mtm_filter.py` be split into smaller, more focused modules?**
  _Cohesion score 0.08819345661450925 - nodes in this community are weakly interconnected._
- **Should `wallet_talent_scout_8012.py` be split into smaller, more focused modules?**
  _Cohesion score 0.1164021164021164 - nodes in this community are weakly interconnected._