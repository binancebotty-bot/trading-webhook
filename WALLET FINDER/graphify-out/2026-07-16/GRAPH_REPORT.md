# Graph Report - WALLET FINDER  (2026-07-16)

## Corpus Check
- 38 files · ~174,922 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 984 nodes · 2882 edges · 50 communities (47 shown, 3 thin omitted)
- Extraction: 100% EXTRACTED · 0% INFERRED · 0% AMBIGUOUS · INFERRED: 11 edges (avg confidence: 0.58)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `790a5f37`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- Community 0
- Community 1
- Community 2
- load_universe
- Community 4
- Community 5
- Community 6
- Community 7
- Community 8
- Community 9
- Community 10
- Community 11
- Community 12
- Community 13
- Community 14
- Community 15
- Community 16
- _account_value_curve
- Community 18
- _model_dashboard_response
- Community 20
- Community 21
- Community 22
- Community 23
- Community 24
- Community 25
- Community 26
- Community 27
- Community 28
- Community 29
- Community 30
- Community 31
- Community 32
- test_true_dd_windowing.py
- Community 34
- Community 35
- Community 36
- Community 37
- Community 38
- _load_state
- 2simplefilter.py
- hl_mtm_lookup.py
- _account_value_curve
- test_sizing_normalisation.py
- run_context.py

## God Nodes (most connected - your core abstractions)
1. `fnum()` - 83 edges
2. `Request` - 54 edges
3. `JSONResponse` - 48 edges
4. `HTMLResponse` - 47 edges
5. `EngineSSOT` - 44 edges
6. `build_model_state()` - 44 edges
7. `_live_audit_summary()` - 40 edges
8. `inum()` - 33 edges
9. `load_json()` - 30 edges
10. `Series` - 29 edges

## Surprising Connections (you probably didn't know these)
- `get_live_copy_summary()` --calls--> `JSONResponse`  [EXTRACTED]
  wallet_proof_engine_8014.py → app.py
- `live_copy_dashboard_json()` --calls--> `JSONResponse`  [EXTRACTED]
  wallet_proof_engine_8014.py → app.py
- `reset_app_history()` --references--> `JSONResponse`  [EXTRACTED]
  wallet_proof_engine_8014.py → app.py
- `test_table_context_returns_total_rows()` --calls--> `_table_context()`  [INFERRED]
  tests/test_table_count_fix.py → wallet_talent_scout_8012.py
- `test_header_pagination_agree()` --calls--> `_table_context()`  [INFERRED]
  tests/test_table_count_fix.py → wallet_talent_scout_8012.py

## Import Cycles
- 1-file cycle: `HL_Copy_Engine_SSOT.py -> HL_Copy_Engine_SSOT.py`

## Communities (50 total, 3 thin omitted)

### Community 0 - "Community 0"
Cohesion: 0.07
Nodes (38): datetime, Any, Path, acquire_lock(), atomic_write_json(), bval(), chunked(), CsvLedger (+30 more)

### Community 1 - "Community 1"
Cohesion: 0.08
Nodes (49): active_wallet(), active_wallet_row(), age_ms_label(), block_num(), core_missing(), core_td(), css_class(), dash_td() (+41 more)

### Community 2 - "Community 2"
Cohesion: 0.13
Nodes (24): apply_user_true_drawdown_rollup(), block(), build_computed_true_drawdown_history(), build_portfolio_history(), compact_history(), _computed_equity_curve(), _computed_true_drawdown_summary(), _drawdown_from_curve_points() (+16 more)

### Community 3 - "load_universe"
Cohesion: 0.15
Nodes (15): Path, _find_all_trades_path(), _latest_state_with_selection(), _load_last_trade_times(), _load_purged_wallets(), _load_state(), _load_summary_kpis(), _load_trade_stats() (+7 more)

### Community 4 - "Community 4"
Cohesion: 0.40
Nodes (5): _live_leader_performance_from_last_good(), _load_live_audit_summary_last_good(), _perf_has_alltime(), Pre-build live audit summary at startup so first page load hits cache., _startup_warm_audit_cache()

### Community 5 - "Community 5"
Cohesion: 0.09
Nodes (36): ClientSession, Path, Semaphore, fetch_mtm(), gate_mtm(), load_progress(), load_wallets(), log() (+28 more)

### Community 6 - "Community 6"
Cohesion: 0.14
Nodes (17): _activity_bucket(), _apply_filters(), _build_sparkline(), _fmt(), get_table_rows(), _get_visible_universe(), _is_dormant(), _is_slow() (+9 more)

### Community 7 - "Community 7"
Cohesion: 0.17
Nodes (25): append_all_trades(), compute_wallet_metrics(), dedup_trades_file(), is_garbage(), load_purged_wallets(), load_trades_index(), load_valid_wallets(), log() (+17 more)

### Community 8 - "Community 8"
Cohesion: 0.08
Nodes (46): _build_execution_quality_summary(), _build_live_leader_performance(), _build_manual_reconciliation_rows(), _build_real_copy_positions(), _classify_owned_exchange_net(), _classify_recon_action_type(), contract_money_equal(), _core_process_rows() (+38 more)

### Community 9 - "Community 9"
Cohesion: 0.06
Nodes (56): Response, alignment_status_for_key(), api_metrics_csv(), _apply_price_model(), avg(), bps_fee(), build_model_state(), calc_unrealized() (+48 more)

### Community 10 - "Community 10"
Cohesion: 0.15
Nodes (20): PlainTextResponse, _build_sparkline(), data_status_card(), export_selected(), _load_curve(), _load_mtm_account_value_series(), _mtm_period_candidates(), _mtm_period_for_days() (+12 more)

### Community 11 - "Community 11"
Cohesion: 0.11
Nodes (24): _activity_bucket(), _compute_compound_score(), _display_ssot_dd(), _fmt(), get_table_rows(), _is_dormant(), _is_slow(), _is_stale() (+16 more)

### Community 12 - "Community 12"
Cohesion: 0.12
Nodes (15): append_summary(), compute_kpis(), compute_trade_kpis(), _ensure_summary_header(), fetch_all_fills(), fetch_recent_fills_probe(), load_progress(), log() (+7 more)

### Community 13 - "Community 13"
Cohesion: 0.21
Nodes (22): append_csv(), get_coins(), init_csv(), inject_leaderboard_wallets(), load_existing_wallets(), main(), poll_worker(), post() (+14 more)

### Community 14 - "Community 14"
Cohesion: 0.07
Nodes (76): JSONResponse, Request, reorder_portfolio(), add_live_config_wallet(), admin_purge_wallet(), api_cache_health(), api_equity(), api_get_ui_state() (+68 more)

### Community 15 - "Community 15"
Cohesion: 0.16
Nodes (17): _current_table_days(), export_selected(), _load_mtm_account_value_series(), _mode_slip_pct(), _mtm_period_candidates(), _mtm_period_for_days(), proof_wallets_txt(), _prune_selection_to_universe() (+9 more)

### Community 16 - "Community 16"
Cohesion: 0.15
Nodes (15): _find_all_trades_path(), _load_last_trade_times(), _load_purged_wallets(), _load_summary_kpis(), _load_trade_stats(), _load_trade_stats_for_wallets(), load_universe(), _overlay_cached_mtm() (+7 more)

### Community 17 - "_account_value_curve"
Cohesion: 0.15
Nodes (14): _account_value_curve(), _active_live_copy_wallet_count(), api_wallet_equity_curve(), _build_live_wallet_rows(), _current_leader_equity_from_cache(), get_effective_max_dd(), _leader_equity_at_or_before(), _mtm_stale_flag() (+6 more)

### Community 18 - "Community 18"
Cohesion: 0.19
Nodes (19): audit_for(), dd_from_pts(), fetch_ch(), fetch_funding_72h(), fetch_ledger_72h(), fetch_portfolio(), fetch_user_fills_72h(), http_post() (+11 more)

### Community 19 - "_model_dashboard_response"
Cohesion: 0.20
Nodes (10): home(), _kick_model_cache_refresh_background(), _model_cache_snapshot_nonblocking(), model_dashboard(), _model_dashboard_html_cache_latest_get(), _model_dashboard_last_good_html_disk_get(), _model_dashboard_response(), Dashboard home route with non-blocking fallback. (+2 more)

### Community 20 - "Community 20"
Cohesion: 0.14
Nodes (28): atomic_write_csv(), atomic_write_json(), _audit_summary_background_rebuild(), backup_purge_files(), _candidate_wallet_file(), get_live_audit_summary(), import_copy_candidates_to_manual_wallets(), _live_order_intents_path() (+20 more)

### Community 21 - "Community 21"
Cohesion: 0.50
Nodes (3): Regression coverage for the 8012 -> 8014 candidate import boundary., test_import_does_not_consult_true_dd_or_model_state(), _wallet()

### Community 22 - "Community 22"
Cohesion: 0.21
Nodes (24): HTMLResponse, apply_filter(), clear_all(), clear_filter(), _get_sort(), portfolio_panel(), Atomic save: write to temp file, then os.replace() — no partial writes possible., refresh() (+16 more)

### Community 23 - "Community 23"
Cohesion: 0.25
Nodes (20): Series, _apply_date_range_to_series(), _curve_metrics(), _display_ssot_dd(), _ensure_trade_replay_series(), get_portfolio_data(), _get_wallet_pnl_series(), _load_curve() (+12 more)

### Community 24 - "Community 24"
Cohesion: 0.14
Nodes (17): Regression test: Table count fix for WALLET FINDER.  Verifies: 1. per_page in st, per_page in wallet_finder_state.json must not be 3., Header must use total_rows, not rows|length., _table_context must include total_rows in its return dict., Header count and pagination count must agree., test_header_pagination_agree(), test_header_uses_total_rows(), test_per_page_not_three() (+9 more)

### Community 25 - "Community 25"
Cohesion: 0.20
Nodes (17): _apply_date_range_to_series(), _current_table_days(), _curve_metrics(), _ensure_trade_replay_series(), get_portfolio_data(), _get_wallet_pnl_series(), _load_trade_replay_pnl_series(), _load_trade_replay_raw_series() (+9 more)

### Community 26 - "Community 26"
Cohesion: 0.22
Nodes (19): apply_filter(), _async_table_plus_filter_oob(), _async_table_plus_portfolio_oob(), clear_all(), clear_filter(), _get_sort(), _parse_filter(), portfolio_panel() (+11 more)

### Community 27 - "Community 27"
Cohesion: 0.12
Nodes (13): load_script(), 8012 candidates must enter 8014 before TRUE DD/readiness evaluation., 8012 keeps its original scout filters separate from 8014 promotion gates., Restored 8012 displays the original Real-DD scout column, not TRUE/SCOUT DD., 8012 has no 8014 TRUE DD promotion gate or reconstructed TRUE column pollution., test_8012_filters_are_invariant_to_normalization(), test_8012_realised_dd_display_is_not_mtm_capped(), test_8012_sparse_mtm_is_floored_by_realised_dd_for_risk() (+5 more)

### Community 28 - "Community 28"
Cohesion: 0.19
Nodes (14): Path, assess_status(), compute_reconstructed_curve(), _empty_candidate(), load_copy_ready_wallets(), load_trades_for_wallets(), main(), 5_reconstructed_drawdown.py — WALLET FINDER EDITION Stage 5: Reconstructed Draw (+6 more)

### Community 29 - "Community 29"
Cohesion: 0.22
Nodes (11): DataFrame, _apply_filters(), data_status(), _get_visible_universe(), _purge_losing_wallets_from_df(), Count losing proof rows without mutating purge files or hiding wallets., Diagnostic endpoint showing data directory, file sizes, row counts, and MTM cove, _sort_universe() (+3 more)

### Community 30 - "Community 30"
Cohesion: 0.16
Nodes (19): _active_filter_count(), _activity_counts(), _async_get_portfolio_data(), _async_render_portfolio_html(), _async_render_table_html(), _async_table_context(), index(), Return active (<=3d), slow (3d-1w), stale (1w-15d), and dormant (>15d) counts. (+11 more)

### Community 31 - "Community 31"
Cohesion: 0.57
Nodes (7): dd_from_pts(), fetch_avh(), fetch_ledger(), local_equity_curve_full(), local_summary_for(), main(), post()

### Community 32 - "Community 32"
Cohesion: 0.25
Nodes (8): _compute_compound_score(), _martingale_flag_reliable(), _penalty_flags(), _penalty_flags_title(), The current scanner flag is unusable if it marks nearly the whole universe., Return a short string showing penalty flags (empty if none)., Return a readable explanation for compact penalty flags., Compute compound risk-adjusted copy score (0-100 scale).      Weights are empi

### Community 33 - "test_true_dd_windowing.py"
Cohesion: 0.23
Nodes (6): _point(), Regression coverage for 8014 proof-window TRUE DD and dashboard caching., test_portfolio_true_dd_cannot_admit_a_zero_fill_wallets_old_history(), test_true_dd_carries_a_quiet_baselined_wallet_forward_as_valid_zero(), test_true_dd_never_falls_back_to_full_history_without_a_proof_window(), test_true_dd_uses_only_the_supplied_proof_window()

### Community 34 - "Community 34"
Cohesion: 0.21
Nodes (13): _age_label_from_ms(), _build_execution_quality_freshness(), _build_execution_quality_rows(), _build_ownership_truth_execution_quality_rows(), _build_reconciliation_execution_quality_rows(), _derive_ownership_severity(), _is_resolved_raw_index_mapping_row(), _is_spot_index_coin() (+5 more)

### Community 35 - "Community 35"
Cohesion: 0.83
Nodes (3): stop_all.sh script, stop_by_pid_file(), stop_by_port()

### Community 44 - "_load_state"
Cohesion: 0.32
Nodes (8): _latest_state_with_selection(), _load_state(), Recover Talent Scout-owned selection keys from the latest rotating backup., Run heavy synchronous startup work in thread pool to avoid blocking event loop., Load state from disk. Falls back to .bak if main file is corrupt.      Migrati, _run_startup_work(), _selected_true_count(), startup()

### Community 45 - "2simplefilter.py"
Cohesion: 0.16
Nodes (18): Path, cache_path(), extract_wallet_stats(), fetch_portfolio(), legacy_main_pre_intake_repair(), load_source_wallets(), main(), stage1_simple.py â€” The ONLY filter stage before the deep-dive trade count chec (+10 more)

### Community 46 - "hl_mtm_lookup.py"
Cohesion: 0.22
Nodes (16): Compatibility wrapper for the renamed Wallet Proof Engine 8014 module., Path, Debug MTM DD values for problem wallets., _cache_path(), _empty_stats(), get_mtm_stats(), get_mtm_stats_async(), hl_mtm_lookup.py — Shared HL portfolio (MTM) lookup utility. WALLET FINDER editi (+8 more)

### Community 47 - "_account_value_curve"
Cohesion: 0.10
Nodes (42): _account_orphan_signed_size(), _account_reconciliation_baseline_timestamp(), api_model_cache_status(), _append_exchange_history(), _apply_ownership_truth_to_integrity(), _build_account_orphan_registry(), _build_account_reconciliation(), _build_live_top_status() (+34 more)

### Community 48 - "test_sizing_normalisation.py"
Cohesion: 0.36
Nodes (6): RawFill, _fill(), Regression coverage for fill-time proportional sizing and USER refresh., test_explicit_wallet_equity_override_remains_fixed(), test_proportional_sizing_uses_leader_equity_at_each_fill(), test_unlocked_saved_equity_value_does_not_freeze_sizing()

### Community 50 - "run_context.py"
Cohesion: 0.52
Nodes (6): get_current_run_id(), latest_run_filter(), _new_run_id(), Run identity helpers for Wallet Finder rebuilds., start_new_run(), write_current_run()

## Knowledge Gaps
- **4 isolated node(s):** `Path`, `Path`, `start_all.sh script`, `Path`
  These have ≤1 connection - possible missing edges or undocumented components.
- **3 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `datetime` connect `Community 0` to `Community 7`, `Community 9`, `Community 10`, `Community 12`, `Community 15`, `run_context.py`, `Community 28`?**
  _High betweenness centrality (0.380) - this node is a cross-community bridge._
- **Why does `_summarise()` connect `hl_mtm_lookup.py` to `Community 10`, `Community 11`, `2simplefilter.py`, `Community 15`, `Community 16`?**
  _High betweenness centrality (0.068) - this node is a cross-community bridge._
- **What connects `Path`, `Path`, `start_all.sh script` to the rest of the system?**
  _4 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Community 0` be split into smaller, more focused modules?**
  _Cohesion score 0.07041198501872659 - nodes in this community are weakly interconnected._
- **Should `Community 1` be split into smaller, more focused modules?**
  _Cohesion score 0.0782312925170068 - nodes in this community are weakly interconnected._
- **Should `Community 2` be split into smaller, more focused modules?**
  _Cohesion score 0.12681159420289856 - nodes in this community are weakly interconnected._
- **Should `load_universe` be split into smaller, more focused modules?**
  _Cohesion score 0.14705882352941177 - nodes in this community are weakly interconnected._