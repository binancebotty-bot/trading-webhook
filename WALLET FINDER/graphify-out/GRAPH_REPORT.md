# Graph Report - WALLET FINDER  (2026-10-08)

## Corpus Check
- 58 files · ~240,846 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1528 nodes · 4384 edges · 86 communities (82 shown, 4 thin omitted)
- Extraction: 99% EXTRACTED · 1% INFERRED · 0% AMBIGUOUS · INFERRED: 30 edges (avg confidence: 0.54)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `e6e048c5`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- .append
- refresh_wallet_leader_equity_source
- IpWeightBudget
- get_portfolio_data
- hl_rate_guard.py
- 3_mtm_filter.py
- get_table_rows
- run_cycle
- _build_live_leader_performance
- EngineSSOT
- get_table_rows
- app.py
- 2simplefilter.py
- 1WalletFinder.py
- load_ui_state
- get
- load_universe
- inum
- _run_harness.py
- update_last_trade_times
- wallet_proof_engine_8014.py
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
- test_hip3_baseline_roll.py
- fnum
- stop_all.sh
- verify_active_wallet_proof_ui.py
- __init__.py
- start_all.sh
- _load_state
- post_with_weight_budget_preacquired
- test_talent_scout_global_sort.py
- 4_useful_wallet_scanner.py
- test_sizing_normalisation.py
- get
- build_model_state
- universe_builder.py
- start_wallet_finder.ps1
- get_adaptive_window_ms
- hl_ip_budget.py
- inum
- _Recorder
- Any
- _load_state
- test_talent_scout_selection_responsiveness.py
- _fetch_exchange_account_snapshot
- test_hip3_builder_dex.py
- log
- JSONResponse
- EngineSSOT
- wallet_proof_epoch_suite.py
- _load_live_copy_config
- hl_mtm_lookup.py
- HL_Copy_Engine_SSOT.py
- hip3_omission_derivation.py
- refresh_selected_portfolio_view
- real_dd_filter.py
- .poll_once
- _model_dashboard_response
- test_true_dd_windowing.py
- atomic_write_json
- .audit_position_drift_only
- Wallet Proof Engine — Epoch Repair Report
- run_context.py
- guard
- load_index_state
- get_live_audit_summary
- last_trade_events

## God Nodes (most connected - your core abstractions)
1. `fnum()` - 88 edges
2. `EngineSSOT` - 77 edges
3. `build_model_state()` - 47 edges
4. `EngineSSOT` - 44 edges
5. `_live_audit_summary()` - 41 edges
6. `inum()` - 36 edges
7. `log()` - 35 edges
8. `load_json()` - 31 edges
9. `load_ui_state()` - 31 edges
10. `render_home()` - 27 edges

## Surprising Connections (you probably didn't know these)
- `monitor_start_ms()` --calls--> `iso_to_ms()`  [INFERRED]
  wallet_proof_engine_8014.py → HL_Copy_Engine_SSOT.py
- `test_background_build_waits_for_cohort_refresh_lock()` --calls--> `_kick_model_cache_refresh_background()`  [EXTRACTED]
  tests/test_cohort_equity_rebuild.py → wallet_proof_engine_8014.py
- `test_full_dashboard_renders_cohort_refresh_result_and_controls()` --calls--> `render_home()`  [EXTRACTED]
  tests/test_cohort_equity_rebuild.py → wallet_proof_engine_8014.py
- `test_wallet_config_endpoint_persists_lock_and_starts_rebuild()` --calls--> `set_wallet_config()`  [EXTRACTED]
  tests/test_sizing_normalisation.py → wallet_proof_engine_8014.py
- `test_dashboard_request_queues_refresh_when_polled_equity_changed()` --calls--> `_model_dashboard_response()`  [EXTRACTED]
  tests/test_sizing_normalisation.py → wallet_proof_engine_8014.py

## Import Cycles
- None detected.

## Communities (86 total, 4 thin omitted)

### Community 0 - ".append"
Cohesion: 0.13
Nodes (9): CsvLedger, load_manual_wallets(), Path, Rebuild internal positions from the append-only ledger, EPOCH-AWARE. A fill…, Append newly added manual wallets without dropping active runtime state., Load measured proof epochs. Fail closed: a corrupt/unreadable file is treated…, RawFill, RawPosition (+1 more)

### Community 1 - "refresh_wallet_leader_equity_source"
Cohesion: 0.22
Nodes (12): parametrize, Cohort rebuild must fetch selected equity and preserve frozen/non-cohort…, test_background_build_waits_for_cohort_refresh_lock(), test_cohort_fetch_only_selected_unlocked_model_rows(), test_failed_fetch_retains_previous_cache_and_does_not_invalidate_model(), test_freeze_or_deselection_during_rate_wait_prevents_fetch(), test_full_dashboard_renders_cohort_refresh_result_and_controls(), test_refresh_route_fetches_before_one_forced_build_and_publishes_summary() (+4 more)

### Community 2 - "IpWeightBudget"
Cohesion: 0.19
Nodes (9): IpWeightBudget, A rolling weight window in a file, shared by every participant. ``reserve`` is…, Hold the machine-wide lock, or raise inside the caller's bound. The lock lives…, Read the committed window and say whether it can be believed. Returns…, Publish the window atomically. Write-then-replace, never truncate-then-write. A…, Weight reserved by every participant in the current window., Atomically reserve ``weight`` if the window can carry it. ``floor`` is weight…, Readable state for the HEALTH display and for tests. (+1 more)

### Community 3 - "get_portfolio_data"
Cohesion: 0.24
Nodes (19): _apply_date_range_to_series(), _curve_metrics(), _display_ssot_dd(), _ensure_trade_replay_series(), get_portfolio_data(), _get_wallet_pnl_series(), _load_curve(), _load_trade_replay_pnl_series() (+11 more)

### Community 4 - "hl_rate_guard.py"
Cohesion: 0.23
Nodes (8): The shared budget could not be consulted within its bound. Deliberately…, SharedBudgetUnavailable, RateGuard, Per-process Hyperliquid request budget for the WALLET FINDER products. Three…, Try to claim budget for one request. False means skip this one. ``timeout_s``…, A rolling-window ceiling for one process, plus the shared IP budget., shared_budget(), weight_for()

### Community 5 - "3_mtm_filter.py"
Cohesion: 0.16
Nodes (22): ClientSession, Semaphore, fetch_mtm(), gate_mtm(), load_progress(), load_wallets(), log(), main() (+14 more)

### Community 6 - "get_table_rows"
Cohesion: 0.15
Nodes (17): _activity_bucket(), _build_sparkline(), _fmt(), get_table_rows(), _is_dormant(), _is_slow(), _is_stale(), _last_trade_age_fmt() (+9 more)

### Community 7 - "run_cycle"
Cohesion: 0.23
Nodes (17): build_universe_rows(), compute_wallet_metrics(), flush_universe(), load_trades_index(), load_valid_wallets(), Single full scan of all_trades.csv for the given wallets. Returns (max_ts,…, Write each wallet's curve, then drop it from the cached metrics. The curve…, Snapshot the currently-known metrics as publishable universe rows. Safe to call… (+9 more)

### Community 8 - "_build_live_leader_performance"
Cohesion: 0.12
Nodes (25): _build_live_leader_performance(), _build_live_wallet_derived(), _build_manual_reconciliation_rows(), _build_real_copy_positions(), _classify_owned_exchange_net(), _classify_recon_action_type(), _earliest_real_order_filled_ms(), _exchange_field() (+17 more)

### Community 9 - "EngineSSOT"
Cohesion: 0.07
Nodes (36): acquire_lock(), atomic_write_json(), bval(), chunked(), CsvLedger, EngineSSOT, ensure_dirs(), fnum() (+28 more)

### Community 10 - "get_table_rows"
Cohesion: 0.11
Nodes (21): _activity_bucket(), _build_sparkline(), _compute_compound_score(), _fmt(), get_table_rows(), _is_dormant(), _is_slow(), _is_stale() (+13 more)

### Community 11 - "app.py"
Cohesion: 0.16
Nodes (19): data_status(), export_selected(), _load_mtm_account_value_series(), _mtm_period_candidates(), _mtm_period_for_days(), _parse_filter(), proof_wallets_txt(), _prune_selection_to_universe() (+11 more)

### Community 12 - "2simplefilter.py"
Cohesion: 0.15
Nodes (19): Path, RuntimeError, cache_path(), extract_wallet_stats(), fetch_portfolio(), legacy_main_pre_intake_repair(), load_source_wallets(), main() (+11 more)

### Community 13 - "1WalletFinder.py"
Cohesion: 0.22
Nodes (21): append_csv(), get_coins(), init_csv(), inject_leaderboard_wallets(), load_existing_wallets(), main(), poll_worker(), post() (+13 more)

### Community 14 - "load_ui_state"
Cohesion: 0.16
Nodes (33): admin_purge_wallet(), api_import_copy_candidates(), api_norm(), api_set_ui_state(), api_wallet_equity_refresh(), api_wallet_meta(), _archive_manual_reconciliation_ledger_row(), _clean_wallet_filters() (+25 more)

### Community 15 - "get"
Cohesion: 0.11
Nodes (28): _compute_compound_score(), data_status(), export_selected(), healthz(), last_trade_events(), _martingale_flag_reliable(), _penalty_flags(), _penalty_flags_title() (+20 more)

### Community 16 - "load_universe"
Cohesion: 0.10
Nodes (28): _apply_filters(), _current_table_days(), data_status_card(), _find_all_trades_path(), _get_visible_universe(), _load_last_trade_times(), _load_purged_wallets(), _load_summary_kpis() (+20 more)

### Community 17 - "inum"
Cohesion: 0.10
Nodes (25): active_wallet_row(), build_computed_true_drawdown_history(), build_proof_status_block(), _build_recent_send_warning_groups(), _clean_wallet_filter_last_result(), _computed_equity_curve(), _computed_true_drawdown_summary(), _drawdown_from_curve_points() (+17 more)

### Community 18 - "_run_harness.py"
Cohesion: 0.19
Nodes (19): audit_for(), dd_from_pts(), fetch_ch(), fetch_funding_72h(), fetch_ledger_72h(), fetch_portfolio(), fetch_user_fills_72h(), http_post() (+11 more)

### Community 19 - "update_last_trade_times"
Cohesion: 0.25
Nodes (15): _file_identity(), _normalise_baseline(), Path, Incremental last-trade timestamp indexing for the 8012 dashboard.  The trade led, Return latest wallet timestamps plus a resumable ledger cursor.      A previous, update_last_trade_times(), Path, _row() (+7 more)

### Community 20 - "wallet_proof_engine_8014.py"
Cohesion: 0.09
Nodes (42): Compatibility wrapper for the renamed Wallet Proof Engine 8014 module., atomic_write_csv(), atomic_write_json(), backup_purge_files(), _candidate_wallet_file(), dash_td(), _file_age_payload(), import_copy_candidates_to_manual_wallets() (+34 more)

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

### Community 33 - "test_hip3_baseline_roll.py"
Cohesion: 0.09
Nodes (34): ch_state(), FakeRequests, new_engine(), pos(), Tests for the HIP-3 defective-baseline guard + targeted epoch roll (fix D).…, The defective prior epoch is preserved UNRESOLVED with the explicit reason., Enumeration/union failure => NO roll, epoch untouched, pair stays flagged., A second detection inside the retry window does NOT re-issue the union fetch. (+26 more)

### Community 34 - "fnum"
Cohesion: 0.11
Nodes (45): active_wallet(), block_num(), core_missing(), core_td(), css_class(), _dd_abs(), dd_current(), dd_max() (+37 more)

### Community 35 - "stop_all.sh"
Cohesion: 0.83
Nodes (3): stop_all.sh script, stop_by_pid_file(), stop_by_port()

### Community 44 - "_load_state"
Cohesion: 0.33
Nodes (7): _latest_state_with_selection(), _load_state(), on_event, Recover Talent Scout-owned selection keys from the latest rotating backup., Load state from disk. Falls back to .bak if main file is corrupt. Migration: if…, _selected_true_count(), startup()

### Community 45 - "post_with_weight_budget_preacquired"
Cohesion: 0.16
Nodes (11): release() must add weight once, NOT double-count., release() must not exceed burst limit., Multiple concurrent releases must sum correctly (no double-count)., test_release_adds_weight_exactly_once(), test_release_caps_at_burst(), test_release_concurrent_safety(), post_with_weight_budget_preacquired(), Leaky-bucket / virtual-clock limiter for this process's API weight share.… (+3 more)

### Community 46 - "test_talent_scout_global_sort.py"
Cohesion: 0.18
Nodes (7): _FakeKernel32, _FakeWinFunction, isolated_table_state(), fixture, parametrize, test_displayed_metric_sort_uses_full_wallet_universe(), test_scout_metric_filters_ignore_copy_engine_wallet_exclusions()

### Community 47 - "4_useful_wallet_scanner.py"
Cohesion: 0.10
Nodes (20): append_summary(), compute_kpis(), compute_trade_kpis(), _ensure_summary_header(), fetch_all_fills(), fetch_recent_fills_probe(), load_progress(), log() (+12 more)

### Community 48 - "test_sizing_normalisation.py"
Cohesion: 0.15
Nodes (17): RawFill, _fill(), Regression coverage for fill-time proportional sizing and USER refresh., test_dashboard_request_queues_refresh_when_polled_equity_changed(), test_explicit_wallet_equity_override_remains_fixed(), test_one_nanosecond_source_change_is_detected(), test_polled_leader_equity_source_change_is_detected(), test_proportional_sizing_uses_leader_equity_at_each_fill() (+9 more)

### Community 49 - "get"
Cohesion: 0.07
Nodes (56): Response, _account_orphan_signed_size(), _age_label_from_ms(), api_get_ui_state(), api_metrics_csv(), _append_exchange_history(), _apply_ownership_truth_to_integrity(), _build_account_orphan_registry() (+48 more)

### Community 51 - "build_model_state"
Cohesion: 0.06
Nodes (45): _account_value_curve(), alignment_status_for_key(), api_wallet_equity_curve(), apply_live_config_wallet_model(), _apply_price_model(), avg(), bps_fee(), build_model_state() (+37 more)

### Community 52 - "universe_builder.py"
Cohesion: 0.13
Nodes (24): append_all_trades(), _build_tail_triple_set(), dedup_trades_file(), estimate_request_weight(), is_garbage(), load_purged_wallets(), log(), _parse_retry_after() (+16 more)

### Community 53 - "start_wallet_finder.ps1"
Cohesion: 0.60
Nodes (5): Get-LogPaths(), Start-Dash(), Start-Feeder(), Test-PortUp(), Test-ScriptRunning()

### Community 54 - "get_adaptive_window_ms"
Cohesion: 0.33
Nodes (6): calculate_optimal_window(), get_adaptive_window_ms(), get_peak_24h_trades(), Get actual peak trades in any 24h window from local trade data (last 30d)., Calculate optimal POLL_WINDOW_MS for a wallet based on peak 24h trades. Target:…, Calculate adaptive window using actual peak 24h trades from local data.

### Community 55 - "hl_ip_budget.py"
Cohesion: 0.22
Nodes (7): default_budget_path(), One weight budget shared by every local Hyperliquid consumer on this IP.…, The process-wide handle to the shared budget, or None if not configured. Absent…, A machine-wide default location, used by the launcher wiring., One non-blocking attempt at the exclusive lock. Deliberately non-blocking on…, shared_budget(), _try_lock_file()

### Community 56 - "inum"
Cohesion: 0.10
Nodes (14): fnum(), inum(), monitor_start_ms(), normalize_side(), Fetch the current clearinghouseState. Returns (positions, fence_ms). fence_ms…, True iff userFillsByTime PROVES no fill for wallet in (lo_ms, hi_ms]. Used to…, Unified REST proof: one logical fetch_fills_since operation that certifies BOTH…, Snapshot native + the given builder DEXes and UNION the positions. Returns… (+6 more)

### Community 58 - "Any"
Cohesion: 0.09
Nodes (37): Any, _active_live_copy_wallet_count(), age_ms_label(), apply_wallet_filters_to_state(), _build_execution_quality_rows(), _build_live_wallet_rows(), _build_reconciliation_execution_quality_rows(), contract_money_equal() (+29 more)

### Community 59 - "_load_state"
Cohesion: 0.29
Nodes (7): _latest_state_with_selection(), _load_state(), on_event, Recover Talent Scout-owned selection keys from the latest rotating backup., Load state from disk. Falls back to .bak if main file is corrupt. Migration: if…, _selected_true_count(), startup()

### Community 60 - "test_talent_scout_selection_responsiveness.py"
Cohesion: 0.20
Nodes (7): _FakeKernel32, _FakeWinFunction, isolated_selection_state(), fixture, parametrize, test_checkbox_selection_returns_without_inline_portfolio_compute(), test_portfolio_remove_queues_refresh_instead_of_computing_inline()

### Community 61 - "_fetch_exchange_account_snapshot"
Cohesion: 0.24
Nodes (10): _account_reconciliation_baseline_timestamp(), _fetch_exchange_account_snapshot(), _fetch_user_fills_by_time(), _fetch_user_realized_pnl_snapshot(), _fetch_user_realized_pnl_snapshot_uncached(), _local_env_value(), _public_account_address(), Realized PnL over days, cached for the operator refresh interval. The uncached… (+2 more)

### Community 62 - "test_hip3_builder_dex.py"
Cohesion: 0.14
Nodes (21): ch_state(), FakeRequests, new_engine(), pos(), Proof tests for the HIP-3 builder-dex snapshot fix (HL_Copy_Engine_SSOT).…, (1) union == native+HIP-3 positions; (2) builder drift disappears., (3) a fill between min and max snapshot time -> union rejected., (4) native reconciliation unchanged; (5) no PER-CYCLE DEX fan-out. (+13 more)

### Community 63 - "log"
Cohesion: 0.19
Nodes (7): log(), Open a fresh MEASURED epoch. This never masks drift. The old implementation set…, Cached list of HIP-3 builder perp-dex names. `clearinghouseState` with NO `dex`…, Builder DEXes to union at COLD BOOTSTRAP: EVERY enumerated DEX. The local…, Create a NEW measured epoch: baseline = current exchange position,…, Compute the capacity invariant dynamically. Reports the measured workload and…, utc_now_ms()

### Community 64 - "JSONResponse"
Cohesion: 0.16
Nodes (21): api_cache_health(), api_engine_health(), api_equity(), api_metrics(), api_model_cache_status(), api_state(), api_trades(), cancel_recon_repair_request() (+13 more)

### Community 65 - "EngineSSOT"
Cohesion: 0.18
Nodes (4): EngineSSOT, Smallest explicit currentness contract. INPUTS_CURRENT answers 'have we…, Frozen manifest of proven pre-fix HIP-3 baseline omissions. Format: {wallet:…, True if a proof job is currently active for this wallet.

### Community 66 - "wallet_proof_epoch_suite.py"
Cohesion: 0.17
Nodes (11): _build(), FailAPI, FakeAPI, fill(), IncompleteAPI, make_engine(), make_engine_keep(), Regression suite for the Wallet Proof epoch repair (42 tests). Covers the… (+3 more)

### Community 67 - "_load_live_copy_config"
Cohesion: 0.33
Nodes (18): add_live_config_wallet(), _enforce_live_copy_cap(), get_live_config(), invalidate_live_audit_summary_cache(), invalidate_model_cache(), _live_config_error(), _live_copy_config_response(), _load_live_copy_config() (+10 more)

### Community 68 - "hl_mtm_lookup.py"
Cohesion: 0.26
Nodes (15): Path, Debug MTM DD values for problem wallets., _cache_path(), _empty_stats(), get_mtm_stats(), get_mtm_stats_async(), hl_mtm_lookup.py — Shared HL portfolio (MTM) lookup utility. WALLET FINDER editi, Read cache without TTL check; used as soft fallback when network is down. (+7 more)

### Community 69 - "HL_Copy_Engine_SSOT.py"
Cohesion: 0.18
Nodes (14): acquire_lock(), bval(), chunked(), ensure_dirs(), main(), normalise_execution_delta_flag(), normalise_recording_method(), datetime (+6 more)

### Community 70 - "hip3_omission_derivation.py"
Cohesion: 0.17
Nodes (15): derive_cohort_from_truth(), derive_omission(), fetch_fills_in_interval(), _fnum(), is_builder_coin(), _normalize_side(), _parse_raw_fill_delta(), HIP-3 baseline-omission derivation: pair-level causal proof. For each candidate… (+7 more)

### Community 71 - "refresh_selected_portfolio_view"
Cohesion: 0.14
Nodes (16): iso_to_ms(), test_render_refresh_rebuilds_user_row_from_current_selection(), apply_user_true_drawdown_rollup(), block(), build_portfolio_history(), compact_history(), _last_trade_ms_from_engine_truth(), Populate USER-only display rollups from selected leader wallets. (+8 more)

### Community 72 - "real_dd_filter.py"
Cohesion: 0.19
Nodes (14): Path, _dd_source_from_mtm(), _extract_dd_from_json(), load_real_dd_for_wallet(), parse_max_real_dd_pct(), real_dd_filter.py — Shared real-drawdown upstream filter.  Provides deterministi, Evaluate the real-DD gate.      Parameters     ----------     stats : dict, Extract real DD from a raw HL portfolio API response.      The response is a lis (+6 more)

### Community 73 - ".poll_once"
Cohesion: 0.16
Nodes (5): Builder DEXes to snapshot for this wallet this cycle. Native is ALWAYS polled.…, Fair paced per-wallet scheduler with proof fence. Each wallet gets its own…, Set the proof fence for a wallet. WS fills newer than fence_hi are buffered,…, Clear the proof fence and return any buffered post-fence fills., Buffer a WS fill that arrived during a proof job.

### Community 74 - "_model_dashboard_response"
Cohesion: 0.19
Nodes (13): test_explicit_dashboard_refresh_rebuilds_from_latest_poll(), get_wallet_meta(), home(), model_dashboard(), _model_dashboard_response(), HTMLResponse, Dashboard home route with non-blocking fallback., Fetch selected, unlocked leader equity, then publish one complete rebuild. (+5 more)

### Community 75 - "test_true_dd_windowing.py"
Cohesion: 0.21
Nodes (11): _point(), Regression coverage for 8014 proof-window TRUE DD and dashboard caching., test_curve_loader_keeps_only_window_seed_and_window_points(), test_dashboard_cold_start_serves_last_good_html_before_json_recovery(), test_dashboard_serves_fresh_cached_html_without_a_model_rebuild(), test_portfolio_true_dd_cannot_admit_a_zero_fill_wallets_old_history(), test_rendered_wallet_row_omits_all_true_drawdown_columns(), test_true_dd_carries_a_quiet_baselined_wallet_forward_as_valid_zero() (+3 more)

### Community 76 - "atomic_write_json"
Cohesion: 0.24
Nodes (5): atomic_write_json(), Advance a wallet's trusted-through watermark. Trusted-through is the END…, utc_now_iso(), test_atomic_write_json_preserves_previous_file_and_cleans_temp_on_failure(), test_atomic_write_json_streams_without_building_complete_string()

### Community 77 - ".audit_position_drift_only"
Cohesion: 0.20
Nodes (4): Builder dex name for a namespaced coin ('XYZ:MRNA' -> 'xyz'), else None.…, Deprecated compatibility wrapper: snapshots do not create fills. The current…, Compare snapshot to ledger state and recover using real fills only. Snapshots…, Controlled epoch roll for a PROVEN defective baseline (fix D + roll). The…

### Community 78 - "Wallet Proof Engine — Epoch Repair Report"
Cohesion: 0.18
Nodes (10): 1. What was wrong (root causes, proven), 2. What changed (all in `HL_Copy_Engine_SSOT.py`, +526 / −126), 3. Regression suite — 26/26 PASS, 4. Cold-restart test on real persisted data, 5. Live API evidence (`userFillsByTime`), 6. Evidence preserved before repair, 7. Status, 8. GPT review response — four gates corrected (revision 2) (+2 more)

### Community 79 - "run_context.py"
Cohesion: 0.52
Nodes (6): get_current_run_id(), latest_run_filter(), _new_run_id(), Run identity helpers for Wallet Finder rebuilds., start_new_run(), write_current_run()

### Community 80 - "guard"
Cohesion: 0.40
Nodes (6): guard(), reset_for_tests(), The fixed slice is the property that holds when everything else fails., The dangerous confusion: 'I did not look' vs 'there was nothing there'.…, test_a_refused_read_is_never_reported_as_an_empty_window(), test_the_process_ceiling_binds_without_any_coordination()

### Community 81 - "load_index_state"
Cohesion: 0.40
Nodes (6): load_index_state(), main_loop(), _new_state(), Persist the trade index to disk so restarts skip the full scan. Writes a single…, Load persisted trade index from disk. Returns state dict or None. Tries pickle…, save_index_state()

### Community 82 - "get_live_audit_summary"
Cohesion: 0.40
Nodes (6): _audit_summary_background_rebuild(), get_live_audit_summary(), Persist the last enriched live summary so App restarts do not blind /live-copy., Background rebuild of live audit summary cache — never blocks the request…, Return live audit summary with non-blocking stale-serve + background rebuild. -…, _save_live_audit_summary_last_good()

### Community 83 - "last_trade_events"
Cohesion: 0.67
Nodes (3): last_trade_events(), StreamingResponse, Push LAST TRADE updates to the browser as engine truth advances. Server-sent…

## Knowledge Gaps
- **13 isolated node(s):** `Path`, `start_all.sh script`, `_FakeKernel32`, `_FakeKernel32`, `Path` (+8 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **4 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Any` connect `Any` to `.append`, `refresh_wallet_leader_equity_source`, `_build_live_leader_performance`, `EngineSSOT`, `1WalletFinder.py`, `load_ui_state`, `inum`, `update_last_trade_times`, `wallet_proof_engine_8014.py`, `fnum`, `test_sizing_normalisation.py`, `get`, `build_model_state`, `inum`, `_fetch_exchange_account_snapshot`, `log`, `JSONResponse`, `EngineSSOT`, `_load_live_copy_config`, `HL_Copy_Engine_SSOT.py`, `hip3_omission_derivation.py`, `refresh_selected_portfolio_view`, `.poll_once`, `_model_dashboard_response`, `atomic_write_json`, `get_live_audit_summary`?**
  _High betweenness centrality (0.352) - this node is a cross-community bridge._
- **Why does `_summarise()` connect `hl_mtm_lookup.py` to `app.py`, `2simplefilter.py`, `load_universe`, `load_universe`, `wallet_talent_scout_8012.py`?**
  _High betweenness centrality (0.225) - this node is a cross-community bridge._
- **Why does `get_mtm_stats()` connect `hl_mtm_lookup.py` to `_build_live_leader_performance`, `refresh_wallet_leader_equity_source`, `wallet_proof_engine_8014.py`, `run_cycle`?**
  _High betweenness centrality (0.200) - this node is a cross-community bridge._
- **What connects `Path`, `start_all.sh script`, `_FakeKernel32` to the rest of the system?**
  _13 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `.append` be split into smaller, more focused modules?**
  _Cohesion score 0.12535612535612536 - nodes in this community are weakly interconnected._
- **Should `get_table_rows` be split into smaller, more focused modules?**
  _Cohesion score 0.14705882352941177 - nodes in this community are weakly interconnected._
- **Should `_build_live_leader_performance` be split into smaller, more focused modules?**
  _Cohesion score 0.12 - nodes in this community are weakly interconnected._