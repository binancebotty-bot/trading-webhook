# Graph Report - WALLET FINDER  (2026-10-12)

## Corpus Check
- 76 files · ~308,945 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 21 file(s) not represented in the graph (top: .err 8, .prev 8, .pre_restart_20260806_095418 2)

## Summary
- 2317 nodes · 6520 edges · 89 communities (79 shown, 10 thin omitted)
- Extraction: 98% EXTRACTED · 2% INFERRED · 0% AMBIGUOUS · INFERRED: 100 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `2c99f5eb`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- _live_audit_summary
- test_cohort_equity_rebuild.py
- test_gate_a_proof_correctness.py
- app.py
- hl_rate_guard.py
- 3_mtm_filter.py
- get_table_rows
- fnum
- refresh_wallet_leader_equity_source
- EngineSSOT
- get_table_rows
- proof_wallets_txt
- 2simplefilter.py
- wallet_intake.py
- load_ui_state
- proof_wallets_txt
- load_universe
- inum
- time
- 5_reconstructed_drawdown.py
- atomic_write_json
- test_import_copy_candidates.py
- _save_state
- wallet_talent_scout_8012.py
- _table_context
- test_universe_builder.py
- get
- wallet_proof_epoch_suite.py
- test_pipeline_policy.py
- _get_visible_universe
- _get_visible_universe
- HL_Copy_App_SSOT.py
- test_ssot_backoff.py
- test_hip3_baseline_roll.py
- fnum
- stop_all.sh
- Any
- start_all.sh
- load_ui_state
- get
- G1-B2 Failed Patches — Read-Only Recovery Analysis
- test_gate_b1_shard_status.py
- _load_state
- atomic_write_json
- test_talent_scout_global_sort.py
- csv
- test_gate_b2_freshness_fence.py
- Any
- 8014 Process Identity & Proof Status Evidence
- build_model_state
- asyncio
- start_wallet_finder.ps1
- test_gate_a1_proof_span.py
- _Queue
- Any
- log
- test_gate_c_capacity_deferral.py
- load_universe
- test_gate_c2_cold_bootstrap_retry.py
- wallet_proof_engine_8014.py
- test_hip3_builder_dex.py
- ._process_wallet_proof
- get
- 2stage1_simple.py
- threading
- 1WalletFinder.py
- json
- HL_Copy_Engine_SSOT.py
- _Queue
- WP-8014 D1–D7 Status + Why 7 Wallets Remain Unproven — Read-Only Diagnosis
- guard
- fnum
- IpWeightBudget
- test_true_dd_windowing.py
- last_trade_events
- EngineSSOT
- Wallet Proof Engine — Epoch Repair Report
- test_gate_c1_cold_bootstrap_retry.py
- render_chart
- hl_ip_budget.py
- SSOT Writer Recovery Evidence — 2026-10-12
- emergency_live_dashboard
- os
- _Recorder
- WP-8014 DUPLICATE-CORE READ-ONLY SAFETY TRIAGE — REDACTED EVIDENCE
- WP-8014-DUPLICATE-CORE-CLOSURE-2 — THREE ACCEPTANCE GAPS CLOSED
- .__init__

## God Nodes (most connected - your core abstractions)
1. `fnum()` - 97 edges
2. `EngineSSOT` - 86 edges
3. `fnum()` - 79 edges
4. `build_model_state()` - 55 edges
5. `EngineSSOT` - 44 edges
6. `build_model_state()` - 43 edges
7. `log()` - 42 edges
8. `_live_audit_summary()` - 41 edges
9. `_live_audit_summary()` - 41 edges
10. `inum()` - 38 edges

## Surprising Connections (you probably didn't know these)
- `skip_reason()` --indirect_call--> `wallet()`  [INFERRED]
  wallet_proof_engine_8014.py → tests/test_cohort_equity_rebuild.py
- `test_background_build_waits_for_cohort_refresh_lock()` --calls--> `_kick_model_cache_refresh_background()`  [EXTRACTED]
  tests/test_cohort_equity_rebuild.py → wallet_proof_engine_8014.py
- `test_full_dashboard_renders_cohort_refresh_result_and_controls()` --calls--> `render_home()`  [EXTRACTED]
  tests/test_cohort_equity_rebuild.py → wallet_proof_engine_8014.py
- `test_import_reports_missing_candidate_source()` --calls--> `import_copy_candidates_to_manual_wallets()`  [EXTRACTED]
  tests/test_import_copy_candidates.py → wallet_proof_engine_8014.py
- `test_scout_metric_filters_ignore_copy_engine_wallet_exclusions()` --calls--> `_apply_filters()`  [EXTRACTED]
  tests/test_talent_scout_global_sort.py → wallet_talent_scout_8012.py

## Import Cycles
- None detected.

## Communities (89 total, 10 thin omitted)

### Community 0 - "_live_audit_summary"
Cohesion: 0.10
Nodes (36): _account_orphan_signed_size(), _account_reconciliation_baseline_timestamp(), _append_exchange_history(), _apply_ownership_truth_to_integrity(), _audit_summary_background_rebuild(), _build_account_orphan_registry(), _build_account_reconciliation(), _build_live_top_status() (+28 more)

### Community 1 - "test_cohort_equity_rebuild.py"
Cohesion: 0.16
Nodes (9): test_background_build_waits_for_cohort_refresh_lock(), test_cohort_fetch_only_selected_unlocked_model_rows(), test_failed_fetch_retains_previous_cache_and_does_not_invalidate_model(), test_freeze_or_deselection_during_rate_wait_prevents_fetch(), test_full_dashboard_renders_cohort_refresh_result_and_controls(), test_refresh_route_fetches_before_one_forced_build_and_publishes_summary(), wallet(), test_wallet_config_endpoint_persists_lock_and_starts_rebuild() (+1 more)

### Community 2 - "test_gate_a_proof_correctness.py"
Cohesion: 0.18
Nodes (12): ch_state(), engine(), FakeRequests, fill_row(), make_fake_requests(), ok(), pos(), Resp (+4 more)

### Community 3 - "app.py"
Cohesion: 0.16
Nodes (25): _apply_date_range_to_series(), _current_table_days(), _curve_metrics(), _display_ssot_dd(), _ensure_trade_replay_series(), get_portfolio_data(), _get_wallet_pnl_series(), _load_curve() (+17 more)

### Community 4 - "hl_rate_guard.py"
Cohesion: 0.21
Nodes (4): SharedBudgetUnavailable, RateGuard, shared_budget(), weight_for()

### Community 5 - "3_mtm_filter.py"
Cohesion: 0.09
Nodes (17): fetch_mtm(), gate_mtm(), load_progress(), load_wallets(), log(), main(), _run_pass(), save_progress() (+9 more)

### Community 6 - "get_table_rows"
Cohesion: 0.12
Nodes (14): test_global_sort_happens_before_pagination_and_preserves_global_rank(), _active_filter_count(), _activity_bucket(), _activity_counts(), _fmt(), get_table_rows(), _is_dormant(), _is_slow() (+6 more)

### Community 7 - "fnum"
Cohesion: 0.06
Nodes (59): active_wallet(), active_wallet_row(), avg(), block_num(), _build_live_wallet_derived(), core_missing(), core_td(), css_class() (+51 more)

### Community 8 - "refresh_wallet_leader_equity_source"
Cohesion: 0.29
Nodes (4): test_targeted_equity_refresh_uses_existing_cache_writer(), _get_mtm_stats(), _leader_equity_source_snapshot(), refresh_wallet_leader_equity_source()

### Community 9 - "EngineSSOT"
Cohesion: 0.06
Nodes (31): acquire_lock(), atomic_write_json(), bval(), chunked(), CsvLedger, EngineSSOT, on_close(), on_error() (+23 more)

### Community 10 - "get_table_rows"
Cohesion: 0.12
Nodes (12): _activity_bucket(), _build_sparkline(), _fmt(), get_table_rows(), _is_dormant(), _is_slow(), _is_stale(), _last_trade_age_fmt() (+4 more)

### Community 11 - "proof_wallets_txt"
Cohesion: 0.48
Nodes (6): export_selected(), proof_wallets_txt(), _selected_wallets_ordered(), selected_wallets_txt(), visible_wallets_txt(), _wallet_text()

### Community 12 - "2simplefilter.py"
Cohesion: 0.15
Nodes (9): cache_path(), extract_wallet_stats(), fetch_portfolio(), legacy_main_pre_intake_repair(), load_source_wallets(), main(), read_cache(), save_progress() (+1 more)

### Community 13 - "wallet_intake.py"
Cohesion: 0.32
Nodes (10): load_intake_rows(), load_manual_injections(), add(), migrate_discovery_csv(), normalize_wallet(), _read_intake_file(), _row(), _truthy() (+2 more)

### Community 14 - "load_ui_state"
Cohesion: 0.09
Nodes (51): _active_live_copy_wallet_count(), add_live_config_wallet(), admin_purge_wallet(), api_import_copy_candidates(), api_norm(), api_set_ui_state(), api_wallet_equity_refresh(), api_wallet_meta() (+43 more)

### Community 15 - "proof_wallets_txt"
Cohesion: 0.39
Nodes (7): export_selected(), proof_wallets_txt(), selected_wallets_json(), _selected_wallets_ordered(), selected_wallets_txt(), visible_wallets_txt(), _wallet_text()

### Community 16 - "load_universe"
Cohesion: 0.11
Nodes (15): _compute_compound_score(), data_status_card(), _display_ssot_dd(), _find_all_trades_path(), _load_last_trade_times(), _load_purged_wallets(), _load_trade_stats(), _load_trade_stats_for_wallets() (+7 more)

### Community 17 - "inum"
Cohesion: 0.08
Nodes (18): age_ms_label(), build_computed_true_drawdown_history(), build_proof_status_block(), n(), _build_recent_send_warning_groups(), _computed_equity_curve(), _computed_true_drawdown_summary(), _drawdown_from_curve_points() (+10 more)

### Community 18 - "time"
Cohesion: 0.17
Nodes (14): audit_for(), dd_from_pts(), fetch_ch(), fetch_funding_72h(), fetch_ledger_72h(), fetch_portfolio(), fetch_user_fills_72h(), http_post() (+6 more)

### Community 19 - "5_reconstructed_drawdown.py"
Cohesion: 0.06
Nodes (23): assess_status(), compute_reconstructed_curve(), _empty_candidate(), load_copy_ready_wallets(), load_trades_for_wallets(), main(), _write_csv(), derive_cohort_from_truth() (+15 more)

### Community 20 - "atomic_write_json"
Cohesion: 0.12
Nodes (29): atomic_write_csv(), atomic_write_json(), _atomic_write_json(), backup_purge_files(), _candidate_wallet_file(), _file_age_payload(), import_copy_candidates_to_manual_wallets(), _refresh() (+21 more)

### Community 21 - "test_import_copy_candidates.py"
Cohesion: 0.50
Nodes (3): test_import_does_not_consult_true_dd_or_model_state(), test_import_reports_missing_candidate_source(), _wallet()

### Community 22 - "_save_state"
Cohesion: 0.17
Nodes (28): apply_filter(), clear_all(), clear_filter(), data_status(), data_status_card(), _get_sort(), index(), portfolio_panel() (+20 more)

### Community 23 - "wallet_talent_scout_8012.py"
Cohesion: 0.09
Nodes (33): _apply_date_range_to_series(), _build_sparkline(), _current_table_days(), _curve_metrics(), _ensure_trade_replay_series(), get_portfolio_data(), _get_portfolio_data_async(), _get_wallet_pnl_series() (+25 more)

### Community 24 - "_table_context"
Cohesion: 0.15
Nodes (8): _active_filter_count(), _activity_counts(), _render_filter_bar(), _table_context(), test_header_pagination_agree(), test_header_uses_total_rows(), test_per_page_not_three(), test_table_context_returns_total_rows()

### Community 25 - "test_universe_builder.py"
Cohesion: 0.07
Nodes (16): _fake_adaptive_window(), _FakePost, _mock_response(), _mock_session(), _noop_budget(), test_poll_extended_recovery_retries_after_error(), test_poll_extended_recovery_returns_none_after_all_fail(), test_poll_extended_recovery_sleeps_between_retries() (+8 more)

### Community 26 - "get"
Cohesion: 0.19
Nodes (26): apply_filter(), clear_all(), clear_filter(), data_status(), _get_sort(), healthz(), index(), portfolio_panel() (+18 more)

### Community 27 - "wallet_proof_epoch_suite.py"
Cohesion: 0.14
Nodes (9): _build(), FailAPI, FakeAPI, fill(), IncompleteAPI, make_engine(), make_engine_keep(), reset_persisted() (+1 more)

### Community 28 - "test_pipeline_policy.py"
Cohesion: 0.12
Nodes (9): load_script(), test_8012_filters_are_invariant_to_normalization(), test_8012_realised_dd_display_is_not_mtm_capped(), test_8012_sparse_mtm_is_floored_by_realised_dd_for_risk(), test_8014_candidate_import_is_not_a_true_dd_promotion_gate(), test_manual_intake_no_longer_sets_force_poll(), test_stage2_hard_fails_true_blockers_only(), test_stage2_martingale_only_is_nonblocking_copy_ready_caution() (+1 more)

### Community 29 - "_get_visible_universe"
Cohesion: 0.20
Nodes (8): _apply_filters(), _get_visible_universe(), _load_summary_kpis(), _overlay_cached_mtm(), _purge_losing_wallets_from_df(), _sort_universe(), _sort_universe_pinned(), _table_execution_required()

### Community 30 - "_get_visible_universe"
Cohesion: 0.20
Nodes (8): test_displayed_metric_sort_uses_full_wallet_universe(), _apply_filters(), _get_visible_universe(), _load_summary_kpis(), _purge_losing_wallets_from_df(), _sort_universe(), _sort_universe_pinned(), _table_execution_required()

### Community 31 - "HL_Copy_App_SSOT.py"
Cohesion: 0.06
Nodes (52): alignment_status_for_key(), _apply_price_model(), bps_fee(), build_model_state(), append_expected_fill(), copy_wallet_exposure(), record_copy_exposure(), record_entry_notional() (+44 more)

### Community 32 - "test_ssot_backoff.py"
Cohesion: 0.21
Nodes (9): _drifting_snapshot(), _engine(), test_a_clean_wallet_is_never_investigated_at_all(), test_a_quiesced_drift_is_still_reported_truthfully(), test_new_fills_reopen_the_investigation_immediately(), test_the_first_unexplained_drift_is_investigated(), test_the_retry_interval_eventually_reopens_it(), test_the_same_unexplained_drift_is_not_investigated_twice() (+1 more)

### Community 33 - "test_hip3_baseline_roll.py"
Cohesion: 0.07
Nodes (19): ch_state(), FakeRequests, new_engine(), pos(), _Resp, test_derivation_buy_sell_signed_delta(), test_derivation_excludes_fill_at_old_fence(), test_derivation_ignores_other_coin_fills() (+11 more)

### Community 34 - "fnum"
Cohesion: 0.06
Nodes (57): active_wallet(), active_wallet_row(), api_metrics(), _append_exchange_history(), avg(), block_num(), _build_account_reconciliation(), contract_money_equal() (+49 more)

### Community 35 - "stop_all.sh"
Cohesion: 0.83
Nodes (3): stop_all.sh script, stop_by_pid_file(), stop_by_port()

### Community 36 - "Any"
Cohesion: 0.06
Nodes (60): _age_label_from_ms(), apply_wallet_filters_to_state(), _row_metric(), _build_execution_quality_freshness(), latest(), _build_execution_quality_rows(), _build_execution_quality_summary(), _build_live_leader_performance() (+52 more)

### Community 39 - "load_ui_state"
Cohesion: 0.11
Nodes (43): _active_live_copy_wallet_count(), add_live_config_wallet(), admin_purge_wallet(), api_import_copy_candidates(), api_norm(), api_set_ui_state(), api_wallet_meta(), apply_live_config_wallet_model() (+35 more)

### Community 40 - "get"
Cohesion: 0.06
Nodes (34): api_cache_health(), api_equity(), api_get_ui_state(), api_metrics(), api_metrics_csv(), api_model_cache_status(), api_state(), api_trades() (+26 more)

### Community 42 - "G1-B2 Failed Patches — Read-Only Recovery Analysis"
Cohesion: 0.09
Nodes (21): 1. Problem Statement, 2.1 G1-B2 First Patch (comment 6103230323), 2.2 G1-B2-R1 First Patch (comment 6103301135), 2.3 G1-B2-R1 Second Patch (comment 6103317033), 2. Failed Attempts, 3. Root Cause, 4. Controller's Acceptance Invariant (from comment 6103301135), 5.1 Data Selection (per-account, not subtree merge) (+13 more)

### Community 43 - "test_gate_b1_shard_status.py"
Cohesion: 0.16
Nodes (13): engine(), ok(), ready_engine(), shard_engine(), test_b17_control_no_fence_queues_the_fill(), test_b17_receive_path_takes_one_acquisition(), test_b18_fill_older_than_fence_is_queued(), test_b19_closed_shard_with_recent_data_is_not_fresh() (+5 more)

### Community 44 - "_load_state"
Cohesion: 0.33
Nodes (4): _latest_state_with_selection(), _load_state(), _selected_true_count(), startup()

### Community 45 - "atomic_write_json"
Cohesion: 0.14
Nodes (26): atomic_write_csv(), atomic_write_json(), _atomic_write_json(), backup_purge_files(), _candidate_wallet_file(), _file_age_payload(), import_copy_candidates_to_manual_wallets(), _live_order_intents_path() (+18 more)

### Community 46 - "test_talent_scout_global_sort.py"
Cohesion: 0.09
Nodes (9): _FakeKernel32, _FakeWinFunction, isolated_table_state(), test_scout_metric_filters_ignore_copy_engine_wallet_exclusions(), _FakeKernel32, _FakeWinFunction, isolated_selection_state(), test_checkbox_selection_returns_without_inline_portfolio_compute() (+1 more)

### Community 47 - "csv"
Cohesion: 0.17
Nodes (15): append_all_trades(), append_summary(), compute_kpis(), fetch_all_fills(), load_progress(), log(), main(), process() (+7 more)

### Community 48 - "test_gate_b2_freshness_fence.py"
Cohesion: 0.13
Nodes (12): ok(), _Queue, shard_engine(), test_b21_control_all_open_is_fresh(), test_b21_empty_string_status_is_not_fresh(), test_b21_no_status_is_not_fresh(), test_b22_control_above_fence_buffered_and_below_queued(), test_b22_control_proven_fill_is_queued() (+4 more)

### Community 49 - "Any"
Cohesion: 0.06
Nodes (72): _account_orphan_signed_size(), _account_value_curve(), _age_label_from_ms(), api_wallet_equity_curve(), _apply_ownership_truth_to_integrity(), _build_account_orphan_registry(), _build_execution_quality_freshness(), latest() (+64 more)

### Community 50 - "8014 Process Identity & Proof Status Evidence"
Cohesion: 0.25
Nodes (7): 8014 Process Identity & Proof Status Evidence, Conclusion, Current Proof Status (from live 8014 page), Process Serving Port 8014, Root Cause from engine_truth.json currentness, Source Match to GitHub, SSOT Data Being Read

### Community 51 - "build_model_state"
Cohesion: 0.05
Nodes (56): _fill(), test_explicit_wallet_equity_override_remains_fixed(), test_one_nanosecond_source_change_is_detected(), test_polled_leader_equity_source_change_is_detected(), test_proportional_sizing_uses_leader_equity_at_each_fill(), test_render_refresh_rebuilds_user_row_from_current_selection(), test_unchanged_nanosecond_mtime_is_compared_without_float_rounding(), test_unlocked_saved_equity_value_does_not_freeze_sizing() (+48 more)

### Community 52 - "asyncio"
Cohesion: 0.15
Nodes (23): full_cycle_cached(), poll_one(), test_cycle(), poll_one(), append_all_trades(), compute_wallet_metrics(), dedup_trades_file(), is_garbage() (+15 more)

### Community 53 - "start_wallet_finder.ps1"
Cohesion: 0.60
Nodes (5): Get-LogPaths(), Start-Dash(), Start-Feeder(), Test-PortUp(), Test-ScriptRunning()

### Community 54 - "test_gate_a1_proof_span.py"
Cohesion: 0.17
Nodes (13): engine(), install_fetch(), _isolate_proof_state(), ok(), row(), test_a11_control_clean_span_is_coherent(), test_a11_fill_before_start_ms_inside_span_is_caught(), test_a12_control_equal_boundary_accepted() (+5 more)

### Community 56 - "Any"
Cohesion: 0.11
Nodes (10): bval(), _row_id(), inum(), iso_to_ms(), monitor_start_ms(), normalise_execution_delta_flag(), normalise_recording_method(), normalize_side() (+2 more)

### Community 57 - "log"
Cohesion: 0.10
Nodes (7): on_close(), on_error(), on_open(), log(), utc_now_ms(), 8. GPT review response — four gates corrected (revision 2), Honest note on CI

### Community 58 - "test_gate_c_capacity_deferral.py"
Cohesion: 0.11
Nodes (16): c1(), c10(), c11(), c2(), c3(), c4(), c5(), c6() (+8 more)

### Community 59 - "load_universe"
Cohesion: 0.12
Nodes (11): _compute_compound_score(), _find_all_trades_path(), _latest_state_with_selection(), _load_last_trade_times(), _load_purged_wallets(), _load_state(), _load_trade_stats(), _load_trade_stats_for_wallets() (+3 more)

### Community 60 - "test_gate_c2_cold_bootstrap_retry.py"
Cohesion: 0.18
Nodes (8): c2_1_cold_bootstrap_deferral_retries_at_60s(), c2_2_control_real_cold_bootstrap_failure_uses_full_cadence(), c2_3_ready_wallet_deferral_still_short(), _cold_bootstrap_engine(), engine(), ok(), _OneShot, _run_one_poll_pass()

### Community 61 - "wallet_proof_engine_8014.py"
Cohesion: 0.07
Nodes (36): _account_reconciliation_baseline_timestamp(), alignment_status_for_key(), apply_wallet_filters_to_state(), _row_metric(), close_positions_fifo(), _earliest_real_order_filled_ms(), _execution_guards_info(), ev() (+28 more)

### Community 62 - "test_hip3_builder_dex.py"
Cohesion: 0.07
Nodes (22): ch_state(), FakeRequests, new_engine(), pos(), _Resp, test_buffered_fills_released_only_after_reconciliation(), test_cold_bootstrap_discovers_preledger_builder_position(), test_cold_bootstrap_no_historical_fetch() (+14 more)

### Community 64 - "get"
Cohesion: 0.06
Nodes (34): test_dashboard_request_queues_refresh_when_polled_equity_changed(), test_explicit_dashboard_refresh_rebuilds_from_latest_poll(), api_cache_health(), api_engine_health(), api_equity(), api_get_ui_state(), api_metrics_csv(), api_model_cache_status() (+26 more)

### Community 65 - "2stage1_simple.py"
Cohesion: 0.19
Nodes (7): extract_wallet_stats(), fetch_portfolio(), load_source_wallets(), main(), read_cache(), save_progress(), write_cache()

### Community 66 - "threading"
Cohesion: 0.22
Nodes (9): engine(), make_fill(), ok(), test_b1_no_fill_is_stranded_by_the_drain(), producer(), test_b2_concurrent_fills_applied_exactly_once(), worker(), test_b3_freshness_requires_every_shard() (+1 more)

### Community 67 - "1WalletFinder.py"
Cohesion: 0.26
Nodes (9): append_csv(), get_coins(), init_csv(), inject_leaderboard_wallets(), load_existing_wallets(), main(), poll_worker(), post() (+1 more)

### Community 68 - "json"
Cohesion: 0.12
Nodes (15): _cache_path(), _empty_stats(), get_mtm_stats(), get_mtm_stats_async(), _read_cache(), _read_cache_any(), _summarise(), _write_cache() (+7 more)

### Community 69 - "HL_Copy_Engine_SSOT.py"
Cohesion: 0.09
Nodes (13): acquire_lock(), atomic_write_json(), CsvLedger, ensure_dirs(), load_manual_wallets(), main(), _handler(), _RateBudgetDeferred (+5 more)

### Community 71 - "WP-8014 D1–D7 Status + Why 7 Wallets Remain Unproven — Read-Only Diagnosis"
Cohesion: 0.18
Nodes (10): 1. Do the seven SSOT defects (D1–D7) remain in the running engine? — NO, 2. Compare running code with the October 7 repair — MATCH, 3. Why do seven wallets remain unproven? — TWO distinct causes, neither is a core defect, 4. CRITICAL FINDING — the SSOT writer is NOT running, 5. Shortest safe route (for controller ruling), 6. Constraints honoured, Cause A — 5 of 7 are NOT in the live cohort (stale epoch records), Cause B — 2 of 7 ARE in the cohort but never bootstrap (rate-budget starvation) (+2 more)

### Community 72 - "guard"
Cohesion: 0.40
Nodes (4): guard(), reset_for_tests(), test_a_refused_read_is_never_reported_as_an_empty_window(), test_the_process_ceiling_binds_without_any_coordination()

### Community 75 - "test_true_dd_windowing.py"
Cohesion: 0.21
Nodes (10): _point(), test_curve_loader_keeps_only_window_seed_and_window_points(), test_dashboard_cold_start_serves_last_good_html_before_json_recovery(), test_dashboard_serves_fresh_cached_html_without_a_model_rebuild(), test_portfolio_true_dd_cannot_admit_a_zero_fill_wallets_old_history(), test_rendered_wallet_row_omits_all_true_drawdown_columns(), test_true_dd_carries_a_quiet_baselined_wallet_forward_as_valid_zero(), test_true_dd_never_falls_back_to_full_history_without_a_proof_window() (+2 more)

### Community 76 - "last_trade_events"
Cohesion: 0.18
Nodes (5): _get_last_trade_lock(), _last_trade_cells_snapshot(), last_trade_events(), stream(), refresh_last_trade_overlay()

### Community 78 - "Wallet Proof Engine — Epoch Repair Report"
Cohesion: 0.22
Nodes (8): 1. What was wrong (root causes, proven), 2. What changed (all in `HL_Copy_Engine_SSOT.py`, +526 / −126), 3. Regression suite — 26/26 PASS, 4. Cold-restart test on real persisted data, 5. Live API evidence (`userFillsByTime`), 6. Evidence preserved before repair, 7. Status, Wallet Proof Engine — Epoch Repair Report

### Community 79 - "test_gate_c1_cold_bootstrap_retry.py"
Cohesion: 0.25
Nodes (5): engine(), _Queue, test_c1_snapshot_budget_refusal_not_a_failure(), test_cold_bootstrap_deferral_is_fail_closed_and_not_a_failure(), test_cold_bootstrap_genuine_failure_still_distinct_from_deferral()

### Community 80 - "render_chart"
Cohesion: 0.31
Nodes (8): render_chart(), compress_points(), compress_true_points(), poly(), poly_points(), x_at(), x_for_t(), y_at()

### Community 81 - "hl_ip_budget.py"
Cohesion: 0.15
Nodes (3): default_budget_path(), shared_budget(), _try_lock_file()

### Community 82 - "SSOT Writer Recovery Evidence — 2026-10-12"
Cohesion: 0.17
Nodes (11): Bootstrap Progress, Epoch Status (from proof_epoch.json — stale), Fresh Evidence Files, Key Findings, Next Steps, Nonce, Recovery Gate Status (fresh), Safeguard Verification (Pre-Start) (+3 more)

### Community 83 - "emergency_live_dashboard"
Cohesion: 0.53
Nodes (5): emergency_live_dashboard(), esc(), kv(), pill(), row_table()

### Community 84 - "os"
Cohesion: 0.13
Nodes (9): main(), get_current_run_id(), latest_run_filter(), _new_run_id(), start_new_run(), write_current_run(), engine(), main() (+1 more)

### Community 86 - "WP-8014 DUPLICATE-CORE READ-ONLY SAFETY TRIAGE — REDACTED EVIDENCE"
Cohesion: 0.22
Nodes (8): 1) DUPLICATE CORE — PIDs 13372 / 46304, 2) SEPARATE AUTHORISED LIVE-COPY RUNTIME — checked independently, 3) ACCOUNT OVERLAP — CRITICAL, 4) FIVE-FILE SSOT INVENTORY (UTC mtime / size / sha256) @ effective output path, 5) THREE-MINUTE WAKE — VERIFIED, 6) GRAPHIFY, SUMMARY, WP-8014 DUPLICATE-CORE READ-ONLY SAFETY TRIAGE — REDACTED EVIDENCE

### Community 89 - "WP-8014-DUPLICATE-CORE-CLOSURE-2 — THREE ACCEPTANCE GAPS CLOSED"
Cohesion: 0.33
Nodes (5): A. SINGLE SEND LOOP EXCLUSIVITY (PIDs 46304/13372) — VERIFIED / UNPROVEN, B. THREE VERIFIED THREE-MINUTE WAKE EVENTS — PARTIAL / FIRST_FAILED_EDGE, C. GRAPHIFY TIMESTAMP RECONCILIATION — RESOLVED, SUMMARY, WP-8014-DUPLICATE-CORE-CLOSURE-2 — THREE ACCEPTANCE GAPS CLOSED

### Community 91 - ".__init__"
Cohesion: 0.19
Nodes (3): RawFill, RawPosition, RawWallet

## Knowledge Gaps
- **10 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `EngineSSOT` connect `EngineSSOT` to `HL_Copy_Engine_SSOT.py`, `fnum`, `test_gate_a1_proof_span.py`, `Any`, `log`, `.__init__`, `._process_wallet_proof`?**
  _High betweenness centrality (0.040) - this node is a cross-community bridge._
- **Should `_live_audit_summary` be split into smaller, more focused modules?**
  _Cohesion score 0.10299003322259136 - nodes in this community are weakly interconnected._
- **Why does `utc_now_ms()` connect `log` to `threading`, `HL_Copy_Engine_SSOT.py`, `fnum`, `test_gate_b1_shard_status.py`, `EngineSSOT`, `test_gate_b2_freshness_fence.py`, `time`, `Any`, `test_gate_c_capacity_deferral.py`, `.__init__`, `test_gate_c2_cold_bootstrap_retry.py`, `._process_wallet_proof`?**
  _High betweenness centrality (0.024) - this node is a cross-community bridge._
- **Should `3_mtm_filter.py` be split into smaller, more focused modules?**
  _Cohesion score 0.09411764705882353 - nodes in this community are weakly interconnected._
- **Should `get_table_rows` be split into smaller, more focused modules?**
  _Cohesion score 0.11904761904761904 - nodes in this community are weakly interconnected._
- **Should `fnum` be split into smaller, more focused modules?**
  _Cohesion score 0.0567287784679089 - nodes in this community are weakly interconnected._
- **Should `EngineSSOT` be split into smaller, more focused modules?**
  _Cohesion score 0.060352233676975946 - nodes in this community are weakly interconnected._