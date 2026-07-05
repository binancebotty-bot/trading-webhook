import json
import tempfile
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add the parent directory to sys.path so we can import real_dd_filter
sys.path.insert(0, str(Path(__file__).parent.parent))

from real_dd_filter import (
    parse_max_real_dd_pct,
    real_dd_gate,
    load_real_dd_for_wallet,
    wallet_passes_real_dd,
    _DD_SOURCE_BLOCKED,
    _DD_SOURCE_LOCAL,
    _DD_SOURCE_FETCHED,
)


def test_parse_max_real_dd_pct_sign_convention():
    """A. sign convention"""
    stats = {
        "allTime_max_drawdown_mtm": -100,
        "allTime_acctV_peak": 1000,
        "mtm_source": "hl_portfolio_api"
    }
    dd_pct, dd_source = parse_max_real_dd_pct(stats)
    assert dd_pct == 10.0
    assert dd_source == "FETCHED_ACCOUNT_VALUE_HISTORY"


def test_parse_max_real_dd_pct_positive_dd():
    """B. positive DD also works"""
    stats = {
        "allTime_max_drawdown_mtm": 100,  # positive drawdown (unusual but possible)
        "allTime_acctV_peak": 1000,
        "mtm_source": "cache_stale"
    }
    dd_pct, dd_source = parse_max_real_dd_pct(stats)
    assert dd_pct == 10.0
    assert dd_source == "LOCAL_ACCOUNT_VALUE_HISTORY"


def test_real_dd_gate_threshold_pass():
    """C. threshold pass"""
    stats = {
        "allTime_max_drawdown_mtm": -200,  # 20% DD
        "allTime_acctV_peak": 1000,
        "mtm_source": "hl_portfolio_api"
    }
    gate = real_dd_gate(stats, max_real_dd_pct=50.0)  # 50% threshold
    assert gate["dd_gate_pass"] is True
    assert gate["real_max_dd_pct"] == 20.0


def test_real_dd_gate_threshold_fail():
    """D. threshold fail"""
    stats = {
        "allTime_max_drawdown_mtm": -300,  # 30% DD from peak 500 = 60% DD
        "allTime_acctV_peak": 500,
        "mtm_source": "hl_portfolio_api"
    }
    gate = real_dd_gate(stats, max_real_dd_pct=50.0)  # 50% threshold -> should fail (60% > 50%)
    assert gate["dd_gate_pass"] is False
    assert "REAL_DD_TOO_HIGH" in gate["dd_gate_reason"]
    assert gate["real_max_dd_pct"] == 60.0


def test_real_dd_gate_missing_data_fails_when_threshold_enabled():
    """E. missing data fails when threshold enabled"""
    stats = {
        "mtm_source": "unavailable"
    }
    gate = real_dd_gate(stats, max_real_dd_pct=50.0)
    assert gate["dd_gate_pass"] is False
    assert gate["dd_gate_reason"] == "DATA_FETCH_BLOCKED"


def test_real_dd_gate_missing_data_does_not_silently_pass():
    """F. missing data does not silently pass"""
    stats = {
        "allTime_max_drawdown_mtm": None,
        "allTime_acctV_peak": None,
        "mtm_source": "unavailable"
    }
    gate = real_dd_gate(stats, max_real_dd_pct=100.0)  # high threshold
    assert gate["dd_gate_pass"] is False
    assert gate["dd_gate_reason"] == "DATA_FETCH_BLOCKED"


def test_real_dd_gate_independence_from_other_gates():
    """G. Stage 1.5 gate preserves existing gates"""
    # Stats with good DD but we'll simulate other gates failing
    stats = {
        "allTime_max_drawdown_mtm": -50,   # 5% DD
        "allTime_acctV_peak": 1000,
        "mtm_source": "hl_portfolio_api"
    }
    
    # real_dd_gate should pass with 10% threshold
    gate = real_dd_gate(stats, max_real_dd_pct=10.0)
    assert gate["dd_gate_pass"] is True
    assert gate["real_max_dd_pct"] == 5.0
    
    # The real_dd_gate function doesn't affect other gates - it just returns its own result
    # This test verifies the function is independent by checking it returns correct DD gate result


def test_wallet_passes_real_dd_full_universe_prefilter(tmp_path):
    """H. full_universe prefilter"""
    # Create temporary directory structure
    cache_dir = tmp_path / "wallet_portfolios"
    cache_dir.mkdir()
    
    # Portfolio JSONs use format: [[period_label, {accountValueHistory: [[ts_ms, "val"], ...]}], ...]
    # High-DD wallet: 80% DD (1000 → 200)
    high_dd_wallet = "0x1111111111111111111111111111111111111111"
    high_dd_data = [
        ["allTime", {
            "accountValueHistory": [
                [1000000, "1000.0"],
                [2000000, "200.0"],
            ]
        }]
    ]
    (cache_dir / f"{high_dd_wallet.lower()}.json").write_text(json.dumps(high_dd_data))
    
    # Low-DD wallet: 20% DD (1000 → 800)
    low_dd_wallet = "0x2222222222222222222222222222222222222222"
    low_dd_data = [
        ["allTime", {
            "accountValueHistory": [
                [1000000, "1000.0"],
                [2000000, "800.0"],
            ]
        }]
    ]
    (cache_dir / f"{low_dd_wallet.lower()}.json").write_text(json.dumps(low_dd_data))
    
    # Missing JSON wallet
    missing_wallet = "0x3333333333333333333333333333333333333333"
    
    # Test with 50% threshold
    # High-DD wallet should fail (80% > 50%)
    result_high = wallet_passes_real_dd(high_dd_wallet, max_real_dd_pct=50.0, cache_dirs=[cache_dir])
    assert result_high["dd_gate_pass"] is False
    assert "REAL_DD_TOO_HIGH" in result_high["dd_gate_reason"]
    assert result_high["real_max_dd_pct"] == 80.0  # (1000-200)/1000 * 100 = 80%
    
    # Low-DD wallet should pass (20% < 50%)
    result_low = wallet_passes_real_dd(low_dd_wallet, max_real_dd_pct=50.0, cache_dirs=[cache_dir])
    assert result_low["dd_gate_pass"] is True
    assert result_low["real_max_dd_pct"] == 20.0  # (1000-800)/1000 * 100 = 20%
    
    # Missing JSON wallet should fail with DATA_FETCH_BLOCKED
    result_missing = wallet_passes_real_dd(missing_wallet, max_real_dd_pct=50.0, cache_dirs=[cache_dir])
    assert result_missing["dd_gate_pass"] is False
    assert "DATA_FETCH_BLOCKED" in result_missing["dd_gate_reason"]


def test_no_closed_pnl_usage():
    """I. no closedPnl DD as risk source"""
    import ast
    
    # Read the source file
    source_file = Path(__file__).parent.parent / "real_dd_filter.py"
    source = source_file.read_text()
    
    # Parse the AST to check for forbidden terms in actual code (not docstrings/comments)
    tree = ast.parse(source)
    
    # Check for forbidden imports
    forbidden_imports = ['numpy', 'pandas']
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if any(forbid in alias.name for forbid in forbidden_imports):
                    assert False, f"Forbidden import found: {alias.name}"
        elif isinstance(node, ast.ImportFrom):
            if node.module and any(forbid in node.module for forbid in forbidden_imports):
                assert False, f"Forbidden import from found: {node.module}"
    
    # Check for forbidden terms in actual code nodes (not docstrings/comments)
    # We only check string constants that are used in expressions, not module docstrings
    forbidden_terms = ['closedPnl', 'cumsum']
    
    # Get the module docstring node and its Constant child to skip both
    module_docstring_nodes = set()
    if tree.body and isinstance(tree.body[0], ast.Expr) and isinstance(tree.body[0].value, ast.Constant) and isinstance(tree.body[0].value.value, str):
        module_docstring_nodes.add(tree.body[0])  # The Expr node
        module_docstring_nodes.add(tree.body[0].value)  # The Constant node inside
    
    for node in ast.walk(tree):
        # Skip module docstring nodes
        if node in module_docstring_nodes:
            continue
        # Check string constants in actual code (not docstrings)
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            for term in forbidden_terms:
                if term in node.value:
                    assert False, f"Forbidden term '{term}' found in code string: {node.value[:100]}"
        # Check attribute access (e.g., obj.closedPnl)
        if isinstance(node, ast.Attribute):
            for term in forbidden_terms:
                if term in node.attr:
                    assert False, f"Forbidden term '{term}' found in attribute access: {node.attr}"
        # Check variable names
        if isinstance(node, ast.Name):
            for term in forbidden_terms:
                if term in node.id:
                    assert False, f"Forbidden term '{term}' found in variable name: {node.id}"


def test_gate_disabled_passes_everything():
    """J. gate disabled passes everything"""
    # When max_real_dd_pct is None, gate should be disabled
    stats = {
        "allTime_max_drawdown_mtm": -5000,  # 500% DD
        "allTime_acctV_peak": 1000,
        "mtm_source": "hl_portfolio_api"
    }
    gate = real_dd_gate(stats, max_real_dd_pct=None)
    assert gate["dd_gate_pass"] is True
    assert gate["real_max_dd_pct"] is None  # gate disabled → no DD computed


def test_zero_dd_passes():
    """K. zero DD passes"""
    stats = {
        "allTime_max_drawdown_mtm": 0,  # 0% DD
        "allTime_acctV_peak": 1000,
        "mtm_source": "hl_portfolio_api"
    }
    gate = real_dd_gate(stats, max_real_dd_pct=50.0)
    assert gate["dd_gate_pass"] is True
    assert gate["real_max_dd_pct"] == 0.0


def test_edge_case_peak_is_zero():
    """L. edge case: peak is zero"""
    stats = {
        "allTime_max_drawdown_mtm": -100,
        "allTime_acctV_peak": 0,  # This would cause division by zero
        "mtm_source": "hl_portfolio_api"
    }
    gate = real_dd_gate(stats, max_real_dd_pct=50.0)
    assert gate["dd_gate_pass"] is False
    assert gate["dd_gate_reason"] == "DATA_FETCH_BLOCKED"  # Should be blocked due to invalid data


# Additional test for parse_max_real_dd_pct with missing data
def test_parse_max_real_dd_pct_missing_data():
    stats = {"mtm_source": "unavailable"}
    dd_pct, dd_source = parse_max_real_dd_pct(stats)
    assert dd_pct is None
    assert dd_source == "DATA_FETCH_BLOCKED"


# Additional test for real_dd_gate with zero peak (division by zero protection)
def test_real_dd_gate_zero_peak():
    stats = {
        "allTime_max_drawdown_mtm": -50,
        "allTime_acctV_peak": 0,
        "mtm_source": "hl_portfolio_api"
    }
    gate = real_dd_gate(stats, max_real_dd_pct=50.0)
    assert gate["dd_gate_pass"] is False
    assert gate["dd_gate_reason"] == "DATA_FETCH_BLOCKED"


if __name__ == "__main__":
    # Simple test runner for manual execution
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))