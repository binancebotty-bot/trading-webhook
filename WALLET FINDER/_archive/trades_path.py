"""
trades_path.py — Single source of truth for the trades CSV path.

Prefers all_trades_clean.csv if it exists, falls back to all_trades.csv.
"""
import os

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

CLEAN = os.path.join(DATA_DIR, "all_trades_clean.csv")
RAW = os.path.join(DATA_DIR, "all_trades.csv")


def trades_csv():
    """Return path to the cleanest available trades file."""
    if os.path.exists(CLEAN):
        return CLEAN
    return RAW
