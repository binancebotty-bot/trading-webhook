"""HL per-coin max-leverage approximation (used for cross-margin calc)."""
LEV_DEFAULT = 10
LEV_MAP = {
    "BTC": 40, "ETH": 25,
    "SOL": 20, "XRP": 20, "BNB": 10, "AVAX": 20, "MATIC": 20, "LTC": 20,
    "ARB": 20, "OP": 20, "APT": 20, "ATOM": 20, "DOGE": 20, "LINK": 20,
    "DOT": 20, "TRX": 20, "BCH": 20, "FIL": 20, "NEAR": 20, "INJ": 20,
    "SUI": 20, "TIA": 20, "JUP": 20, "AAVE": 20, "ADA": 20, "TON": 20,
    "HYPE": 10, "PENDLE": 10, "FARTCOIN": 5, "KPEPE": 5, "WIF": 5,
}
def lev(coin: str) -> int:
    return LEV_MAP.get((coin or "").upper(), LEV_DEFAULT)
