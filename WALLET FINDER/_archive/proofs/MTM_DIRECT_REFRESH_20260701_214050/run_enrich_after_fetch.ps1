$ErrorActionPreference='Continue'
Set-Location -LiteralPath 'C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER'
$env:PYTHONUNBUFFERED='1'
Wait-Process -Id 48308
python enrich_summary_mtm.py *> 'C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\proofs\MTM_DIRECT_REFRESH_20260701_214050\02_enrich_summary_mtm_after_fetch.log'
