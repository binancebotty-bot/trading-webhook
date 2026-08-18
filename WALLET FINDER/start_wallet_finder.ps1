<#
start_wallet_finder.ps1 - boots the full WALLET FINDER stack in one go.

Starts (skipping anything already running):
  1. universe_builder.py      feeder -> data/wallet_universe.csv, data/equity_curves/            (for 8012)
  2. HL_Copy_Engine_SSOT.py   feeder -> hl_copy_output/engine_truth.json, raw_live_fills.csv     (for 8014)
  3. Wallet Talent Scout      :8012  -> uvicorn wallet_talent_scout_8012:app
  4. Wallet Proof Engine      :8014  -> uvicorn wallet_proof_engine_8014:app

Notes:
  - 8012 holds a global mutex (Global\WalletFinderAppSingleton): only ONE instance
    can exist machine-wide. A second copy exits immediately with a [singleton] message.
  - 8012 takes ~50s to bind and ~40s for its first page render (heavy import-time
    data load). After that it serves in well under a second.
  - Keep this file ASCII-only: Windows PowerShell 5.1 reads BOM-less files as ANSI,
    and a mojibake'd em dash becomes U+201D, which the parser treats as a quote.
#>

$ErrorActionPreference = 'Stop'
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

# --- Hyperliquid IP weight budget -------------------------------------------
# Hyperliquid meters /info per source IP, not per process. Three independent
# products read it from this machine: the Build-4 copy runtime, the SSOT
# tracker and the :8014 proof engine. They stay independent in every other
# respect - separate state, cursors, ownership, journals, business logic and
# cached exchange truth - and share only this budget.
#
# Two brakes, deliberately:
#   1. Each product carries a FIXED ceiling that binds on its own with no
#      coordination at all (HL_SSOT_WEIGHT_PER_MIN, HL_8014_WEIGHT_PER_MIN).
#   2. The coordinator below lets participating products see each other's
#      spend in one rolling 60s window.
#
# The limit here is the SUM OF THE PARTICIPATING SLICES, not the IP allowance.
# Build-4 runs from its own frozen release and does not participate yet, so its
# share must not be lendable to these two. Raise this to 1050 only when Build-4
# is deployed with the coordinator enabled.
if (-not $env:HL_IP_BUDGET_PATH) {
    $env:HL_IP_BUDGET_PATH = 'C:\Users\Public\HyperliquidProduction\ip_weight_budget.bin'
}
if (-not $env:HL_IP_BUDGET_WEIGHT_PER_MIN) {
    $env:HL_IP_BUDGET_WEIGHT_PER_MIN = '250'
}
Write-Host "IP budget: $env:HL_IP_BUDGET_PATH limit=$env:HL_IP_BUDGET_WEIGHT_PER_MIN wu/min" -ForegroundColor DarkGray

function Test-PortUp([int]$Port) {
    [bool](Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue)
}

function Test-ScriptRunning([string]$Pattern) {
    [bool](Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue |
           Where-Object { $_.CommandLine -match $Pattern })
}

$LogDir = Join-Path $Root 'logs'
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }

function Get-LogPaths([string]$Name) {
    # Keep one generation of history: a crash loop otherwise overwrites the
    # evidence of why the previous run died.
    $out = Join-Path $LogDir "$Name.log"
    $err = Join-Path $LogDir "$Name.err"
    foreach ($f in @($out, $err)) {
        if (Test-Path $f) { Move-Item $f "$f.prev" -Force }
    }
    return @($out, $err)
}

function Start-Feeder([string]$Script, [string]$Title, [string]$LogName) {
    if (Test-ScriptRunning ([regex]::Escape($Script))) {
        Write-Host "  [skip]  $Title already running" -ForegroundColor DarkGray
        return
    }
    $paths = Get-LogPaths $LogName
    Start-Process -FilePath 'python' -ArgumentList $Script `
                  -WorkingDirectory $Root -WindowStyle Minimized `
                  -RedirectStandardOutput $paths[0] -RedirectStandardError $paths[1]
    Write-Host "  [start] $Title  -> logs\$LogName.log" -ForegroundColor Green
}

function Start-Dash([string]$Module, [int]$Port, [string]$Title) {
    if (Test-PortUp $Port) {
        Write-Host "  [skip]  $Title already listening on $Port" -ForegroundColor DarkGray
        return
    }
    $paths = Get-LogPaths $Module
    Start-Process -FilePath 'python' `
        -ArgumentList '-m','uvicorn',"${Module}:app",'--host','127.0.0.1','--port',"$Port",'--log-level','warning' `
        -WorkingDirectory $Root -WindowStyle Minimized `
        -RedirectStandardOutput $paths[0] -RedirectStandardError $paths[1]
    Write-Host "  [start] $Title on $Port  -> logs\$Module.log" -ForegroundColor Green
}

Write-Host ""
Write-Host "=== WALLET FINDER - full stack ===" -ForegroundColor Cyan
Write-Host ""

Write-Host "Feeders:"
Start-Feeder 'universe_builder.py'    'universe_builder (8012 feeder)'    'universe_builder'
Start-Feeder 'HL_Copy_Engine_SSOT.py' 'HL_Copy_Engine_SSOT (8014 feeder)' 'hl_copy_engine_ssot'

Write-Host ""
Write-Host "Dashboards:"
Start-Dash 'wallet_talent_scout_8012' 8012 'Wallet Talent Scout'
Start-Dash 'wallet_proof_engine_8014' 8014 'Wallet Proof Engine'

Write-Host ""
Write-Host "Waiting for ports to bind (8012 cold start is ~50s)..." -ForegroundColor Yellow
$deadline = (Get-Date).AddSeconds(180)
while ((Get-Date) -lt $deadline) {
    if ((Test-PortUp 8012) -and (Test-PortUp 8014)) { break }
    Start-Sleep -Seconds 5
}

Write-Host ""
foreach ($p in 8012, 8014) {
    if (Test-PortUp $p) { Write-Host ("  OK    {0}  http://localhost:{0}/" -f $p) -ForegroundColor Green }
    else                { Write-Host ("  FAIL  {0}  did not bind" -f $p) -ForegroundColor Red }
}

Write-Host ""
Write-Host "  Wallet Talent Scout:  http://localhost:8012/" -ForegroundColor Cyan
Write-Host "  Wallet Proof Engine:  http://localhost:8014/" -ForegroundColor Cyan
Write-Host ""
Write-Host "First 8012 page load takes ~40s (cold cache), then it is instant." -ForegroundColor DarkGray
Write-Host ""
