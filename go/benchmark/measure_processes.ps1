$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$envLine = Get-Content -LiteralPath (Join-Path $root '.env') | Where-Object { $_ -match '^ARBITRUM_RPC_URL=' } | Select-Object -First 1
if (-not $envLine) { throw 'Missing local Arbitrum RPC configuration.' }
$forkUrl = $envLine.Substring($envLine.IndexOf('=') + 1).Trim('"')
$block = 496890646

function Start-Fork {
    $fork = Start-Process -FilePath 'D:\forge\anvil.exe' -ArgumentList @('--port','8555','--fork-url',$forkUrl,'--fork-block-number',"$block",'--chain-id','42161') -WindowStyle Hidden -PassThru
    Start-Sleep -Seconds 4
    return $fork
}
function Stop-Fork($fork) { if (-not $fork.HasExited) { Stop-Process -Id $fork.Id -Force } }
function Measure-Child($file, $arguments, $workingDirectory) {
    $start = @{ FilePath = $file; WorkingDirectory = $workingDirectory; RedirectStandardOutput = (Join-Path $workingDirectory 'runtime\benchmark_child.json'); RedirectStandardError = (Join-Path $workingDirectory 'runtime\benchmark_child.err'); WindowStyle = 'Hidden'; PassThru = $true }
    if ($arguments.Count -gt 0) { $start.ArgumentList = $arguments }
    $process = Start-Process @start
    $watch = [Diagnostics.Stopwatch]::StartNew(); $peak = 0
    while (-not $process.HasExited) { $process.Refresh(); if ($process.WorkingSet64 -gt $peak) { $peak = $process.WorkingSet64 }; Start-Sleep -Milliseconds 10 }
    $watch.Stop(); $process.Refresh()
    [PSCustomObject]@{ wall_clock_s = $watch.Elapsed.TotalSeconds; cpu_process_s = $process.CPU; peak_rss_bytes = $peak; output = (Get-Content -Raw -LiteralPath (Join-Path $workingDirectory 'runtime\benchmark_child.json')).Trim() }
}

$env:ARBITRUM_RPC_URL = 'http://127.0.0.1:8555'; $env:ARBITRUM_WSS_RPC_URL = 'ws://127.0.0.1:8555'; $env:DRY_RUN = 'true'; $env:EXECUTION_MODE = 'dry_run'; $env:PYTHONPATH = $root
$pythonFork = Start-Fork
try { $python = Measure-Child (Join-Path $root '.venv\Scripts\python.exe') @((Join-Path $root 'go\benchmark\python_pipeline_benchmark.py')) $root } finally { Stop-Fork $pythonFork }

$env:BENCH_RPC_URL = 'http://127.0.0.1:8555'; $env:BENCH_CONFIG = (Join-Path $root 'config\arbitrum.json')
$goFork = Start-Fork
try { $go = Measure-Child (Join-Path $root 'go\runtime\go_phase2_benchmark.exe') @() (Join-Path $root 'go') } finally { Stop-Fork $goFork }

[PSCustomObject]@{block=$block;python=$python;go=$go} | ConvertTo-Json -Depth 5
