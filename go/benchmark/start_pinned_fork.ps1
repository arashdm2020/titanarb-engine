param([int]$Port = 8555)

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$line = Get-Content -LiteralPath (Join-Path $root '.env') | Where-Object { $_ -match '^ARBITRUM_RPC_URL=' } | Select-Object -First 1
if (-not $line) { throw 'ARBITRUM_RPC_URL is not configured locally.' }
$forkUrl = $line.Substring($line.IndexOf('=') + 1).Trim('"')
$anvil = 'D:\forge\anvil.exe'
$process = Start-Process -FilePath $anvil -ArgumentList @('--port', "$Port", '--fork-url', $forkUrl, '--fork-block-number', '496890646', '--chain-id', '42161') -WindowStyle Hidden -PassThru
Start-Sleep -Seconds 4
Write-Output $process.Id
