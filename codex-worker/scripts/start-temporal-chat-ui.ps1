param(
    [Parameter(Mandatory = $true)][string]$UiSource,
    [Parameter(Mandatory = $true)][string]$TemporalCli,
    [Parameter(Mandatory = $true)][string]$Database,
    [Parameter(Mandatory = $true)][string]$BuildRoot,
    [int]$ApiPort = 7233,
    [int]$UiPort = 18000,
    [int]$MetricsPort = 3208
)

$ErrorActionPreference = 'Stop'
$UiSource = (Resolve-Path -LiteralPath $UiSource).Path
$TemporalCli = (Resolve-Path -LiteralPath $TemporalCli).Path
$Database = [IO.Path]::GetFullPath($Database)
$BuildRoot = [IO.Path]::GetFullPath($BuildRoot)
New-Item -ItemType Directory -Force -Path $BuildRoot | Out-Null
$statePath = Join-Path $BuildRoot 'server.json'
$version = Get-Date -Format 'yyyyMMdd-HHmmss-fff'
$assetPath = Join-Path $BuildRoot $version
$previousBuildPath = $env:BUILD_PATH
try {
    $env:BUILD_PATH = $assetPath
    Push-Location $UiSource
    try {
        & pnpm build:server
        if ($LASTEXITCODE -ne 0) { throw 'UI build failed; the serving process was not changed.' }
    } finally { Pop-Location }
} finally { $env:BUILD_PATH = $previousBuildPath }

$htmlPath = Join-Path $assetPath 'index.html'
$html = Get-Content -LiteralPath $htmlPath -Raw
$references = [regex]::Matches($html, '(?:src|href)="([^"?#]+\.(?:js|css))"|import\("([^"?#]+\.js)"\)')
$assets = @($references | ForEach-Object {
    if ($_.Groups[1].Success) { $_.Groups[1].Value } else { $_.Groups[2].Value }
} | Sort-Object -Unique)
if ($assets.Count -eq 0) { throw 'The staged HTML has no startup JS/CSS assets.' }
foreach ($asset in $assets) {
    if ($asset -match '^(https?:)?//') { throw "Unexpected remote startup asset: $asset" }
    $localAsset = [IO.Path]::GetFullPath((Join-Path $assetPath $asset.TrimStart('/')))
    if (-not $localAsset.StartsWith($assetPath + [IO.Path]::DirectorySeparatorChar)) { throw "Asset outside staged build: $asset" }
    if (-not (Test-Path -LiteralPath $localAsset -PathType Leaf)) { throw "Missing staged asset: $asset" }
}

if (Test-Path -LiteralPath $statePath) {
    $state = Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json
    $ownedProcess = Get-CimInstance Win32_Process -Filter "ProcessId = $($state.pid)"
    if ($ownedProcess) {
        if ($ownedProcess.ExecutablePath -ne $TemporalCli -or -not $ownedProcess.CommandLine.Contains($Database)) {
            throw 'Recorded PID no longer identifies this Temporal server; refusing to stop it.'
        }
        Stop-Process -Id $state.pid
        Wait-Process -Id $state.pid -Timeout 30 -ErrorAction SilentlyContinue
    }
}
foreach ($port in @($ApiPort, $UiPort)) {
    if (Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue) {
        throw "Port $port is occupied by a process not owned by this script."
    }
}
$arguments = @('server', 'start-dev', '--db-filename', "`"$Database`"", '--ip', '127.0.0.1', '--port', $ApiPort, '--ui-port', $UiPort, '--metrics-port', $MetricsPort, '--ui-asset-path', "`"$assetPath`"", '--ui-disable-news-fetch')
$server = Start-Process -FilePath $TemporalCli -ArgumentList $arguments -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $BuildRoot "$version.stdout.log") -RedirectStandardError (Join-Path $BuildRoot "$version.stderr.log")
@{ pid = $server.Id; database = $Database; assets = $assetPath; apiPort = $ApiPort; uiPort = $UiPort } | ConvertTo-Json | Set-Content -LiteralPath $statePath -Encoding utf8NoBOM
$origin = "http://127.0.0.1:$UiPort"
$deadline = (Get-Date).AddSeconds(60)
$response = $null
while ((Get-Date) -lt $deadline) {
    if ($server.HasExited) { throw "Temporal server exited: $(Get-Content (Join-Path $BuildRoot "$version.stderr.log") -Raw)" }
    try { $response = Invoke-WebRequest "$origin/"; break } catch { Start-Sleep -Milliseconds 250 }
}
if (-not $response) { throw 'Temporal UI did not become ready in 60 seconds.' }
if ($response.Content -ne $html) { throw 'Served HTML differs from the staged manifest.' }
foreach ($asset in $assets) {
    $assetUrl = [Uri]::new([Uri]"$origin/", $asset)
    $assetResponse = Invoke-WebRequest $assetUrl
    if ($assetResponse.StatusCode -ne 200 -or $assetResponse.Headers['Content-Type'] -match 'text/html') {
        throw "Startup asset did not return JS/CSS: $asset"
    }
}
Get-Content -LiteralPath $statePath
