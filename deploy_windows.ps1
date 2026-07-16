#requires -Version 5
<#
  deploy_windows.ps1 — one-shot Windows deploy for the Everpure TCO tool using
  Podman, a free, open-source container engine (no Docker Desktop license needed).

  It checks EVERY prerequisite and helps install anything missing (hardware
  virtualization, WSL2, winget, Podman, a compose provider), starts the Podman
  machine, builds + runs the container, waits until it's healthy, and opens it.

  Usage (PowerShell):
    .\deploy_windows.ps1            # interactive — asks before installing anything
    .\deploy_windows.ps1 -Yes       # non-interactive — auto-install missing prereqs
    .\deploy_windows.ps1 -Down      # stop the app (keeps its data volume)
    .\deploy_windows.ps1 -Destroy   # stop the app AND delete its data volume
    .\deploy_windows.ps1 -Logs      # follow the container logs

  Requires WSL2 + hardware virtualization (Podman runs a small Linux VM). If your
  machine can't virtualize, run the app natively instead (see DEPLOYMENT.md §6).
#>
[CmdletBinding()]
param([switch]$Yes, [switch]$Down, [switch]$Destroy, [switch]$Logs)
$ErrorActionPreference = 'Stop'

$PORT   = 5000
$URL    = "http://localhost:$PORT"
$HEALTH = "$URL/api/auth/status"
$APP    = 'Everpure TCO'

function Step($m){ Write-Host "`n==> $m" -ForegroundColor Cyan }
function Ok($m){   Write-Host "  [ok] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "  [!]  $m" -ForegroundColor Yellow }
function Fail($m){ Write-Host "  [x]  $m" -ForegroundColor Red; exit 1 }
function Have($c){ [bool](Get-Command $c -ErrorAction SilentlyContinue) }
function Ask($q){ if($Yes){return $true}; $a=Read-Host "  $q [Y/n]"; return ($a -eq '' -or $a -match '^(y|yes)$') }
function Refresh-Path { $env:Path = [Environment]::GetEnvironmentVariable('Path','Machine') + ';' + [Environment]::GetEnvironmentVariable('Path','User') }

Set-Location -Path $PSScriptRoot

# ── compose provider (podman compose, else podman-compose, else fetch docker-compose) ──
$script:CC = $null
function Resolve-Compose {
  podman compose version 2>&1 | Out-Null
  if ($LASTEXITCODE -eq 0) { $script:CC = @('podman','compose'); return }
  if (Have 'podman-compose') { $script:CC = @('podman-compose'); return }
  Warn 'no compose provider; downloading docker-compose (v2) binary...'
  $dir = Join-Path $env:LOCALAPPDATA 'Podman\compose'
  New-Item -ItemType Directory -Force -Path $dir | Out-Null
  $dst = Join-Path $dir 'docker-compose.exe'
  if (-not (Test-Path $dst)) {
    Invoke-WebRequest -UseBasicParsing -Uri 'https://github.com/docker/compose/releases/latest/download/docker-compose-windows-x86_64.exe' -OutFile $dst
  }
  $env:Path = "$dir;$env:Path"
  podman compose version 2>&1 | Out-Null
  if ($LASTEXITCODE -eq 0) { $script:CC = @('podman','compose') }
  else { Fail 'no compose provider. Install Podman Desktop (bundles compose) or run: pip install podman-compose' }
}
function Compose {
  $exe = $script:CC[0]
  $pre = if ($script:CC.Count -gt 1) { $script:CC[1..($script:CC.Count-1)] } else { @() }
  & $exe @($pre + $args)
}

# ── management shortcuts ────────────────────────────────────────────────────────
if ($Down -or $Destroy -or $Logs) {
  Resolve-Compose
  if ($Logs)    { Compose logs -f; exit 0 }
  if ($Down)    { Step "Stopping $APP"; Compose down; Ok 'stopped (data kept)'; exit 0 }
  if ($Destroy) { Step "Stopping $APP and deleting its data"; Compose down -v; Ok 'stopped, data volume removed'; exit 0 }
}

# ── 1. platform ─────────────────────────────────────────────────────────────────
Step 'Platform'
if ($PSVersionTable.PSEdition -eq 'Core' -and -not $IsWindows) { Fail 'This script is for Windows.' }
Ok "Windows ($([Environment]::OSVersion.Version))"

# ── 2. hardware virtualization ──────────────────────────────────────────────────
Step 'Hardware virtualization'
$virt = $true
try { $virt = [bool](Get-CimInstance -ClassName Win32_Processor).VirtualizationFirmwareEnabled } catch {}
if ($virt) { Ok 'enabled in firmware' } else {
  Warn 'virtualization appears DISABLED in BIOS/UEFI. Podman (WSL2) needs it.'
  Write-Host '     Reboot -> firmware setup -> enable Intel VT-x / AMD-V (SVM / Virtualization Technology), save, reboot.'
  if (-not (Ask 'Continue anyway (Podman may fail to start)?')) { exit 1 }
}

# ── 3. WSL2 ─────────────────────────────────────────────────────────────────────
Step 'WSL2'
$wslOk = $false
try { wsl.exe --status 2>&1 | Out-Null; $wslOk = ($LASTEXITCODE -eq 0) } catch {}
if ($wslOk) { Ok 'WSL is installed' } else {
  Warn 'WSL2 is not installed (Podman uses it as the Linux backend).'
  $admin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
  if ($admin -and (Ask 'Install WSL2 now (wsl --install)? A reboot will be required.')) {
    wsl.exe --install
    Fail 'WSL2 installed. REBOOT, then re-run this script.'
  } else {
    Fail 'Open an ADMIN PowerShell and run:  wsl --install  then reboot and re-run this script.'
  }
}

# ── 4. winget ───────────────────────────────────────────────────────────────────
Step 'winget (App Installer)'
if (Have 'winget') { Ok 'present' } else { Fail 'winget not found. Install "App Installer" from the Microsoft Store, then re-run.' }

# ── 5. Podman ───────────────────────────────────────────────────────────────────
Step 'Podman'
if (-not (Have 'podman')) {
  Warn 'Podman is not installed.'
  if (Ask 'Install Podman now (winget install RedHat.Podman)?') {
    winget install -e --id RedHat.Podman --accept-package-agreements --accept-source-agreements
    Refresh-Path
  } else { Fail 'Podman is required.' }
}
if (-not (Have 'podman')) { Fail 'Podman still not on PATH. Open a new PowerShell window and re-run.' }
Ok "podman $((podman --version) -replace 'podman version ','')"

# ── 6. Podman machine (the Linux VM) ────────────────────────────────────────────
Step 'Podman machine'
$machines = @(podman machine list --format '{{.Name}}' 2>$null)
if (-not $machines -or $machines.Count -eq 0) {
  Write-Host '  initializing a Podman machine (first run downloads an image)...'
  podman machine init
}
$startOut = (podman machine start 2>&1)
if ($LASTEXITCODE -ne 0 -and "$startOut" -notmatch 'already running') { Warn "$startOut" }
Ok 'Podman machine running'

# ── 7. engine reachable ─────────────────────────────────────────────────────────
Step 'Container engine'
podman info 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { Fail 'podman engine not reachable. Try: podman machine start' }
Ok 'reachable'

# ── 8. compose provider + repo sanity ───────────────────────────────────────────
Step 'Compose provider'
Resolve-Compose
Ok ("using: " + ($script:CC -join ' '))
if (-not (Test-Path 'docker-compose.yml')) { Fail 'docker-compose.yml not found - run this from the repo root.' }

# ── 9. build + run ───────────────────────────────────────────────────────────────
Step "Building and starting $APP (first build takes a few minutes)"
Compose up -d --build
if ($LASTEXITCODE -ne 0) { Fail 'build/run failed - see the output above.' }
Ok 'container is up'

# ── 10. wait for health ──────────────────────────────────────────────────────────
Step "Waiting for $URL to respond"
$up = $false
for ($i=0; $i -lt 60; $i++) {
  try { if ((Invoke-WebRequest -UseBasicParsing -Uri $HEALTH -TimeoutSec 3).StatusCode -eq 200) { $up=$true; break } } catch {}
  Start-Sleep -Seconds 2
}
if ($up) { Ok "serving on $URL" } else { Warn 'app has not responded yet - recent logs:'; Compose logs --tail 40 }

# ── 11. open + summary ────────────────────────────────────────────────────────────
Start-Process $URL
Write-Host ''
Write-Host "$APP is running -> $URL" -ForegroundColor Cyan
Write-Host '  Default logins: admin / password123  (or demo / demo) - change before real use.'
Warn 'docker-compose.yml ships a placeholder FLASK_SECRET_KEY; set a real one for production.'
Write-Host ''
Write-Host '  Manage it:'
Write-Host '    .\deploy_windows.ps1 -Logs      # follow logs'
Write-Host '    .\deploy_windows.ps1 -Down      # stop (keeps data)'
Write-Host '    .\deploy_windows.ps1 -Destroy   # stop + delete the data volume'
Write-Host '    podman machine stop             # stop the whole Podman VM'
