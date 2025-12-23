<#
============================================================
 PMS MEDIA PROCESSING ENGINE (QUARTERLY)
 AUTHORITATIVE + ADAPTIVE + HASH VERIFIED
============================================================
#>

# ------------------------- GLOBALS -------------------------
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# ---------------------- CONFIGURATION ----------------------
$cdDriveLetter          = 'E'
$cdDrive                = "${cdDriveLetter}:\"

$shareRoot              = '\\10.10.80.42\govdb'
$baseNetworkRoot         = '\\10.10.80.42\govdb\PMS\PMS-Carriers'

$logRoot                = 'C:\ProgramData\PMS\Logs'
$stagingRoot            = 'C:\ProgramData\PMS\Staging'

$slowThresholdMB        = 2
$enableHashVerification = $true

$networkRetries         = 5
$retryDelaySec          = 3

# ------------------ ENSURE LOCAL DIRECTORIES ----------------
foreach ($path in @($logRoot, $stagingRoot)) {
    if (-not (Test-Path -LiteralPath $path)) {
        New-Item -ItemType Directory -Path $path -Force | Out-Null
    }
}

$runID   = Get-Date -Format 'yyyyMMdd_HHmmss'
$mainLog = Join-Path $logRoot "PMS_Ingest_$runID.log"

# -------------------------- LOGGING -------------------------
function Write-Log {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Message
    )

    $ts = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    "$ts  $Message" | Tee-Object -FilePath $mainLog -Append
}

# -------------------------- STATUS --------------------------
function Show-Status {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Phase,
        [Parameter(Mandatory)][string]$Detail,
        [int]$Percent = -1
    )

    Write-Progress -Activity 'PMS Media Processing Engine' `
        -Status "$Phase`n$Detail" `
        -PercentComplete $Percent
}

# -------------------- NETWORK SPEED TEST --------------------
function Test-NetworkSpeedMBps {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Path
    )

    $testFile = Join-Path $Path 'pms_speed_test.tmp'
    $data     = New-Object byte[] (5MB)
    (New-Object Random).NextBytes($data)

    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        [System.IO.File]::WriteAllBytes($testFile, $data)
    }
    catch {
        return 0
    }
    finally {
        Remove-Item -LiteralPath $testFile -Force -ErrorAction SilentlyContinue
    }

    $sw.Stop()
    return [math]::Round((5 / $sw.Elapsed.TotalSeconds), 2)
}

# --------------------- HASH VERIFICATION ---------------------
function Compare-FileHashIfNeeded {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Source,
        [Parameter(Mandatory)][string]$Destination
    )

    if (-not (Test-Path -LiteralPath $Destination)) {
        return $false
    }

    $srcHash  = (Get-FileHash -LiteralPath $Source      -Algorithm SHA256).Hash
    $destHash = (Get-FileHash -LiteralPath $Destination -Algorithm SHA256).Hash

    if ($srcHash -ne $destHash) {
        Write-Log "HASH MISMATCH: $Source"
        return $false
    }

    Write-Log "Hash verified: $Source"
    return $true
}

# ============================ START ============================
Write-Log 'PMS ingest started'
Write-Log "Source: $cdDrive"
Write-Log "Destination: $baseNetworkRoot"

# --------------------- NETWORK PREFLIGHT ----------------------
$reachable = $false
for ($i = 1; $i -le $networkRetries; $i++) {
    if (Test-Path -LiteralPath $shareRoot) {
        $reachable = $true
        break
    }

    Write-Log "Network share unreachable (attempt $i of $networkRetries)"
    Start-Sleep -Seconds $retryDelaySec
}

if (-not $reachable) {
    throw "Network share unavailable: $shareRoot"
}

# ---------------- ENSURE DESTINATION DIRECTORY ----------------
if (-not (Test-Path -LiteralPath $baseNetworkRoot)) {
    Write-Log "Destination folder missing - creating: $baseNetworkRoot"
    New-Item -ItemType Directory -Path $baseNetworkRoot -Force | Out-Null
}

# ------------------------- PREFLIGHT --------------------------
Show-Status -Phase 'Preflight' -Detail 'Enumerating media...'
$files = Get-ChildItem -LiteralPath $cdDrive -Recurse -File

if ($files.Count -eq 0) {
    throw 'No readable files found.'
}

Write-Log "Preflight OK - $($files.Count) files detected"

# --------------------- NETWORK DECISION -----------------------
$speed      = Test-NetworkSpeedMBps -Path $baseNetworkRoot
$useStaging = $speed -lt $slowThresholdMB

if ($useStaging) {
    Show-Status -Phase 'Network Check' -Detail "Slow network ($speed MB/s). Using staging."
    Write-Log   "Slow network detected ($speed MB/s) - staging enabled"
}
else {
    Show-Status -Phase 'Network Check' -Detail "Fast network ($speed MB/s). Direct copy."
    Write-Log   "Fast network detected ($speed MB/s) - direct copy"
}

# -------------------------- STAGING ---------------------------
if ($useStaging) {
    $stagingPath = Join-Path $stagingRoot 'STAGING'

    if (Test-Path -LiteralPath $stagingPath) {
        Remove-Item -LiteralPath $stagingPath -Recurse -Force -ErrorAction SilentlyContinue
    }

    New-Item -ItemType Directory -Path $stagingPath -Force | Out-Null

    Write-Log 'Staging media locally'
    robocopy $cdDrive $stagingPath /MIR /R:3 /W:3 /MT:8 /COPY:DAT /DCOPY:T /FFT |
        Tee-Object -FilePath $mainLog -Append

    $copySource = $stagingPath
}
else {
    $copySource = $cdDrive
}

# ------------------- AUTHORITATIVE COPY -----------------------
Write-Log 'Starting authoritative mirror copy'
robocopy $copySource $baseNetworkRoot /MIR /R:3 /W:3 /MT:16 /COPY:DAT /DCOPY:T /FFT |
    Tee-Object -FilePath $mainLog -Append

# -------------------- HASH VERIFICATION -----------------------
if ($enableHashVerification) {
    $checked = 0

    foreach ($f in $files) {
        $relative = $f.FullName.Substring($cdDrive.Length)
        $dest     = Join-Path $baseNetworkRoot $relative

        if (-not (Compare-FileHashIfNeeded -Source $f.FullName -Destination $dest)) {
            Copy-Item -LiteralPath $f.FullName -Destination $dest -Force
        }

        $checked++
        $pct = [int](($checked / $files.Count) * 100)

        Show-Status -Phase 'Verification' -Detail "$checked of $($files.Count) verified" -Percent $pct
    }
}

# -------------------------- COMPLETE --------------------------
Write-Log 'PMS ingest complete'
Show-Status -Phase 'Complete' -Detail 'Processing complete.' -Percent 100

Write-Host "`nPMS MEDIA PROCESSING COMPLETE"
Write-Host "Log file: $mainLog"
