param(
    [Parameter(Mandatory=$true)][string] $Python,
    [Parameter(Mandatory=$true)][string] $MemoryRoot,
    [Parameter(Mandatory=$true)][string] $PlatformRoot,
    [Parameter(Mandatory=$true)][string] $GatewayRoot,
    [Parameter(Mandatory=$true)][string] $CoordinationRoot,
    [Parameter(Mandatory=$true)][string] $IntegrationRoot,
    [string] $ExtraSitePackages = ''
)
$ErrorActionPreference = 'Stop'
$taskCompanionRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$env:TS050_RUNTIME = Join-Path $taskCompanionRoot '.runtime/memory-learning-joint'
New-Item -ItemType Directory -Force -Path $env:TS050_RUNTIME | Out-Null
$env:TS050_SNAPSHOT_ROOT = Join-Path $env:TS050_RUNTIME 'sources'
# The fixture only needs a subprocess cwd here. Actual Gateway imports come from GatewayRoot.
New-Item -ItemType Directory -Force -Path (Join-Path $env:TS050_SNAPSHOT_ROOT 'model-gateway') | Out-Null
$env:TS050_CONTRACTS = Join-Path $CoordinationRoot 'contracts/text-dialogue/v1'
$env:TS012_CONTRACT_DIR = $env:TS050_CONTRACTS
$env:TIANSHU_CONTRACTS = $env:TS050_CONTRACTS
$env:TIANSHU_CORE_REPO = $taskCompanionRoot
$env:PYTHONDONTWRITEBYTECODE = '1'
$taskPaths = @(
    (Join-Path $taskCompanionRoot 'src'), $PSScriptRoot,
    (Join-Path $MemoryRoot 'src'), $PlatformRoot,
    (Join-Path $GatewayRoot 'src'), (Join-Path $IntegrationRoot 'tests/integration')
)
if ($ExtraSitePackages) { $taskPaths += $ExtraSitePackages }
$env:PYTHONPATH = $taskPaths -join ';'
& $Python -B -m unittest test_memory_learning_joint.MemoryLearningJoint.test_chat_extract_persist_restart_and_recall_without_trigger_words -v
exit $LASTEXITCODE
