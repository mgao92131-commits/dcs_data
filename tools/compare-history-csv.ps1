param(
    [Parameter(Mandatory=$true)]
    [string]$LegacyCsv,
    [Parameter(Mandatory=$true)]
    [string]$V2Csv
)

function Read-DataLines([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) {
        throw "CSV file not found: $Path"
    }

    $rows = New-Object System.Collections.ArrayList
    foreach ($line in (Get-Content -LiteralPath $Path -Encoding UTF8)) {
        $normalized = $line -replace '^\uFEFF', ''
        if ($normalized.StartsWith('#')) {
            continue
        }
        if ($normalized -eq 'Timestamp,Value,DataType,Flags') {
            continue
        }
        if ($normalized.Length -eq 0) {
            continue
        }
        $rows.Add($line)
    }
    return $rows.ToArray()
}

$legacyRows = @(Read-DataLines $LegacyCsv)
$v2Rows = @(Read-DataLines $V2Csv)

if ($legacyRows.Count -ne $v2Rows.Count) {
    Write-Error ("ROW COUNT MISMATCH: v1={0}, v2={1}" -f $legacyRows.Count, $v2Rows.Count)
    exit 1
}

for ($index = 0; $index -lt $legacyRows.Count; $index++) {
    if ($legacyRows[$index] -cne $v2Rows[$index]) {
        Write-Error ("ROW MISMATCH at data row {0}:`n  v1: {1}`n  v2: {2}" -f ($index + 1), $legacyRows[$index], $v2Rows[$index])
        exit 1
    }
}

Write-Output ("CSV DATA MATCH: {0} rows" -f $legacyRows.Count)
exit 0
