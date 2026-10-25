# The installer for Windows, in PowerShell:
#
#   irm https://raw.githubusercontent.com/williammgb/universal-data-pipeline/main/deploy/install.ps1 | iex
#
# It puts compose.release.yaml, udp.cmd and udp (the launcher for Git Bash) in UDP_HOME (default
# $HOME\udp), makes sources\ and pipelines\ there, and adds that folder to the user's PATH and to
# this window's, so `udp up` works at once and in every new window. Running it again updates the
# compose file and the launchers; .env, sources\, pipelines\ and backups\ are never touched.
#
# Every file is fetched to a temporary name first and only moved into place once all of them
# arrived, so a failed download leaves the files from before as they were. udp.cmd is written
# with CRLF line endings whatever it arrived with: with LF alone cmd can miss its labels.
#
# The user's PATH is changed in the registry with the kind it already has, so an entry such as
# %USERPROFILE%\bin keeps expanding; SetEnvironmentVariable then tells open programs, Explorer
# among them, that the environment changed, so a window started from it sees the new PATH.
#
# UDP_INSTALL_FROM=<folder> copies the files from that folder instead of downloading them, and
# UDP_SKIP_PATH=1 leaves PATH alone: both are for the tests.
#
# Everything runs in one script block, so `iex` leaves no variables behind in the window and an
# error stops the installer, not the window. Plain ASCII only: Windows PowerShell 5.1 reads a
# download with no charset as Latin-1.

& {
    $ErrorActionPreference = 'Stop'
    $ProgressPreference = 'SilentlyContinue'
    [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12

    $base = 'https://raw.githubusercontent.com/williammgb/universal-data-pipeline/main/deploy'
    $files = 'compose.release.yaml', 'udp.cmd', 'udp'
    $from = $env:UDP_INSTALL_FROM

    $udpHome = if ($env:UDP_HOME) { $env:UDP_HOME } else { Join-Path $HOME 'udp' }
    $udpHome = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($udpHome)
    $null = [IO.Directory]::CreateDirectory($udpHome)

    $temps = @()
    try {
        foreach ($name in $files) {
            $temp = Join-Path $udpHome ".$name.download"
            $temps += $temp
            if ($from) {
                Copy-Item -LiteralPath (Join-Path $from $name) -Destination $temp -Force
            } else {
                Invoke-WebRequest -UseBasicParsing -Uri "$base/$name" -OutFile $temp
            }
            if ($name -eq 'udp.cmd') {
                $text = [IO.File]::ReadAllText($temp)
                [IO.File]::WriteAllText($temp, ($text -replace "`r?`n", "`r`n"))
            }
        }
        foreach ($name in $files) {
            Move-Item -LiteralPath (Join-Path $udpHome ".$name.download") -Destination (Join-Path $udpHome $name) -Force
        }
    } finally {
        foreach ($temp in $temps) {
            if (Test-Path -LiteralPath $temp) { Remove-Item -LiteralPath $temp -Force }
        }
    }
    $null = [IO.Directory]::CreateDirectory((Join-Path $udpHome 'sources'))
    $null = [IO.Directory]::CreateDirectory((Join-Path $udpHome 'pipelines'))

    $isHome = {
        param($entry)
        [Environment]::ExpandEnvironmentVariables($entry).Trim().TrimEnd('\') -eq $udpHome.TrimEnd('\')
    }

    if ($env:UDP_SKIP_PATH -eq '1') {
        $next = "Add $udpHome to PATH, then run: udp up"
    } else {
        $key = [Microsoft.Win32.Registry]::CurrentUser.CreateSubKey('Environment')
        try {
            $old = ''
            $kind = [Microsoft.Win32.RegistryValueKind]::ExpandString
            if ($key.GetValueNames() -contains 'Path') {
                $old = [string]$key.GetValue('Path', '', [Microsoft.Win32.RegistryValueOptions]::DoNotExpandEnvironmentNames)
                $kind = $key.GetValueKind('Path')
            }
            if (-not @($old -split ';' | Where-Object { & $isHome $_ })) {
                $new = if ($old.TrimEnd(';')) { $old.TrimEnd(';') + ';' + $udpHome } else { $udpHome }
                $key.SetValue('Path', $new, $kind)
                [Environment]::SetEnvironmentVariable('UDP_INSTALLING', '1', 'User')
                # [NullString]: PowerShell hands a plain $null to .NET as '', which PowerShell 7
                # keeps as an empty variable instead of deleting it.
                [Environment]::SetEnvironmentVariable('UDP_INSTALLING', [NullString]::Value, 'User')
            }
        } finally {
            $key.Close()
        }
        if (-not @($env:Path -split ';' | Where-Object { & $isHome $_ })) {
            $env:Path = $env:Path.TrimEnd(';') + ';' + $udpHome
        }
        $next = 'Run: udp up'
    }

    Write-Host "Installed udp in $udpHome"
    Write-Host $next
}
