@echo off
rem The release launcher for Windows: the same commands as the sh launcher `udp` beside it.
rem Put it beside compose.release.yaml, and `udp load my_shop` is
rem `docker compose -f <this folder>\compose.release.yaml run --rm app load my_shop`, from any
rem folder. The first command (any but help) makes sources, pipelines and backups here and
rem writes .env with POSTGRES_PASSWORD and UDP_API_KEYS: the set values when there are any, else
rem random ones, each in single quotes. It is never rewritten. With UDP_DRY_RUN=1 every docker call is printed instead
rem of made. PowerShell is used for two things only: a random key and the backup's timestamp.
setlocal EnableExtensions DisableDelayedExpansion

set "here=%~dp0"
set "here=%here:~0,-1%"
set "file=%here%\compose.release.yaml"
if not "%file: =%"=="%file%" set file="%file%"
set "dc=docker compose -f %file%"
set "dry="
if "%UDP_DRY_RUN%"=="1" set "dry=1"

set "command=%~1"
if "%command%"=="" goto :help
if /i "%command%"=="help" goto :help

rem Every argument after the command, each as typed, quotes kept: %* minus its first word.
set "rest="
:rest
shift
if "%~1"=="" goto :rest_done
set rest=%rest% %1
goto :rest
:rest_done

if defined dry goto :setup
where docker >nul 2>&1
if not errorlevel 1 goto :setup
1>&2 echo Docker is not installed or not on PATH: see https://docs.docker.com/get-docker/
exit /b 1

:setup
if exist "%here%\.env" goto :dispatch
if not exist "%here%\sources" mkdir "%here%\sources"
if not exist "%here%\pipelines" mkdir "%here%\pipelines"
if not exist "%here%\backups" mkdir "%here%\backups"
rem The values are only ever expanded with !, after the line is parsed, so any of ^ & > | ) " %
rem in them stays text. Each is written in single quotes, which Compose reads as it is.
setlocal EnableDelayedExpansion
set "password=!POSTGRES_PASSWORD!"
set "key=!UDP_API_KEYS!"
if defined password goto :have_password
call :random password
if defined dry goto :have_password
docker volume inspect udp_pgdata >nul 2>&1
if errorlevel 1 goto :have_password
echo A database from before exists. If it was made with another password, put that password in !here!\.env as POSTGRES_PASSWORD before udp up.
:have_password
if not defined key call :random key
rem Single quotes cannot hold a ', nor a final \, which Compose takes for an escaped quote.
set "name=POSTGRES_PASSWORD"
if not "!password:'=!"=="!password!" goto :unquotable
if "!password:~-1!"=="\" goto :unquotable
set "name=UDP_API_KEYS"
if not "!key:'=!"=="!key!" goto :unquotable
if "!key:~-1!"=="\" goto :unquotable
> "!here!\.env" echo POSTGRES_PASSWORD='!password!'
>> "!here!\.env" echo UDP_API_KEYS='!key!'
echo Saved a new password and API key in !here!\.env. Your API key: !key!
endlocal

:dispatch
if /i "%command%"=="up" goto :up
if /i "%command%"=="run" goto :up
if /i "%command%"=="down" goto :down
if /i "%command%"=="status" goto :status
if /i "%command%"=="logs" goto :logs
if /i "%command%"=="open" goto :open
if /i "%command%"=="restart" goto :restart
if /i "%command%"=="upgrade" goto :upgrade
if /i "%command%"=="backup" goto :backup
if /i "%command%"=="new" goto :new

rem Anything else is a command of the app: load, pipeline, profile, doctor.
if defined dry goto :app_dry
%dc% run --rm app %*
exit /b
:app_dry
echo(%dc% run --rm app %*
exit /b 0

:up
if defined rest goto :refuse
if defined dry goto :up_call
docker info >nul 2>&1
if not errorlevel 1 goto :up_call
1>&2 echo Docker is not running: start Docker Desktop (or the Docker service) and wait until it says the engine is running
exit /b 1
:up_call
call :do %dc% up -d --wait
if errorlevel 1 exit /b
if defined dry exit /b 0
call :port
echo Dashboard: http://127.0.0.1:%port% (API key in %here%\.env)
exit /b 0

:down
rem `down -v` would delete the database, so down takes nothing at all.
if defined rest goto :refuse
call :do %dc% down
exit /b

:status
call :do %dc% ps%rest%
exit /b

:logs
call :do %dc% logs --tail 100%rest%
exit /b

:open
if defined rest goto :refuse
call :port
call :do start "" http://127.0.0.1:%port%
exit /b

:restart
if defined rest goto :refuse
call :do %dc% restart api scheduler
exit /b

:upgrade
if defined rest goto :refuse
call :do %dc% pull
if errorlevel 1 exit /b
call :do %dc% up -d --wait
exit /b

:backup
if defined rest goto :refuse
for /f "delims=" %%s in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd-HHmmss"') do set "stamp=%%s"
set "dump=%here%\backups\udp-%stamp%.dump"
set "shown=%dump%"
if not "%dump: =%"=="%dump%" set shown="%dump%"
if not defined dry goto :backup_run
echo(%dc% exec -T postgres pg_dump -U udp -Fc udp ^> %shown%
exit /b 0
:backup_run
if not exist "%here%\backups" mkdir "%here%\backups"
rem The dump is named only once it is whole, so a failed one never looks like a backup.
%dc% exec -T postgres pg_dump -U udp -Fc udp > "%dump%.part"
if errorlevel 1 goto :backup_failed
move /y "%dump%.part" "%dump%" >nul
echo Saved %dump%
exit /b 0
:backup_failed
del "%dump%.part" 2>nul
exit /b 1

:new
call :do %dc% run --rm --no-deps scaffold new%rest%
exit /b

:refuse
1>&2 echo usage: udp %command%
exit /b 2

:unquotable
1>&2 echo !name! holds a ' or a final \, which .env cannot keep as it is: set another, or write !here!\.env yourself
exit /b 2

:help
echo usage: udp ^<command^>
echo(
echo   udp up (or udp run)    start the platform and print its address
echo   udp down               stop it, keeping the data
echo   udp status             show the containers
echo   udp logs [service]     the last 100 log lines (add -f to follow them)
echo   udp open               open the dashboard in the browser
echo   udp restart            restart the api and the scheduler (after adding a schedule)
echo   udp upgrade            pull the newest images and start on them
echo   udp backup             save the database to backups\udp-^<date^>-^<time^>.dump
echo   udp new ^<source^>       write sources\^<source^>\source.yaml from that folder's files
echo   udp help               this list
echo   udp ^<command^> ...      anything else runs in the app: load, pipeline, profile, doctor
echo(
echo The first command writes .env here with a new password and API key. upgrade updates the
echo images only: a new launcher or compose file comes from running the installer again.
exit /b 0

rem Prints the call in a dry run, makes it otherwise.
:do
if not defined dry goto :do_run
echo(%*
exit /b 0
:do_run
%*
exit /b

rem 32 random hex characters into the variable named by the argument.
:random
for /f "delims=" %%k in ('powershell -NoProfile -Command "[guid]::NewGuid().ToString('N')"') do set "%~1=%%k"
exit /b 0

rem UDP_API_PORT from .env, else 8000.
:port
set "port=8000"
for /f "tokens=1,* delims==" %%a in ('findstr /b "UDP_API_PORT=" "%here%\.env" 2^>nul') do set "port=%%b"
exit /b 0
