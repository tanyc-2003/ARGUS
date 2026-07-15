@echo off
setlocal EnableExtensions EnableDelayedExpansion
REM ===========================================================================
REM  setup_argus.bat  -  one-shot ARGUS environment bootstrap (Windows)
REM
REM  What it does:
REM    1. Picks a DATA ROOT (where argus.duckdb + parquet + logs + the venv live).
REM       You choose the drive/folder - e.g. D:\argus-data instead of C:\argus-data.
REM    2. Writes that location into .env as ARGUS_DATA_ROOT (only that line is
REM       touched; your API keys and everything else are left as-is).
REM    3. Creates a Python venv under <data root>\venv and pip-installs the repo
REM       with its [dev] extras (pytest/ruff/mypy — the verify loop runs from there).
REM    4. Runs `argus init-db` to create the schema, then `argus check`.
REM
REM  Assumes .env already exists and is populated correctly (API keys etc.).
REM
REM  Usage:
REM    scripts\setup_argus.bat                 (prompts for the location)
REM    scripts\setup_argus.bat D:\argus-data   (non-interactive)
REM ===========================================================================

REM --- resolve repo root (this script lives in <repo>\scripts) ----------------
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "REPO_ROOT=%%~fI"

echo.
echo === ARGUS setup ===========================================================
echo Repo root: %REPO_ROOT%
echo.

REM --- sanity: is this actually the repo? -------------------------------------
if not exist "%REPO_ROOT%\pyproject.toml" (
    echo [ERROR] pyproject.toml not found under "%REPO_ROOT%".
    echo         Run this script from inside the ARGUS repo.
    goto :fail
)
if not exist "%REPO_ROOT%\.env" (
    echo [ERROR] .env not found at "%REPO_ROOT%\.env".
    echo         Copy .env.example to .env and fill it in first.
    goto :fail
)

REM --- read the current ARGUS_DATA_ROOT from .env (used as the default) --------
set "ENV_DATA_ROOT="
for /f "usebackq tokens=1,* delims==" %%A in ("%REPO_ROOT%\.env") do (
    if /i "%%A"=="ARGUS_DATA_ROOT" set "ENV_DATA_ROOT=%%B"
)

REM --- determine target data root: arg > .env value > D:\argus-data -----------
set "DATA_ROOT=%~1"
if not defined DATA_ROOT (
    set "DEFAULT_ROOT=%ENV_DATA_ROOT%"
    if not defined DEFAULT_ROOT set "DEFAULT_ROOT=D:\argus-data"
    echo Where should the database and data files live?
    echo   ^(must be a LOCAL disk, NOT inside OneDrive/Dropbox/Google Drive^)
    set /p "DATA_ROOT=Data root [!DEFAULT_ROOT!]: "
    if not defined DATA_ROOT set "DATA_ROOT=!DEFAULT_ROOT!"
)

REM --- strip surrounding quotes and any trailing backslash/space -------------
set "DATA_ROOT=%DATA_ROOT:"=%"
if "%DATA_ROOT:~-1%"=="\" set "DATA_ROOT=%DATA_ROOT:~0,-1%"
if "%DATA_ROOT:~-1%"==" " set "DATA_ROOT=%DATA_ROOT:~0,-1%"

echo.
echo Using data root: %DATA_ROOT%

REM --- guard: refuse a cloud-synced path (matches settings.py behaviour) -------
echo %DATA_ROOT% | findstr /i /c:"onedrive" /c:"dropbox" /c:"google drive" /c:"googledrive" >nul
if not errorlevel 1 (
    echo [ERROR] "%DATA_ROOT%" looks like a cloud-synced folder.
    echo         Parquet/DuckDB churn under a sync client corrupts data.
    echo         Pick a local path such as D:\argus-data.
    goto :fail
)

REM --- guard: .env already points at a DIFFERENT data root --------------------
REM normalise the existing .env value (strip quotes/trailing slash/space) so the
REM comparison doesn't false-alarm on cosmetic differences.
set "ENV_ROOT_NORM=%ENV_DATA_ROOT%"
if defined ENV_ROOT_NORM (
    set "ENV_ROOT_NORM=!ENV_ROOT_NORM:"=!"
    if "!ENV_ROOT_NORM:~-1!"=="\" set "ENV_ROOT_NORM=!ENV_ROOT_NORM:~0,-1!"
    if "!ENV_ROOT_NORM:~-1!"==" " set "ENV_ROOT_NORM=!ENV_ROOT_NORM:~0,-1!"
)
if defined ENV_ROOT_NORM if /i not "!ENV_ROOT_NORM!"=="%DATA_ROOT%" (
    echo.
    echo [WARNING] .env already points at a different data root:
    echo     current : !ENV_ROOT_NORM!
    echo     new     : %DATA_ROOT%
    echo Continuing will repoint .env to the new location. The old data root is NOT
    echo deleted, but ARGUS will stop using it ^(its DB/history become orphaned^).
    set "CONFIRM_REPOINT="
    set /p "CONFIRM_REPOINT=Repoint to the new location? [y/N]: "
    if /i not "!CONFIRM_REPOINT!"=="y" (
        echo Aborted. .env is unchanged and nothing was created.
        goto :fail
    )
)

REM --- create the data root ---------------------------------------------------
if not exist "%DATA_ROOT%" (
    echo Creating %DATA_ROOT% ...
    mkdir "%DATA_ROOT%" || (echo [ERROR] could not create "%DATA_ROOT%". & goto :fail)
)

REM --- write ARGUS_DATA_ROOT back into .env (preserve every other line) -------
echo Updating ARGUS_DATA_ROOT in .env ...
set "TMP_ENV=%REPO_ROOT%\.env.setuptmp"
if exist "%TMP_ENV%" del "%TMP_ENV%"
set "WROTE_ROOT="
REM keeporder: read line-by-line, rewrite the DATA_ROOT line, copy the rest verbatim
for /f "usebackq delims=" %%L in (`findstr /n "^" "%REPO_ROOT%\.env"`) do (
    set "line=%%L"
    REM strip the "N:" line-number prefix that findstr /n adds
    set "line=!line:*:=!"
    set "key="
    for /f "tokens=1 delims==" %%K in ("!line!") do set "key=%%K"
    if /i "!key!"=="ARGUS_DATA_ROOT" (
        echo ARGUS_DATA_ROOT=%DATA_ROOT%>>"%TMP_ENV%"
        set "WROTE_ROOT=1"
    ) else (
        echo(!line!>>"%TMP_ENV%"
    )
)
if not defined WROTE_ROOT echo ARGUS_DATA_ROOT=%DATA_ROOT%>>"%TMP_ENV%"
move /y "%TMP_ENV%" "%REPO_ROOT%\.env" >nul || (echo [ERROR] failed to update .env. & goto :fail)

REM --- locate a Python 3.11+ interpreter --------------------------------------
set "PYLAUNCH="
py -3.11 --version >nul 2>&1 && set "PYLAUNCH=py -3.11"
if not defined PYLAUNCH ( py -3 --version >nul 2>&1 && set "PYLAUNCH=py -3" )
if not defined PYLAUNCH ( python --version >nul 2>&1 && set "PYLAUNCH=python" )
if not defined PYLAUNCH (
    echo [ERROR] No Python found. Install Python 3.11+ and re-run.
    goto :fail
)
echo Python launcher: %PYLAUNCH%

REM --- create the venv under the data root ------------------------------------
set "VENV=%DATA_ROOT%\venv"
if exist "%VENV%\Scripts\python.exe" (
    echo Reusing existing venv at %VENV%
) else (
    echo Creating venv at %VENV% ...
    %PYLAUNCH% -m venv "%VENV%" || (echo [ERROR] venv creation failed. & goto :fail)
)
set "VPY=%VENV%\Scripts\python.exe"

REM --- install ARGUS (editable) ----------------------------------------------
echo Upgrading pip ...
"%VPY%" -m pip install --upgrade pip >nul || (echo [ERROR] pip upgrade failed. & goto :fail)
REM [dev] pulls pytest/ruff/mypy too: the verify loop runs out of this venv
REM (D:\argus-data\venv\Scripts\pytest.exe), so a runtime-only install leaves the
REM repo untestable on a fresh machine.
echo Installing ARGUS + dev tools (editable) - this can take a couple of minutes ...
pushd "%REPO_ROOT%"
"%VPY%" -m pip install -e ".[dev]" || (popd & echo [ERROR] pip install failed. & goto :fail)
popd

REM --- initialise the database + smoke check ----------------------------------
set "ARGUS_EXE=%VENV%\Scripts\argus.exe"
echo.
echo Initialising the database ...
pushd "%REPO_ROOT%"
"%ARGUS_EXE%" init-db || (popd & echo [ERROR] init-db failed. & goto :fail)
echo.
echo Running environment check ...
"%ARGUS_EXE%" check
popd

REM --- optional: seed the 10y history spine now (needs the Polygon key) --------
REM re-read the key from the (now-updated) .env; bootstrap hard-requires it.
set "POLYGON_KEY="
for /f "usebackq tokens=1,* delims==" %%A in ("%REPO_ROOT%\.env") do (
    if /i "%%A"=="ARGUS_POLYGON_API_KEY" set "POLYGON_KEY=%%B"
)
echo.
if not defined POLYGON_KEY (
    echo Skipping history bootstrap: ARGUS_POLYGON_API_KEY is not set in .env.
    echo   Run it later once the key is in:  "%ARGUS_EXE%" bootstrap
) else (
    echo Bootstrap seeds a ~10y daily spine ^(Polygon CAs -^> history -^> serve^).
    echo It is rate-limited ^(Polygon free tier ~5 calls/min^) and can take a while.
    set "DO_BOOTSTRAP="
    set /p "DO_BOOTSTRAP=Bootstrap history now? [y/N]: "
    if /i "!DO_BOOTSTRAP!"=="y" (
        echo Running bootstrap ...
        pushd "%REPO_ROOT%"
        "%ARGUS_EXE%" bootstrap
        set "BOOT_RC=!errorlevel!"
        popd
        if not "!BOOT_RC!"=="0" (
            echo [WARN] bootstrap exited with code !BOOT_RC!. Setup itself is complete;
            echo        re-run history seeding later with:  "%ARGUS_EXE%" bootstrap
        )
    ) else (
        echo Skipped. Seed later with:  "%ARGUS_EXE%" bootstrap
    )
)

echo.
echo === Done ==================================================================
echo   Data root : %DATA_ROOT%
echo   Database  : %DATA_ROOT%\argus.duckdb
echo   Venv      : %VENV%
echo   argus.exe : %ARGUS_EXE%
echo.
echo Next steps:
echo   - Register the scheduled tasks (elevated for the logon Catch-up task):
echo       powershell -ExecutionPolicy Bypass -File "%REPO_ROOT%\scripts\register_scheduled_tasks.ps1" -ArgusExe "%ARGUS_EXE%"
echo.
endlocal
exit /b 0

:fail
echo.
echo Setup did not complete. Nothing further was changed.
endlocal
exit /b 1
