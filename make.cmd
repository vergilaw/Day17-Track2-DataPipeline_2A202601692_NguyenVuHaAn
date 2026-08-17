@echo off
setlocal
set "PYTHONUTF8=1"
set "LAB17_DB=%~dp0warehouse.duckdb"
set "DBT_PROFILES_DIR=%~dp0dbt"

set "PY=%~dp0.venv\Scripts\python.exe"
set "PIP=%~dp0.venv\Scripts\pip.exe"
set "DBT=%~dp0.venv\Scripts\dbt.exe"

set "TARGET=%~1"
if "%TARGET%"=="" set "TARGET=help"

if /I "%TARGET%"=="setup" goto target_setup
if /I "%TARGET%"=="seed" goto target_seed
if /I "%TARGET%"=="seed-extra" goto target_seed_extra
if /I "%TARGET%"=="pipeline" goto target_pipeline
if /I "%TARGET%"=="verify" goto target_verify
if /I "%TARGET%"=="quick" goto target_quick
if /I "%TARGET%"=="explain" goto target_explain
if /I "%TARGET%"=="plan" goto target_plan
if /I "%TARGET%"=="compact" goto target_compact
if /I "%TARGET%"=="dbt-test" goto target_dbt_test
if /I "%TARGET%"=="dbt-docs" goto target_dbt_docs
if /I "%TARGET%"=="crash-test" goto target_crash_test
if /I "%TARGET%"=="reset" goto target_reset
if /I "%TARGET%"=="clean" goto target_clean
goto target_help

:target_setup
if not exist "%~dp0.venv" python -m venv "%~dp0.venv"
"%PIP%" install -q --upgrade pip
"%PIP%" install -q -r "%~dp0requirements.txt"
"%PY%" "%~dp0seed\generate.py" --extra
echo.
echo   xong. Buoc tiep theo:  .\make pipeline   roi   .\make verify
goto :eof

:target_seed
"%PY%" "%~dp0seed\generate.py"
goto :eof

:target_seed_extra
"%PY%" "%~dp0seed\generate.py" --extra
"%PY%" "%~dp0tools\explain.py" --save-baseline
goto :eof

:target_pipeline
"%PY%" "%~dp0tools\run_pipeline.py" %2 %3 %4 %5 %6
goto :eof

:target_verify
"%PY%" "%~dp0tools\verify.py" %2 %3 %4 %5 %6
goto :eof

:target_quick
"%PY%" "%~dp0tools\verify.py" --runs 1 %2 %3 %4 %5 %6
goto :eof

:target_explain
"%PY%" "%~dp0tools\explain.py" %2 %3 %4 %5 %6
goto :eof

:target_plan
"%PY%" "%~dp0tools\explain.py" --plan %2 %3 %4 %5 %6
goto :eof

:target_compact
"%PY%" "%~dp0tools\compact.py" %2 %3 %4 %5 %6
goto :eof

:target_dbt_test
pushd "%~dp0dbt"
"%DBT%" test --profiles-dir . --target-path target --log-path logs
popd
goto :eof

:target_dbt_docs
pushd "%~dp0dbt"
"%DBT%" docs generate --profiles-dir . --target-path target --log-path logs
"%DBT%" docs serve --profiles-dir . --target-path target
popd
goto :eof

:target_crash_test
"%PY%" "%~dp0tools\crash_test.py" %2 %3 %4 %5 %6
goto :eof

:target_reset
del /F /Q "%~dp0warehouse.duckdb" "%~dp0warehouse.duckdb.wal" 2>nul
echo   kho da xoa.
goto :eof

:target_clean
del /F /Q "%~dp0warehouse.duckdb" "%~dp0warehouse.duckdb.wal" 2>nul
rmdir /S /Q "%~dp0dbt\target" "%~dp0dbt\logs" "%~dp0data\crash" 2>nul
echo   da don.
goto :eof

:target_help
echo.
echo   LAB 17 -- Data Pipeline Engineering
echo.
echo     .\make setup        - venv + thu vien + sinh du lieu (chay mot lan)
echo     .\make pipeline     - chay duong ong mot luot (14 ngay van hanh)
echo     .\make verify       - xoa kho, chay 3 luot, in bang cham
echo     .\make quick        - nhu verify nhung chi 1 luot (nhanh)
echo     .\make seed         - sinh lai du lieu seed
echo     .\make seed-extra   - sinh them du lieu cho bai mo rong
echo     .\make explain      - do rows scanned cua queries/dashboard.sql
echo     .\make plan         - explain + in cay EXPLAIN ANALYZE
echo     .\make compact      - chay tools/compact.py
echo     .\make dbt-test     - chay dbt test
echo     .\make dbt-docs     - dung va mo tai lieu dbt
echo     .\make crash-test   - kich ban consumer bi giet giua batch
echo     .\make reset        - xoa kho DuckDB
echo     .\make clean        - xoa kho + target dbt + logs
echo.
goto :eof
