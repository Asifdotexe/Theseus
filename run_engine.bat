@echo off
set CARGO_CMD=cargo
where cargo >nul 2>nul
if %errorlevel% neq 0 (
    set CARGO_CMD="%USERPROFILE%\.cargo\bin\cargo.exe"
)

echo Running Theseus Engine...
pushd "%~dp0engine" || exit /b 1
%CARGO_CMD% run --release -- %*
set "EXIT_CODE=%errorlevel%"
popd
if %EXIT_CODE% neq 0 (
    echo Engine execution failed!
    pause
    exit /b %EXIT_CODE%
)
