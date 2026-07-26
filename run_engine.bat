@echo off
set CARGO_CMD=cargo
where cargo >nul 2>nul
if %errorlevel% neq 0 (
    set CARGO_CMD="%USERPROFILE%\.cargo\bin\cargo.exe"
)

cd engine
echo Running Theseus Engine...
%CARGO_CMD% run --release -- %*
if %errorlevel% neq 0 (
    echo Engine execution failed!
    pause
    exit /b %errorlevel%
)
cd ..
