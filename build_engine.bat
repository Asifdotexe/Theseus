@echo off
set CARGO_CMD=cargo
where cargo >nul 2>nul
if %errorlevel% neq 0 (
    set CARGO_CMD="%USERPROFILE%\.cargo\bin\cargo.exe"
)

echo Building Theseus Engine (Release mode)...
cd engine
%CARGO_CMD% build --release
if %errorlevel% neq 0 (
    echo Build failed! Make sure Rust is installed and in your PATH.
    pause
    exit /b %errorlevel%
)
echo Build successful! The binary is located at engine\target\release\engine.exe
cd ..
pause
