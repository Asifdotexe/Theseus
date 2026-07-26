@echo off
cd engine
echo Running Theseus Engine...
cargo run --release -- %*
if %errorlevel% neq 0 (
    echo Engine execution failed!
    pause
    exit /b %errorlevel%
)
cd ..
