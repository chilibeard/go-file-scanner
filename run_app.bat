@echo off
echo Updating PATH to include MinGW-w64...
setx PATH "%PATH%;C:\msys64\mingw64\bin"
echo PATH updated. Please reboot your system for the changes to take effect.
echo After rebooting, run this batch file again to compile and run the application.
echo.
echo If you have already rebooted, press any key to continue with compilation...
pause > nul

cd file_scanner
go mod tidy
if %errorlevel% neq 0 (
    echo Failed to update dependencies. Please check the error messages above.
    pause
    exit /b %errorlevel%
)
go build -o file_scanner.exe
if %errorlevel% neq 0 (
    echo Compilation failed. Please check the error messages above.
    pause
    exit /b %errorlevel%
)
echo Compilation successful. Running the application...
start file_scanner.exe
