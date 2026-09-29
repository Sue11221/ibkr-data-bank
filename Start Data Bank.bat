@echo off
setlocal
cd /d "%~dp0"
set "PYTHONW_EXE="

if exist "%LOCALAPPDATA%\Python\pythoncore-3.14-64\pythonw.exe" set "PYTHONW_EXE=%LOCALAPPDATA%\Python\pythoncore-3.14-64\pythonw.exe"
if defined PYTHONW_EXE goto launch

for /f "delims=" %%P in ('where.exe pythonw.exe 2^>nul') do if not defined PYTHONW_EXE set "PYTHONW_EXE=%%P"
if defined PYTHONW_EXE goto launch

for /f "delims=" %%P in ('where.exe python.exe 2^>nul') do if not defined PYTHONW_EXE if exist "%%~dpPpythonw.exe" set "PYTHONW_EXE=%%~dpPpythonw.exe"
if defined PYTHONW_EXE goto launch

for /f "usebackq delims=" %%P in (`py -3 -c "import pathlib,sys; print(pathlib.Path(sys.executable).with_name('pythonw.exe'))" 2^>nul`) do if exist "%%P" set "PYTHONW_EXE=%%P"

:launch
if not defined PYTHONW_EXE goto missing
start "" "%PYTHONW_EXE%" "%~dp0launch_app.py"
exit /b 0

:missing
echo Python's windowless executable ^(pythonw.exe^) could not be found.
echo Install Python or add it to PATH, then try again.
pause
exit /b 1
