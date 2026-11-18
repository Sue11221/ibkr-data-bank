@echo off
setlocal
set "PYTHON_EXE="
if exist "%LOCALAPPDATA%\Python\pythoncore-3.14-64\python.exe" set "PYTHON_EXE=%LOCALAPPDATA%\Python\pythoncore-3.14-64\python.exe"
if not defined PYTHON_EXE for /f "delims=" %%P in ('where.exe python.exe 2^>nul') do if not defined PYTHON_EXE set "PYTHON_EXE=%%P"
if defined PYTHON_EXE goto run
py -3 "%~dp0register_midnight_task.py" --register
exit /b %ERRORLEVEL%

:run
"%PYTHON_EXE%" "%~dp0register_midnight_task.py" --register
exit /b %ERRORLEVEL%
