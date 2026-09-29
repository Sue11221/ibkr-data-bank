@echo off
setlocal
cd /d "%~dp0"

if defined EMA_EXPORT_PYTHON goto custom_python
if exist "%LOCALAPPDATA%\Python\pythoncore-3.14-64\python.exe" goto project_python
where py >nul 2>&1
if not errorlevel 1 goto py_launcher
goto path_python

:custom_python
"%EMA_EXPORT_PYTHON%" "engine\export_cli.py" %*
goto finished

:project_python
"%LOCALAPPDATA%\Python\pythoncore-3.14-64\python.exe" "engine\export_cli.py" %*
goto finished

:py_launcher
py -3 "engine\export_cli.py" %*
goto finished

:path_python
python "engine\export_cli.py" %*

:finished
set "EXPORT_RC=%ERRORLEVEL%"
echo.
pause
exit /b %EXPORT_RC%
