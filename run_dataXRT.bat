@echo off
REM Batch file to run dataXRT.py using the virtual environment Python

REM Get the directory where this batch file is located
set SCRIPT_DIR=%~dp0

REM Run dataXRT.py using the virtual environment Python
"%SCRIPT_DIR%.venv\Scripts\python.exe" "%SCRIPT_DIR%dataXRT.py" %*
