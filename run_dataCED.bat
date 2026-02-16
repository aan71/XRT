@echo off
REM Batch file to run dataCED.py using the virtual environment Python

REM Get the directory where this batch file is located
set SCRIPT_DIR=%~dp0

REM Run dataCED.py using the virtual environment Python
"%SCRIPT_DIR%.venv\Scripts\python.exe" "%SCRIPT_DIR%dataCED.py" %*

