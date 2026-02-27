@echo off
REM ============================================================
REM  build.bat — Genera el ejecutable .exe con PyInstaller
REM  Ejecutar desde la carpeta del proyecto: build.bat
REM ============================================================

echo.
echo [1/3] Instalando dependencias...
pip install -r requirements.txt

echo.
echo [2/3] Compilando con PyInstaller...
pyinstaller ^
  --onefile ^
  --windowed ^
  --name "CSV_Processor" ^
  --collect-data customtkinter ^
  --hidden-import customtkinter ^
  --hidden-import pandas ^
  --hidden-import chardet ^
  main.py

echo.
echo [3/3] Listo!
echo El ejecutable esta en: dist\CSV_Processor.exe
echo.
pause
