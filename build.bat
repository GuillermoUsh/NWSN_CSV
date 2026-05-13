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
python -m PyInstaller ^
  --onefile ^
  --windowed ^
  --name "CSV_Processor" ^
  --hidden-import PyQt6 ^
  --hidden-import PyQt6.QtWidgets ^
  --hidden-import PyQt6.QtCore ^
  --hidden-import PyQt6.QtGui ^
  --hidden-import pandas ^
  --hidden-import chardet ^
  --hidden-import constants ^
  --hidden-import ui ^
  --hidden-import ui.theme ^
  --hidden-import ui.workers ^
  --hidden-import ui.widgets ^
  --hidden-import ui.tabs ^
  --hidden-import ui.tabs.export_csv ^
  --hidden-import ui.tabs.export_txt ^
  --hidden-import ui.tabs.export_json ^
  --hidden-import ui.tabs.search ^
  --hidden-import ui.tabs.add_column ^
  --hidden-import ui.tabs.part_name ^
  main.py

echo.
echo [3/3] Listo!
echo El ejecutable esta en: dist\CSV_Processor.exe
echo.
pause
