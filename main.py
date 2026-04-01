"""
main.py — Punto de entrada del CSV Processor.
"""

import sys
from PyQt6.QtWidgets import QApplication
from app import MainWindow


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setProperty("dark_mode", True)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
