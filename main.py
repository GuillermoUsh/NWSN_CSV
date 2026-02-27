"""
main.py — Punto de entrada del CSV Processor.
"""

import sys
import os

# Necesario para PyInstaller --onefile: los recursos están en sys._MEIPASS
if getattr(sys, "frozen", False):
    os.chdir(sys._MEIPASS)

import customtkinter as ctk
from app import CSVProcessorApp


def main():
    ctk.set_appearance_mode("System")   # "System", "Dark" o "Light"
    ctk.set_default_color_theme("blue")
    app = CSVProcessorApp()
    app.mainloop()


if __name__ == "__main__":
    main()
