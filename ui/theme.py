"""
ui/theme.py — Paletas de color, hojas de estilo y recursos gráficos temporales.
"""

import os
import tempfile

# ── Paletas ───────────────────────────────────────────────────────────────────

DARK: dict = {
    "bg": "#111827", "surface": "#1f2937", "surface2": "#374151",
    "border": "#4b5563", "text": "#e5e7eb", "text_muted": "#9ca3af",
    "accent": "#6096d0", "accent_hover": "#4f85bf",
    "header_bg": "#1e3a5f", "header_text": "#e2e8f0",
    "row_alt": "#1a2535", "row_sel": "#3a6ea8", "row_sel_text": "#ffffff",
    "btn_bg": "#374151", "btn_hover": "#4b5563", "btn_text": "#e5e7eb",
    "success": "#4ade80", "error": "#f87171", "warning": "#fbbf24",
    "input_bg": "#1f2937", "panel_bg": "#1a2535",
    "btn_success": "#16a34a", "btn_error": "#dc2626",
}

LIGHT: dict = {
    "bg": "#f1f5f9", "surface": "#ffffff", "surface2": "#e9eff6",
    "border": "#c8d5e3", "text": "#1e293b", "text_muted": "#64748b",
    "accent": "#4a7fc1", "accent_hover": "#3a6eb0",
    "header_bg": "#4a7fc1", "header_text": "#ffffff",
    "row_alt": "#eef2f7", "row_sel": "#4a7fc1", "row_sel_text": "#ffffff",
    "btn_bg": "#dce6f0", "btn_hover": "#c9d8ea", "btn_text": "#1e293b",
    "success": "#16a34a", "error": "#b84040", "warning": "#b07820",
    "input_bg": "#ffffff", "panel_bg": "#f0f4f9",
    "btn_success": "#16a34a", "btn_error": "#dc2626",
}

# ── SVG temporales para QComboBox::down-arrow ─────────────────────────────────

def _write_arrow_svg(color: str) -> str:
    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="10" height="6">'
        f'<polygon points="0,0 10,0 5,6" fill="{color}"/>'
        f'</svg>'
    )
    f = tempfile.NamedTemporaryFile(suffix=".svg", delete=False, mode="w", encoding="utf-8")
    f.write(svg)
    f.close()
    return f.name


_ARROW_DARK_FILE  = _write_arrow_svg("#9ca3af")
_ARROW_LIGHT_FILE = _write_arrow_svg("#64748b")

# Rutas con separadores forward-slash para Qt
_ARROW_DARK  = _ARROW_DARK_FILE.replace("\\", "/")
_ARROW_LIGHT = _ARROW_LIGHT_FILE.replace("\\", "/")


def cleanup_temp_arrows() -> None:
    """Elimina los archivos SVG temporales. Debe llamarse al salir la aplicación."""
    for path in (_ARROW_DARK_FILE, _ARROW_LIGHT_FILE):
        try:
            os.unlink(path)
        except OSError:
            pass


# ── Hojas de estilo ───────────────────────────────────────────────────────────

def app_stylesheet(p: dict) -> str:
    arrow_path = _ARROW_DARK if p.get("bg") == DARK["bg"] else _ARROW_LIGHT
    return f"""
    /* ── Base ─────────────────────────────────────────────────────────────── */
    QWidget {{
        background-color: {p['bg']};
        color: {p['text']};
        font-family: "Segoe UI";
        font-size: 10pt;
    }}

    /* ── Tab bar — estilo flat con indicador de color en tab activo ────────── */
    QTabWidget::pane {{
        border: 1px solid {p['border']};
        border-top: none;
        border-radius: 0 0 6px 6px;
    }}
    QTabBar {{
        background: {p['bg']};
    }}
    QTabBar::tab {{
        background: {p['surface2']};
        color: {p['text_muted']};
        padding: 10px 22px;
        border: 1px solid {p['border']};
        border-bottom: none;
        border-radius: 6px 6px 0 0;
        margin-right: 3px;
        font-weight: 500;
        min-width: 80px;
    }}
    QTabBar::tab:selected {{
        background: {p['accent']};
        color: #ffffff;
        border-color: {p['accent']};
        font-weight: 700;
    }}
    QTabBar::tab:hover:!selected {{
        background: {p['btn_hover']};
        color: {p['text']};
    }}

    /* ── Botones ───────────────────────────────────────────────────────────── */
    QPushButton {{
        background-color: {p['btn_bg']};
        color: {p['btn_text']};
        border: 1px solid {p['border']};
        border-radius: 6px;
        padding: 4px 16px;
        font-weight: 500;
    }}
    QPushButton:hover  {{ background-color: {p['btn_hover']}; }}
    QPushButton:pressed {{ background-color: {p['accent']}; color: #ffffff; border-color: {p['accent']}; }}
    QPushButton:disabled {{ background-color: {p['surface2']}; color: {p['text_muted']}; border-color: {p['border']}; }}

    QPushButton#success {{
        background-color: {p['btn_success']};
        color: #ffffff;
        border-color: {p['btn_success']};
        font-weight: 600;
    }}
    QPushButton#success:hover {{ background-color: {p['accent_hover']}; border-color: {p['accent_hover']}; }}
    QPushButton#success:disabled {{ background-color: {p['surface2']}; color: {p['text_muted']}; border-color: {p['border']}; }}

    QPushButton#danger {{
        background-color: {p['btn_error']};
        color: #ffffff;
        border-color: {p['btn_error']};
    }}
    QPushButton#danger:hover {{ background-color: #b91c1c; border-color: #b91c1c; }}
    QPushButton#danger:disabled {{ background-color: {p['surface2']}; color: {p['text_muted']}; border-color: {p['border']}; }}

    QPushButton#accent {{
        background-color: {p['accent']};
        color: #ffffff;
        border-color: {p['accent']};
        font-weight: 600;
    }}
    QPushButton#accent:hover {{ background-color: {p['accent_hover']}; border-color: {p['accent_hover']}; }}

    /* ── Inputs ────────────────────────────────────────────────────────────── */
    QLineEdit, QPlainTextEdit, QTextEdit, QComboBox {{
        background-color: {p['input_bg']};
        color: {p['text']};
        border: 1px solid {p['border']};
        border-radius: 5px;
        padding: 4px 8px;
    }}
    QLineEdit:focus, QPlainTextEdit:focus, QTextEdit:focus {{
        border: 2px solid {p['accent']};
        padding: 3px 7px;
    }}
    QComboBox:focus {{ border: 2px solid {p['accent']}; padding: 3px 7px; }}

    QComboBox::drop-down {{
        border-left: 1px solid {p['border']};
        width: 24px;
        background: {p['surface2']};
        border-top-right-radius: 4px;
        border-bottom-right-radius: 4px;
    }}
    QComboBox::down-arrow {{ image: url("{arrow_path}"); width: 10px; height: 6px; }}
    QComboBox QAbstractItemView {{
        background: {p['surface']};
        color: {p['text']};
        selection-background-color: {p['accent']};
        border: 1px solid {p['border']};
    }}

    /* ── Checkboxes y Radios ───────────────────────────────────────────────── */
    QCheckBox {{ color: {p['text']}; spacing: 6px; }}
    QCheckBox::indicator {{
        width: 16px; height: 16px;
        border: 2px solid {p['border']};
        border-radius: 3px;
        background: {p['input_bg']};
    }}
    QCheckBox::indicator:checked {{ background: {p['accent']}; border-color: {p['accent']}; }}

    QRadioButton {{ color: {p['text']}; spacing: 6px; }}
    QRadioButton::indicator {{
        width: 14px; height: 14px;
        border-radius: 7px;
        border: 2px solid {p['border']};
        background: {p['input_bg']};
    }}
    QRadioButton::indicator:checked {{ background: {p['accent']}; border-color: {p['accent']}; }}

    /* ── Scrollbars ────────────────────────────────────────────────────────── */
    QScrollBar:vertical {{
        background: {p['surface2']}; width: 8px; border-radius: 4px; margin: 0;
    }}
    QScrollBar::handle:vertical {{
        background: {p['border']}; border-radius: 4px; min-height: 24px;
    }}
    QScrollBar::handle:vertical:hover {{ background: {p['accent']}; }}

    QScrollBar:horizontal {{
        background: {p['surface2']}; height: 8px; border-radius: 4px; margin: 0;
    }}
    QScrollBar::handle:horizontal {{
        background: {p['border']}; border-radius: 4px; min-width: 24px;
    }}
    QScrollBar::handle:horizontal:hover {{ background: {p['accent']}; }}
    QScrollBar::add-line, QScrollBar::sub-line {{ width: 0; height: 0; }}

    /* ── Progress bar — slim y moderno ────────────────────────────────────── */
    QProgressBar {{
        border: none;
        border-radius: 4px;
        background: {p['surface2']};
        text-align: center;
        color: transparent;
    }}
    QProgressBar::chunk {{
        background: {p['accent']};
        border-radius: 4px;
    }}

    /* ── Misceláneos ───────────────────────────────────────────────────────── */
    QSplitter::handle {{ background: {p['border']}; }}
    QFrame[frameShape="4"], QFrame[frameShape="5"] {{ color: {p['border']}; max-height: 1px; }}

    QStatusBar {{
        background: {p['surface']};
        color: {p['text_muted']};
        font-size: 9pt;
        border-top: 1px solid {p['border']};
    }}
    QDialog {{ background: {p['bg']}; }}
    QScrollArea {{ border: none; background: transparent; }}
    QScrollArea > QWidget > QWidget {{ background: transparent; }}

    QWidget#ColumnPanel, QWidget#SearchFilesPanel {{
        background-color: {p['panel_bg']};
        border-right: 1px solid {p['border']};
    }}
    QToolButton#btn_theme {{
        font-size: 20pt;
        font-family: "Segoe UI Symbol";
        background: transparent;
        border: none;
        border-radius: 6px;
        padding: 2px;
    }}
    QToolButton#btn_theme:hover {{ background: {p['surface2']}; }}
"""


def table_stylesheet(p: dict) -> str:
    return f"""
    QTableView {{
        background-color: {p['surface']};
        color: {p['text']};
        gridline-color: {p['border']};
        border: 1px solid {p['border']};
        border-radius: 6px;
        outline: none;
    }}
    QTableView::item:selected {{
        background-color: {p['row_sel']};
        color: {p['row_sel_text']};
    }}
    QHeaderView::section {{
        background-color: {p['header_bg']};
        color: {p['header_text']};
        padding: 5px 10px;
        border: none;
        border-right: 1px solid {p['border']};
        font-weight: 700;
        font-size: 10pt;
    }}
    QHeaderView::section:hover {{ background-color: {p['accent_hover']}; }}
    QHeaderView::section:pressed {{ background-color: {p['accent']}; }}
    """
