"""
app.py — CSV Processor · PyQt6
"""

import os
import sys
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
from PyQt6.QtCore import (
    Qt, QAbstractTableModel, QModelIndex, QSortFilterProxyModel,
    QThread, pyqtSignal, QTimer,
)
from PyQt6.QtGui import QColor, QFont, QKeySequence, QShortcut, QCursor
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QDialog,
    QVBoxLayout, QHBoxLayout, QGridLayout, QFormLayout,
    QSplitter, QTabWidget, QStackedWidget, QScrollArea,
    QTableView, QHeaderView, QAbstractItemView,
    QLabel, QPushButton, QToolButton, QLineEdit, QPlainTextEdit,
    QTextEdit, QCheckBox, QRadioButton, QButtonGroup, QComboBox,
    QProgressBar, QFrame, QSizePolicy, QFileDialog, QMessageBox,
    QStatusBar,
)

from processor import (
    detect_encoding, detect_delimiter, get_columns, get_preview,
    get_unique_column_values, get_unique_values_by_group,
    collect_rows_by_group, collect_rows_by_two_groups,
    sanitize_filename, process_csv, search_value_in_csv, search_values_in_csv,
)

# ── Constantes ────────────────────────────────────────────────────────────────

PRESET_COLUMNS   = ["PHONEMODEL_NAME", "SN", "KEYUNITBARCODE", "CLASSCODE", "CREATETIME", "KEYMATERIAL"]
PRESET_TO_JSON_KEY = {
    "PHONEMODEL_NAME": "phoneModelName", "SN": "sn",
    "KEYUNITBARCODE": "keyUnitBarcode",  "CLASSCODE": "classCode",
    "CREATETIME": "createTime",          "KEYMATERIAL": "keyMaterial",
}
CLASS_CODE_MAP = {
    "AT": "Battery_AT", "KTL": "Front_Housing_KTL", "HS": "Rear_Camera_HS",
    "KMTL": "Middle_Frame_KMTL", "CP": "Charger_Port_CP", "BTN": "Side_Button_BTN",
    "SPK": "Speaker_SPK", "MIC": "Microphone_MIC", "VIB": "Vibrator_VIB",
    "CAM": "Front_Camera_CAM", "SCR": "Screen_Assembly_SCR", "BT": "Bluetooth_Module_BT",
    "WIFI": "WiFi_Module_WIFI", "NFC": "NFC_Module_NFC", "FP": "Fingerprint_FP",
    "LCD": "LCD_Panel_LCD", "TP": "Touch_Panel_TP", "PCB": "Main_Board_PCB",
}
SEARCH_FILE_PALETTE = [
    ("#dbeafe", "#1e3a5f"), ("#dcfce7", "#14532d"), ("#fef9c3", "#713f12"),
    ("#fce7f3", "#831843"), ("#ede9fe", "#4c1d95"), ("#ffedd5", "#7c2d12"),
    ("#cffafe", "#164e63"), ("#f1f5f9", "#334155"),
]
PREVIEW_ROWS  = 2000   # filas cargadas en memoria para el preview
PAGE_SIZE     = 200    # filas visibles por página

# ── Paletas ───────────────────────────────────────────────────────────────────

DARK = {
    "bg": "#111827", "surface": "#1f2937", "surface2": "#374151",
    "border": "#4b5563", "text": "#e5e7eb", "text_muted": "#9ca3af",
    "accent": "#6096d0", "accent_hover": "#4f85bf",
    "header_bg": "#1e3a5f", "header_text": "#e2e8f0",
    "row_alt": "#1a2535", "row_sel": "#3a6ea8", "row_sel_text": "#ffffff",
    "btn_bg": "#374151", "btn_hover": "#4b5563", "btn_text": "#e5e7eb",
    "success": "#4ade80", "error": "#f87171", "warning": "#fbbf24",
    "input_bg": "#1f2937", "panel_bg": "#1f2937",
}
LIGHT = {
    "bg": "#f1f5f9", "surface": "#ffffff", "surface2": "#e9eff6",
    "border": "#c8d5e3", "text": "#1e293b", "text_muted": "#64748b",
    "accent": "#4a7fc1", "accent_hover": "#3a6eb0",
    "header_bg": "#4a7fc1", "header_text": "#ffffff",
    "row_alt": "#eef2f7", "row_sel": "#4a7fc1", "row_sel_text": "#ffffff",
    "btn_bg": "#dce6f0", "btn_hover": "#c9d8ea", "btn_text": "#1e293b",
    "success": "#3d9970", "error": "#b84040", "warning": "#b07820",
    "input_bg": "#ffffff", "panel_bg": "#f8fafc",
}

# ── Helpers de estilo ─────────────────────────────────────────────────────────

def app_stylesheet(p: dict) -> str:
    return f"""
    QWidget {{ background-color: {p['bg']}; color: {p['text']}; font-family: "Segoe UI"; font-size: 10pt; }}
    QTabWidget::pane {{ border: 1px solid {p['border']}; border-radius: 4px; }}
    QTabBar::tab {{ background: {p['surface2']}; color: {p['text_muted']}; padding: 7px 18px;
                    border-top-left-radius: 4px; border-top-right-radius: 4px;
                    border: 1px solid {p['border']}; margin-right: 2px; }}
    QTabBar::tab:selected {{ background: {p['accent']}; color: #ffffff; border-color: {p['accent']}; }}
    QTabBar::tab:hover:!selected {{ background: {p['btn_hover']}; color: {p['text']}; }}
    QPushButton {{ background-color: {p['btn_bg']}; color: {p['btn_text']};
                   border: 1px solid {p['border']}; border-radius: 6px; padding: 5px 14px; }}
    QPushButton:hover {{ background-color: {p['btn_hover']}; }}
    QPushButton:disabled {{ background-color: {p['surface2']}; color: {p['text_muted']}; }}
    QPushButton#success {{ background-color: {p['success']}; color: #ffffff; border-color: {p['success']}; }}
    QPushButton#success:hover {{ background-color: {p['accent_hover']}; }}
    QPushButton#danger {{ background-color: {p['error']}; color: #ffffff; border-color: {p['error']}; }}
    QPushButton#danger:hover {{ background-color: {p['accent_hover']}; }}
    QPushButton#accent {{ background-color: {p['accent']}; color: #ffffff; border-color: {p['accent']}; }}
    QPushButton#accent:hover {{ background-color: {p['accent_hover']}; }}
    QLineEdit, QPlainTextEdit, QTextEdit, QComboBox {{
        background-color: {p['input_bg']}; color: {p['text']};
        border: 1px solid {p['border']}; border-radius: 4px; padding: 4px 8px; }}
    QLineEdit:focus, QPlainTextEdit:focus, QTextEdit:focus {{ border-color: {p['accent']}; }}
    QComboBox::drop-down {{ border: none; width: 20px; }}
    QComboBox QAbstractItemView {{ background: {p['surface']}; color: {p['text']};
                                    selection-background-color: {p['accent']}; border: 1px solid {p['border']}; }}
    QCheckBox {{ color: {p['text']}; spacing: 6px; }}
    QCheckBox::indicator {{ width: 16px; height: 16px; border: 1px solid {p['border']};
                             border-radius: 3px; background: {p['input_bg']}; }}
    QCheckBox::indicator:checked {{ background: {p['accent']}; border-color: {p['accent']}; }}
    QRadioButton {{ color: {p['text']}; spacing: 6px; }}
    QRadioButton::indicator {{ width: 14px; height: 14px; border-radius: 7px;
                                border: 1px solid {p['border']}; background: {p['input_bg']}; }}
    QRadioButton::indicator:checked {{ background: {p['accent']}; border-color: {p['accent']}; }}
    QScrollBar:vertical {{ background: {p['surface2']}; width: 10px; border-radius: 5px; }}
    QScrollBar::handle:vertical {{ background: {p['border']}; border-radius: 5px; min-height: 30px; }}
    QScrollBar::handle:vertical:hover {{ background: {p['accent']}; }}
    QScrollBar:horizontal {{ background: {p['surface2']}; height: 10px; border-radius: 5px; }}
    QScrollBar::handle:horizontal {{ background: {p['border']}; border-radius: 5px; min-width: 30px; }}
    QScrollBar::handle:horizontal:hover {{ background: {p['accent']}; }}
    QScrollBar::add-line, QScrollBar::sub-line {{ width: 0; height: 0; }}
    QProgressBar {{ border: 1px solid {p['border']}; border-radius: 4px;
                    background: {p['surface2']}; text-align: center; color: {p['text']}; height: 18px; }}
    QProgressBar::chunk {{ background: {p['accent']}; border-radius: 3px; }}
    QSplitter::handle {{ background: {p['border']}; }}
    QFrame[frameShape="4"], QFrame[frameShape="5"] {{ color: {p['border']}; }}
    QStatusBar {{ background: {p['surface']}; color: {p['text_muted']}; font-size: 9pt; border-top: 1px solid {p['border']}; }}
    QDialog {{ background: {p['bg']}; }}
    QScrollArea {{ border: none; background: transparent; }}
    QScrollArea > QWidget > QWidget {{ background: transparent; }}
    QWidget#ColumnPanel, QWidget#SearchFilesPanel {{ background-color: {p['panel_bg']}; }}
    """

def table_stylesheet(p: dict) -> str:
    return f"""
    QTableView {{ background-color: {p['surface']}; color: {p['text']}; gridline-color: {p['border']};
                  border: 1px solid {p['border']}; border-radius: 6px; outline: none; }}
    QTableView::item:selected {{ background-color: {p['row_sel']}; color: {p['row_sel_text']}; }}
    QHeaderView::section {{ background-color: {p['header_bg']}; color: {p['header_text']};
                             padding: 5px 8px; border: none; border-right: 1px solid {p['border']};
                             font-weight: bold; }}
    QHeaderView::section:hover {{ background-color: {p['accent_hover']}; }}
    QHeaderView::section:pressed {{ background-color: {p['accent']}; }}
    """

# ── Workers ───────────────────────────────────────────────────────────────────

class FileLoaderWorker(QThread):
    done  = pyqtSignal(str, str, str, list, object)  # path, enc, delim, cols, df
    error = pyqtSignal(str)

    def __init__(self, path: str):
        super().__init__()
        self._path = path

    def run(self):
        try:
            enc   = detect_encoding(self._path)
            delim = detect_delimiter(self._path, enc)
            cols  = get_columns(self._path, enc, delim)
            df    = get_preview(self._path, enc, delim, PREVIEW_ROWS)
            self.done.emit(self._path, enc, delim, cols, df)
        except Exception as e:
            self.error.emit(str(e))


class CSVExportWorker(QThread):
    progress  = pyqtSignal(int, str)
    done      = pyqtSignal(str)
    error     = pyqtSignal(str)

    def __init__(self, filepath, enc, delim, columns, filters, out_dir, out_delim, rename_map):
        super().__init__()
        self._filepath   = filepath
        self._enc        = enc
        self._delim      = delim
        self._columns    = columns
        self._filters    = filters
        self._out_dir    = out_dir
        self._out_delim  = out_delim
        self._rename_map = rename_map

    def run(self):
        def cb(pct, msg): self.progress.emit(int(pct * 100), msg)
        try:
            process_csv(
                filepath         = self._filepath,
                encoding         = self._enc,
                delimiter        = self._delim,
                selected_columns = self._columns,
                filters          = self._filters,
                output_dir       = self._out_dir,
                out_delimiter    = self._out_delim,
                rename_map       = self._rename_map,
                progress_callback= cb,
            )
            self.done.emit(self._out_dir)
        except Exception as e:
            self.error.emit(str(e))


class SearchWorker(QThread):
    progress  = pyqtSignal(int, str)
    done      = pyqtSignal(list)
    error     = pyqtSignal(str)
    cancelled = pyqtSignal()

    def __init__(self, files: list, values: list, search_col: str):
        super().__init__()
        self._files      = files
        self._values     = values
        self._search_col = search_col
        self._cancel     = False

    def cancel(self): self._cancel = True

    def run(self):
        results = []
        total   = len(self._files)
        try:
            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = {
                    pool.submit(self._search_one, fp, i): fp
                    for i, fp in enumerate(self._files)
                }
                for idx, fut in enumerate(as_completed(futures)):
                    if self._cancel:
                        pool.shutdown(wait=False, cancel_futures=True)
                        self.cancelled.emit()
                        return
                    rows, fp = fut.result()
                    results.extend(rows)
                    pct = int((idx + 1) / total * 100)
                    self.progress.emit(pct, f"Buscando en {Path(fp).name}…")
            self.done.emit(results)
        except Exception as e:
            self.error.emit(str(e))

    def _search_one(self, filepath: str, file_idx: int):
        try:
            enc   = detect_encoding(filepath)
            delim = detect_delimiter(filepath, enc)
            cols  = get_columns(filepath, enc, delim)
            # resolver columna de búsqueda
            col = self._search_col
            if col not in cols:
                col = next((c for c in cols if self._search_col.lower() in c.lower()), None)
            if col is None:
                return [], filepath
            rows = search_values_in_csv(filepath, enc, delim, col, self._values)
            palette_idx = file_idx % len(SEARCH_FILE_PALETTE)
            for row in rows:
                row["__file__"]    = Path(filepath).name
                row["__filepath__"] = filepath
                row["__palette__"] = palette_idx
            return rows, filepath
        except Exception as e:
            return [], filepath  # el worker principal captura y emite error




class GenericWorker(QThread):
    """Worker genérico para TXT, JSON, AddColumn y exportaciones de búsqueda."""
    progress  = pyqtSignal(int, str)
    done      = pyqtSignal(str)
    error     = pyqtSignal(str)
    cancelled = pyqtSignal()

    def __init__(self, fn, *args):
        super().__init__()
        self._fn     = fn
        self._args   = args
        self._cancel = False

    def cancel(self): self._cancel = True

    def run(self):
        def cb(pct, msg): self.progress.emit(pct, msg)
        try:
            result = self._fn(*self._args, lambda: self._cancel, cb)
            if self._cancel:
                self.cancelled.emit()
            else:
                self.done.emit(result or "")
        except Exception as e:
            self.error.emit(str(e))

# ── Modelo virtual ────────────────────────────────────────────────────────────

class PandasTableModel(QAbstractTableModel):
    def __init__(self, df: pd.DataFrame, palette: dict, row_colors: list = None, parent=None):
        super().__init__(parent)
        self._df         = df
        self._p          = palette
        self._row_colors = row_colors or []   # list of (light_hex, dark_hex) per row

    def rowCount(self, parent=QModelIndex()): return len(self._df)
    def columnCount(self, parent=QModelIndex()): return len(self._df.columns)

    def data(self, index: QModelIndex, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid(): return None
        row, col = index.row(), index.column()

        if role == Qt.ItemDataRole.DisplayRole:
            val = self._df.iat[row, col]
            return "" if pd.isna(val) else str(val)

        if role == Qt.ItemDataRole.BackgroundRole:
            if self._row_colors and row < len(self._row_colors):
                dark_mode = self._p is DARK
                hex_color = self._row_colors[row][1 if dark_mode else 0]
                return QColor(hex_color)
            return QColor(self._p["row_alt"] if row % 2 else self._p["surface"])

        if role == Qt.ItemDataRole.ForegroundRole:
            return QColor(self._p["text"])

        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if role == Qt.ItemDataRole.DisplayRole:
            if orientation == Qt.Orientation.Horizontal:
                return str(self._df.columns[section])
            return str(section + 1)
        if role == Qt.ItemDataRole.FontRole:
            return QFont("Segoe UI", 10, QFont.Weight.Bold)
        return None

    def update_df(self, df: pd.DataFrame, row_colors: list = None):
        self.beginResetModel()
        self._df         = df
        self._row_colors = row_colors or []
        self.endResetModel()

    def update_palette(self, p: dict):
        self._p = p
        self.layoutChanged.emit()

    @property
    def df(self): return self._df

# ── Tabla estilizada ──────────────────────────────────────────────────────────

class DataTable(QWidget):
    """QTableView con proxy de sort y CopyBar integrada."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self._model: PandasTableModel | None = None
        self._proxy = QSortFilterProxyModel()
        self._last_src_index: tuple[int, int] | None = None

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)

        self.view = QTableView()
        self.view.setModel(self._proxy)
        self.view.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectItems)
        self.view.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.view.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.view.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.view.horizontalHeader().setStretchLastSection(True)
        self.view.verticalHeader().setDefaultSectionSize(26)
        self.view.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        self.view.setSortingEnabled(True)
        layout.addWidget(self.view, stretch=1)

        # Copy bar
        copy_row = QHBoxLayout()
        copy_row.setContentsMargins(0, 0, 0, 0)
        self.lbl_copy = QLabel("")
        self.lbl_copy.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.lbl_copy.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        self.btn_copy = QPushButton("⎘  Copiar")
        self.btn_copy.setFixedSize(90, 28)
        self.btn_copy.setEnabled(False)
        self.btn_copy.clicked.connect(self._do_copy)
        copy_row.addWidget(self.lbl_copy)
        copy_row.addWidget(self.btn_copy)
        layout.addLayout(copy_row)

        QShortcut(QKeySequence("Ctrl+C"), self.view).activated.connect(self._do_copy)

    def set_model(self, model: PandasTableModel):
        self._model = model
        self._proxy.setSourceModel(model)
        self.view.selectionModel().currentChanged.connect(self._on_cell_changed)
        self._last_src_index = None
        self.lbl_copy.setText("")
        self.btn_copy.setEnabled(False)

    def _on_cell_changed(self, current: QModelIndex, _prev: QModelIndex):
        if not current.isValid() or self._model is None:
            self.lbl_copy.setText("")
            self.btn_copy.setEnabled(False)
            self._last_src_index = None
            return
        src = self._proxy.mapToSource(current)
        row, col = src.row(), src.column()
        self._last_src_index = (row, col)
        col_name = str(self._model.df.columns[col])
        value    = str(self._model.df.iat[row, col])
        self.lbl_copy.setText(f'{col_name}: "{value}"')
        self.btn_copy.setEnabled(True)

    def _do_copy(self):
        if self._last_src_index is None or self._model is None: return
        row, col = self._last_src_index
        value = str(self._model.df.iat[row, col])
        QApplication.clipboard().setText(value)

    def apply_palette(self, p: dict):
        self.view.setStyleSheet(table_stylesheet(p))
        self.lbl_copy.setStyleSheet(f"color: {p['accent']}; padding: 2px 4px;")
        if self._model:
            self._model.update_palette(p)

# ── Barra de progreso reutilizable ────────────────────────────────────────────

class ProgressRow(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        h = QHBoxLayout(self)
        h.setContentsMargins(0, 0, 0, 0)
        h.setSpacing(8)
        self.bar = QProgressBar()
        self.bar.setRange(0, 100)
        self.bar.setValue(0)
        self.bar.setFixedHeight(26)
        self.lbl = QLabel("0%")
        self.lbl.setFixedWidth(38)
        h.addWidget(self.bar, stretch=1)
        h.addWidget(self.lbl)

    def set(self, pct: int):
        self.bar.setValue(pct)
        self.lbl.setText(f"{pct}%")

    def reset(self):
        self.bar.setValue(0)
        self.lbl.setText("0%")

# ── Separador ─────────────────────────────────────────────────────────────────

def hline():
    f = QFrame()
    f.setFrameShape(QFrame.Shape.HLine)
    return f

# ── ColumnMapDialog ───────────────────────────────────────────────────────────

class ColumnMapDialog(QDialog):
    def __init__(self, missing: list, available: list, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Mapear columnas")
        self.setModal(True)
        self.setMinimumWidth(460)
        self.result_map: dict = {}
        self.cancelled = True

        layout = QVBoxLayout(self)
        layout.setSpacing(10)

        lbl = QLabel("Algunas columnas preset no se encontraron en el CSV.\n"
                     "Mapeá cada una o elegí '(omitir)'.")
        lbl.setWordWrap(True)
        layout.addWidget(lbl)
        layout.addWidget(hline())

        self._combos: dict[str, QComboBox] = {}
        options = ["(omitir)"] + available
        for preset in missing:
            row = QHBoxLayout()
            lbl_p = QLabel(preset)
            lbl_p.setFixedWidth(170)
            lbl_p.setStyleSheet("color: #f59e0b; font-weight: bold;")
            combo = QComboBox()
            combo.addItems(options)
            guess = self._best_guess(preset, available)
            if guess:
                combo.setCurrentText(guess)
            self._combos[preset] = combo
            row.addWidget(lbl_p)
            row.addWidget(QLabel("→"))
            row.addWidget(combo, stretch=1)
            layout.addLayout(row)

        layout.addWidget(hline())
        btns = QHBoxLayout()
        btn_ok = QPushButton("Aplicar selección")
        btn_ok.setObjectName("success")
        btn_ok.clicked.connect(self._confirm)
        btn_cancel = QPushButton("Cancelar")
        btn_cancel.clicked.connect(self.reject)
        btns.addStretch()
        btns.addWidget(btn_cancel)
        btns.addWidget(btn_ok)
        layout.addLayout(btns)

    def _best_guess(self, preset: str, available: list) -> str | None:
        p = preset.lower()
        for a in available:
            if p in a.lower() or a.lower() in p:
                return a
        return None

    def _confirm(self):
        self.result_map = {
            preset: (None if combo.currentText() == "(omitir)" else combo.currentText())
            for preset, combo in self._combos.items()
        }
        self.cancelled = False
        self.accept()

# ── Panel izquierdo: Columnas ─────────────────────────────────────────────────

class ColumnPanel(QWidget):
    columns_changed = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedWidth(270)
        self.setObjectName("ColumnPanel")
        self._checks: dict[str, QCheckBox] = {}

        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(6)

        title = QLabel("Columnas de salida")
        title.setFont(QFont("Segoe UI", 11, QFont.Weight.Bold))
        layout.addWidget(title)

        # Quick buttons
        btn_row = QHBoxLayout()
        self.btn_all    = QPushButton("✓ Todas")
        self.btn_preset = QPushButton("⭐ Preset")
        for b in (self.btn_all, self.btn_preset):
            b.setFixedHeight(28)
        self.btn_all.clicked.connect(self._select_all)
        self.btn_preset.clicked.connect(self._apply_preset)
        btn_row.addWidget(self.btn_all)
        btn_row.addWidget(self.btn_preset)
        layout.addLayout(btn_row)

        # Scroll de checkboxes
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        self._scroll_widget = QWidget()
        self._scroll_layout = QVBoxLayout(self._scroll_widget)
        self._scroll_layout.setContentsMargins(4, 4, 4, 4)
        self._scroll_layout.setSpacing(3)
        self._scroll_layout.addStretch()
        scroll.setWidget(self._scroll_widget)
        layout.addWidget(scroll, stretch=1)

        self._rename_map: dict[str, str] = {}

    def set_columns(self, columns: list, rename_map: dict = None):
        self._rename_map = rename_map or {}
        # limpiar
        while self._scroll_layout.count() > 1:
            item = self._scroll_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        self._checks.clear()
        for col in columns:
            label = self._rename_map.get(col, col)
            display = f"{label}  →  {col}" if label != col else col
            cb = QCheckBox(display)
            cb.setChecked(True)
            cb.stateChanged.connect(lambda _: self.columns_changed.emit())
            self._checks[col] = cb
            self._scroll_layout.insertWidget(self._scroll_layout.count() - 1, cb)

    def get_selected(self) -> list:
        return [col for col, cb in self._checks.items() if cb.isChecked()]

    def get_rename_map(self) -> dict:
        return {col: self._rename_map[col] for col in self.get_selected() if col in self._rename_map}

    def _select_all(self):
        for cb in self._checks.values(): cb.setChecked(True)

    def _select_none(self):
        for cb in self._checks.values(): cb.setChecked(False)

    def _apply_preset(self):
        for col, cb in self._checks.items():
            mapped = self._rename_map.get(col, col)
            cb.setChecked(mapped in PRESET_COLUMNS or col in PRESET_COLUMNS)

# ── Panel izquierdo: Archivos de búsqueda ────────────────────────────────────

class SearchFilesPanel(QWidget):
    files_changed = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedWidth(270)
        self.setObjectName("SearchFilesPanel")
        self._files: list[str] = []

        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(6)

        title = QLabel("Archivos CSV")
        title.setFont(QFont("Segoe UI", 11, QFont.Weight.Bold))
        layout.addWidget(title)

        btn_row = QHBoxLayout()
        self.btn_add   = QPushButton("➕ Agregar")
        self.btn_clear = QPushButton("🗑 Limpiar")
        self.btn_add.setFixedHeight(28)
        self.btn_clear.setFixedHeight(28)
        self.btn_add.clicked.connect(self._add_files)
        self.btn_clear.clicked.connect(self._clear)
        btn_row.addWidget(self.btn_add)
        btn_row.addWidget(self.btn_clear)
        layout.addLayout(btn_row)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        self._list_widget = QWidget()
        self._list_layout = QVBoxLayout(self._list_widget)
        self._list_layout.setContentsMargins(4, 4, 4, 4)
        self._list_layout.setSpacing(4)
        self._list_layout.addStretch()
        scroll.setWidget(self._list_widget)
        layout.addWidget(scroll, stretch=1)

    def get_files(self) -> list: return list(self._files)

    def _add_files(self):
        paths, _ = QFileDialog.getOpenFileNames(self, "Agregar CSVs", "", "CSV (*.csv);;Todos (*)")
        for p in paths:
            if p not in self._files:
                self._files.append(p)
        self._render()
        self.files_changed.emit()

    def _clear(self):
        self._files.clear()
        self._render()
        self.files_changed.emit()

    def _remove(self, path: str):
        self._files = [f for f in self._files if f != path]
        self._render()
        self.files_changed.emit()

    def _render(self):
        while self._list_layout.count() > 1:
            item = self._list_layout.takeAt(0)
            if item.widget(): item.widget().deleteLater()
        for fp in self._files:
            row_w = QWidget()
            row_h = QHBoxLayout(row_w)
            row_h.setContentsMargins(0, 0, 0, 0)
            lbl = QLabel(Path(fp).name)
            lbl.setToolTip(fp)
            lbl.setWordWrap(False)
            btn_x = QPushButton("✕")
            btn_x.setFixedSize(22, 22)
            btn_x.clicked.connect(lambda _, f=fp: self._remove(f))
            row_h.addWidget(lbl, stretch=1)
            row_h.addWidget(btn_x)
            self._list_layout.insertWidget(self._list_layout.count() - 1, row_w)

# ── Tab: Exportar CSV ─────────────────────────────────────────────────────────

class ExportCSVTab(QWidget):
    request_columns = pyqtSignal()   # pide columnas al MainWindow

    def __init__(self, parent=None):
        super().__init__(parent)
        self._worker: CSVExportWorker | None = None
        self._last_out_dir: str = ""
        self._df_full: pd.DataFrame | None = None   # preview completo (hasta PREVIEW_ROWS)
        self._page: int = 0
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(6)

        # Barra de paginación
        page_bar = QHBoxLayout()
        self.btn_first = QPushButton("|◀"); self.btn_first.setFixedSize(38, 28)
        self.btn_prev  = QPushButton("◀");  self.btn_prev.setFixedSize(38, 28)
        self.btn_next  = QPushButton("▶");  self.btn_next.setFixedSize(38, 28)
        self.btn_last  = QPushButton("▶|"); self.btn_last.setFixedSize(38, 28)
        for b in (self.btn_first, self.btn_prev, self.btn_next, self.btn_last):
            b.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        self.lbl_page  = QLabel("—")
        self.lbl_page.setFont(QFont("Segoe UI", 9))
        self.lbl_page.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_page.setMinimumWidth(160)
        for b in (self.btn_first, self.btn_prev, self.btn_next, self.btn_last):
            b.setEnabled(False)
        self.btn_first.clicked.connect(lambda: self._go_page(0))
        self.btn_prev.clicked.connect(lambda: self._go_page(self._page - 1))
        self.btn_next.clicked.connect(lambda: self._go_page(self._page + 1))
        self.btn_last.clicked.connect(lambda: self._go_page(self._total_pages() - 1))
        page_bar.addStretch()
        page_bar.addWidget(self.btn_first)
        page_bar.addWidget(self.btn_prev)
        page_bar.addWidget(self.lbl_page)
        page_bar.addWidget(self.btn_next)
        page_bar.addWidget(self.btn_last)
        page_bar.addStretch()
        layout.addLayout(page_bar)

        # Grilla de preview
        self.table = DataTable()
        layout.addWidget(self.table, stretch=1)

        layout.addWidget(hline())

        # Carpeta + delimitador
        opts = QHBoxLayout()
        opts.addWidget(QLabel("📁 Salida:"))
        self.entry_out = QLineEdit()
        self.entry_out.setPlaceholderText("Carpeta de salida...")
        self.entry_out.setText("")
        opts.addWidget(self.entry_out, stretch=1)
        opts.addSpacing(16)

        opts.addWidget(QLabel("Delimitador:"))
        self._delim_group = QButtonGroup(self)
        for label, val in [("Coma", "comma"), ("Punto y coma", "semicolon"), ("Tab", "tab")]:
            rb = QRadioButton(label)
            rb.setProperty("delim_val", val)
            self._delim_group.addButton(rb)
            opts.addWidget(rb)
            if val == "comma": rb.setChecked(True)
        layout.addLayout(opts)

        # Botones de acción
        actions = QHBoxLayout()
        self.btn_process = QPushButton("▶  PROCESAR")
        self.btn_process.setObjectName("success")
        self.btn_process.setFixedHeight(38)
        self.btn_cancel  = QPushButton("✕  Cancelar")
        self.btn_cancel.setObjectName("danger")
        self.btn_cancel.setFixedHeight(38)
        self.btn_cancel.setEnabled(False)
        self.btn_open    = QPushButton("📂  Abrir carpeta")
        self.btn_open.setFixedHeight(38)
        self.btn_open.setEnabled(False)
        self.btn_process.clicked.connect(self._start)
        self.btn_cancel.clicked.connect(self._cancel)
        self.btn_open.clicked.connect(self._open_folder)
        actions.addWidget(self.btn_process)
        actions.addWidget(self.btn_cancel)
        actions.addWidget(self.btn_open)
        actions.addStretch()
        layout.addLayout(actions)

        self.progress = ProgressRow()
        layout.addWidget(self.progress)

        self.lbl_status = QLabel("")
        self.lbl_status.setWordWrap(True)
        layout.addWidget(self.lbl_status)

        # contexto del archivo (seteado por MainWindow)
        self._filepath = ""
        self._enc      = ""
        self._delim    = ""
        self._get_columns_fn   = None
        self._get_rename_fn    = None

    def setup(self, filepath, enc, delim, get_columns_fn, get_rename_fn):
        self._filepath       = filepath
        self.entry_out.setText(str(Path(filepath).parent / "csv_output"))
        self._enc            = enc
        self._delim          = delim
        self._get_columns_fn  = get_columns_fn
        self._get_rename_fn   = get_rename_fn

    def set_preview(self, df: pd.DataFrame, palette: dict):
        self._df_full = df
        self._page    = 0
        self._render_page(palette)

    def _total_pages(self) -> int:
        if self._df_full is None or len(self._df_full) == 0: return 1
        return max(1, -(-len(self._df_full) // PAGE_SIZE))  # ceil division

    def _go_page(self, page: int):
        self._page = max(0, min(page, self._total_pages() - 1))
        self._render_page()

    def _render_page(self, palette: dict = None):
        if self._df_full is None: return
        p     = palette or getattr(self, "_palette", DARK)
        if palette: self._palette = palette
        start = self._page * PAGE_SIZE
        end   = start + PAGE_SIZE
        page_df = self._df_full.iloc[start:end]
        model = PandasTableModel(page_df, p)
        self.table.set_model(model)
        self.table.apply_palette(p)
        # actualizar barra
        total     = self._total_pages()
        row_start = start + 1
        row_end   = min(end, len(self._df_full))
        total_rows = len(self._df_full)
        suffix = f" (preview {total_rows:,})" if total_rows >= PREVIEW_ROWS else f" de {total_rows:,}"
        self.lbl_page.setText(f"Pág. {self._page + 1} / {total}  ·  filas {row_start}–{row_end}{suffix}")
        self.btn_first.setEnabled(self._page > 0)
        self.btn_prev.setEnabled(self._page > 0)
        self.btn_next.setEnabled(self._page < total - 1)
        self.btn_last.setEnabled(self._page < total - 1)

    def _browse_out(self):
        d = QFileDialog.getExistingDirectory(self, "Carpeta de salida", self.entry_out.text())
        if d: self.entry_out.setText(d)

    _DELIM_MAP = {"comma": ",", "semicolon": ";", "tab": "\t"}

    def _get_delim(self) -> str:
        for btn in self._delim_group.buttons():
            if btn.isChecked():
                return self._DELIM_MAP.get(btn.property("delim_val"), ",")
        return ","

    def _start(self):
        if not self._filepath:
            QMessageBox.warning(self, "Sin archivo", "Cargá un archivo CSV primero.")
            return
        columns = self._get_columns_fn() if self._get_columns_fn else []
        if not columns:
            QMessageBox.warning(self, "Sin columnas", "Seleccioná al menos una columna.")
            return
        out_dir = self.entry_out.text().strip()
        if not out_dir:
            QMessageBox.warning(self, "Sin carpeta", "Ingresá una carpeta de salida.")
            return
        rename_map = self._get_rename_fn() if self._get_rename_fn else {}
        self._worker = CSVExportWorker(
            self._filepath, self._enc, self._delim,
            columns, {}, out_dir, self._get_delim(), rename_map
        )
        self._worker.progress.connect(self._on_progress)
        self._worker.done.connect(self._on_done)
        self._worker.error.connect(self._on_error)
        self.btn_process.setEnabled(False)
        self.btn_cancel.setEnabled(False)
        self.btn_open.setEnabled(False)
        self.progress.reset()
        self._set_status("Procesando…", "muted")
        QApplication.setOverrideCursor(QCursor(Qt.CursorShape.WaitCursor))
        self._worker.start()

    def _cancel(self):
        pass  # process_csv no soporta cancelación

    def _on_progress(self, pct, msg):
        self.progress.set(pct)
        self._set_status(msg, "muted")

    def _on_done(self, out_dir):
        self._last_out_dir = out_dir
        self._finish(f"✓ Exportado en: {out_dir}", "success")
        self.btn_open.setEnabled(True)

    def _on_error(self, msg):
        self._finish(f"✗ Error: {msg}", "error")

    def _finish(self, msg, state):
        self.btn_process.setEnabled(True)
        self._set_status(msg, state)
        QApplication.restoreOverrideCursor()

    def _set_status(self, msg, state):
        colors = {"success": "#22c55e", "error": "#ef4444", "warning": "#f59e0b", "muted": "gray"}
        c = colors.get(state, "gray")
        self.lbl_status.setText(msg)
        self.lbl_status.setStyleSheet(f"color: {c};")

    def _open_folder(self):
        if self._last_out_dir and os.path.isdir(self._last_out_dir):
            os.startfile(self._last_out_dir)

    def apply_palette(self, p: dict):
        self.table.apply_palette(p)

# ── Tab: Exportar TXT ─────────────────────────────────────────────────────────

class ExportTXTTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._worker: GenericWorker | None = None
        self._filepath = self._enc = self._delim = ""
        self._last_dir     = ""
        self._df_preview   = None
        self._created_files: list = []

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)

        # Columna
        row1 = QHBoxLayout()
        row1.addWidget(QLabel("Columna:"))
        self.combo_col = QComboBox(); self.combo_col.setMinimumWidth(200)
        self.combo_col.currentTextChanged.connect(self._update_preview)
        row1.addWidget(self.combo_col)
        row1.addSpacing(24)
        row1.addWidget(QLabel("Formato:"))
        self._fmt_group = QButtonGroup(self)
        for label, val in [("Valor solo", "plain"), ("'valor', (SQL)", "quoted")]:
            rb = QRadioButton(label)
            rb.setProperty("fmt_val", val)
            self._fmt_group.addButton(rb)
            row1.addWidget(rb)
            if val == "plain": rb.setChecked(True)
        self._fmt_group.buttonClicked.connect(lambda _: self._update_preview())
        row1.addStretch()
        layout.addLayout(row1)

        self.txt_preview = QTextEdit()
        self.txt_preview.setReadOnly(True)
        self.txt_preview.setFont(QFont("Consolas", 10))
        self.txt_preview.setFixedHeight(220)
        self.txt_preview.setPlaceholderText("Vista previa del formato…")
        layout.addWidget(self.txt_preview)

        self.lbl_files_info = QLabel("")
        self.lbl_files_info.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        self.lbl_files_info.setWordWrap(True)
        layout.addWidget(self.lbl_files_info)

        layout.addWidget(hline())

        # Agrupación
        row2 = QHBoxLayout()
        row2.addWidget(QLabel("Agrupación:"))
        self._grp_group = QButtonGroup(self)
        self.rb_no_grp = QRadioButton("Sin agrupar"); self.rb_no_grp.setChecked(True)
        self.rb_grp    = QRadioButton("Agrupar por:")
        for rb in (self.rb_no_grp, self.rb_grp):
            self._grp_group.addButton(rb)
            row2.addWidget(rb)
        self.combo_grp = QComboBox(); self.combo_grp.setMinimumWidth(150); self.combo_grp.setEnabled(False)
        self.rb_grp.toggled.connect(self.combo_grp.setEnabled)
        self.rb_grp.toggled.connect(lambda _: self._update_files_info())
        self.rb_no_grp.toggled.connect(lambda _: self._update_files_info())
        self.combo_grp.currentTextChanged.connect(lambda _: self._update_files_info())
        row2.addWidget(self.combo_grp)
        row2.addStretch()
        layout.addLayout(row2)

        # Destino
        dest_row = QHBoxLayout()
        dest_row.addWidget(QLabel("📁 Destino:"))
        self.lbl_dest = QLabel("—")
        self.lbl_dest.setStyleSheet("color: gray;")
        dest_row.addWidget(self.lbl_dest, stretch=1)
        layout.addLayout(dest_row)

        # Botones
        actions = QHBoxLayout()
        self.btn_export = QPushButton("📄  Exportar TXT")
        self.btn_export.setObjectName("success")
        self.btn_export.setFixedHeight(38)
        self.btn_cancel = QPushButton("✕  Cancelar")
        self.btn_cancel.setObjectName("danger")
        self.btn_cancel.setFixedHeight(38)
        self.btn_cancel.setEnabled(False)
        self.btn_open_dest = QPushButton("📂  Abrir carpeta")
        self.btn_open_dest.setFixedHeight(38)
        self.btn_open_dest.setEnabled(False)
        self.btn_export.clicked.connect(self._start)
        self.btn_cancel.clicked.connect(self._cancel)
        self.btn_open_dest.clicked.connect(self._open_dest)
        actions.addWidget(self.btn_export)
        actions.addWidget(self.btn_cancel)
        actions.addWidget(self.btn_open_dest)
        actions.addStretch()
        layout.addLayout(actions)

        self.progress = ProgressRow()
        layout.addWidget(self.progress)
        self.lbl_status = QLabel("")
        self.lbl_status.setWordWrap(True)
        layout.addWidget(self.lbl_status)
        layout.addStretch()

    # Candidatos para columna y agrupación por defecto
    _SN_CANDIDATES  = ["SN", "STR_PSN_1", "SN_1", "PSN", "SERIAL"]
    _GRP_CANDIDATES = ["PHONEMODEL_NAME", "PHONE_MODEL", "MODEL_NAME", "MODEL", "PHONEMODEL"]

    def setup(self, filepath, enc, delim, columns, rename_map: dict = None, df=None):
        self._filepath   = filepath
        self._enc        = enc
        self._delim      = delim
        self._df_preview = df
        rename_map       = rename_map or {}
        # rename_map = {csv_col: preset_name}
        # invertido para buscar: {preset_name: csv_col}
        preset_to_col = {v: k for k, v in rename_map.items()}

        self.combo_col.blockSignals(True)
        self.combo_grp.blockSignals(True)
        self.combo_col.clear(); self.combo_col.addItems(columns)
        self.combo_grp.clear(); self.combo_grp.addItems(columns)

        # Auto-seleccionar columna SN
        sn_col = self._find_col(columns, self._SN_CANDIDATES, preset_to_col)
        if sn_col:
            self.combo_col.setCurrentText(sn_col)

        # Auto-seleccionar agrupación por PHONEMODEL_NAME
        grp_col = self._find_col(columns, self._GRP_CANDIDATES, preset_to_col)
        if grp_col:
            self.combo_grp.setCurrentText(grp_col)
            self.rb_grp.setChecked(True)
            self.combo_grp.setEnabled(True)
        else:
            self.rb_no_grp.setChecked(True)
            self.combo_grp.setEnabled(False)

        self.combo_col.blockSignals(False)
        self.combo_grp.blockSignals(False)
        self._update_dest()
        self._update_preview()
        self._update_files_info()

    def _find_col(self, columns: list, candidates: list, preset_to_col: dict) -> str | None:
        """Busca la primera columna que coincida con los candidatos (por nombre directo o por mapeo)."""
        for c in candidates:
            if c in columns:
                return c
            if c in preset_to_col and preset_to_col[c] in columns:
                return preset_to_col[c]
        return None

    def _get_fmt(self):
        for b in self._fmt_group.buttons():
            if b.isChecked(): return b.property("fmt_val")
        return "plain"

    def _update_preview(self):
        col = self.combo_col.currentText()
        fmt = self._get_fmt()
        # Tomar hasta 8 valores únicos reales del df si está disponible
        samples = []
        if self._df_preview is not None and col in self._df_preview.columns:
            samples = self._df_preview[col].dropna().unique().tolist()[:15]
        if not samples:
            samples = [f"VALOR_{i+1}" for i in range(12)]
        if fmt == "quoted":
            lines = "\n".join(f"'{v}'," for v in samples)
        else:
            lines = "\n".join(str(v) for v in samples)
        self.txt_preview.setPlainText(lines)

    def _update_files_info(self):
        if not self.rb_grp.isChecked():
            self.lbl_files_info.setText("📄 Se generará un solo archivo .txt")
            self.lbl_files_info.setStyleSheet("color: gray; padding: 2px 0;")
        else:
            self.lbl_files_info.setText("")
        self._created_files: list = []

    def _add_created_file(self, name: str):
        """Llamado durante exportación cada vez que se crea un nuevo archivo."""
        if name in self._created_files:
            return
        self._created_files.append(name)
        total    = len(self._created_files)
        MAX_SHOW = 5
        shown    = " · ".join(self._created_files[:MAX_SHOW])
        suffix   = f"  …+{total - MAX_SHOW} más" if total > MAX_SHOW else ""
        self.lbl_files_info.setText(f"📂 {total} archivo(s):  {shown}{suffix}")
        self.lbl_files_info.setStyleSheet(
            "color: #4ade80; padding: 2px 0;" if self._is_dark()
            else "color: #166534; padding: 2px 0;"
        )

    def _is_dark(self) -> bool:
        app = QApplication.instance()
        return bool(app and app.property("dark_mode"))

    def _update_dest(self):
        if self._filepath:
            base = Path(self._filepath).parent / "txt_output"
            self.lbl_dest.setText(str(base))
            self._last_dir = str(base)
        else:
            self.lbl_dest.setText("—")

    def _start(self):
        if not self._filepath:
            QMessageBox.warning(self, "Sin archivo", "Cargá un archivo CSV primero."); return
        col   = self.combo_col.currentText()
        fmt   = self._get_fmt()
        grp   = self.combo_grp.currentText() if self.rb_grp.isChecked() else None
        out   = str(Path(self._filepath).parent / "txt_output")

        def export_fn(fp, enc, delim, col, fmt, grp, out, cancel_fn, progress_cb):
            import csv as _csv
            Path(out).mkdir(parents=True, exist_ok=True)

            def fmt_val(v):
                return f"'{v}',\n" if fmt == "quoted" else f"{v}\n"

            if grp:
                handles: dict = {}
                seen: dict    = {}   # {grupo: set de valores ya escritos}
                try:
                    with open(fp, encoding=enc, newline="") as f:
                        reader = _csv.DictReader(f, delimiter=delim)
                        for i, row in enumerate(reader):
                            if cancel_fn(): return None
                            k     = sanitize_filename(row.get(grp, "sin_grupo"))
                            value = row.get(col, "")
                            if k not in handles:
                                handles[k] = open(Path(out) / f"{k}.txt", "w", encoding="utf-8")
                                seen[k]    = set()
                                progress_cb(0, f"FILE:{k}")
                            if value not in seen[k]:
                                handles[k].write(fmt_val(value))
                                seen[k].add(value)
                            if i % 1000 == 0:
                                progress_cb(i % 100, f"Procesando fila {i:,}…")
                finally:
                    for h in handles.values(): h.close()
                progress_cb(100, f"✓ {len(handles)} archivo(s) generado(s)")
            else:
                fpath = Path(out) / f"{sanitize_filename(col)}.txt"
                seen  = set()
                with open(fp, encoding=enc, newline="") as fin, \
                     open(fpath, "w", encoding="utf-8") as fout:
                    reader = _csv.DictReader(fin, delimiter=delim)
                    for i, row in enumerate(reader):
                        if cancel_fn(): return None
                        value = row.get(col, "")
                        if value not in seen:
                            fout.write(fmt_val(value))
                            seen.add(value)
                        if i % 1000 == 0:
                            progress_cb(i % 100, f"Procesando fila {i:,}…")
                progress_cb(100, "✓ Archivo generado")
            return out

        self._update_files_info()   # resetea el label antes de exportar
        self._worker = GenericWorker(export_fn, self._filepath, self._enc, self._delim, col, fmt, grp, out)
        def _on_progress(p, m):
            if m.startswith("FILE:"):
                self._add_created_file(m[5:])
            else:
                self.progress.set(p)
        self._worker.progress.connect(_on_progress)
        self._worker.done.connect(lambda d: self._finish(f"✓ Exportado en: {d}", "success", d))
        self._worker.error.connect(lambda e: self._finish(f"✗ {e}", "error", ""))
        self._worker.cancelled.connect(lambda: self._finish("⚠ Cancelado.", "warning", ""))
        self.btn_export.setEnabled(False); self.btn_cancel.setEnabled(True)
        self.progress.reset()
        self._worker.start()

    def _cancel(self):
        if self._worker: self._worker.cancel()

    def _finish(self, msg, state, out_dir):
        self.btn_export.setEnabled(True); self.btn_cancel.setEnabled(False)
        colors = {"success": "#22c55e", "error": "#ef4444", "warning": "#f59e0b"}
        self.lbl_status.setText(msg)
        self.lbl_status.setStyleSheet(f"color: {colors.get(state, 'gray')};")
        if out_dir: self._last_dir = out_dir; self.btn_open_dest.setEnabled(True)

    def _open_dest(self):
        if self._last_dir and os.path.isdir(self._last_dir): os.startfile(self._last_dir)

# ── Tab: Exportar JSON ────────────────────────────────────────────────────────

class ExportJSONTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._worker: GenericWorker | None = None
        self._filepath = self._enc = self._delim = ""
        self._last_dir = ""
        self._get_selected_cols_fn = None
        self._all_cols: list = []
        self._created_files: list = []
        self._df = None

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)

        row1 = QHBoxLayout()
        row1.addWidget(QLabel("Carpetas por:"))
        self.combo_folder = QComboBox(); self.combo_folder.setMinimumWidth(180)
        self.combo_folder.currentTextChanged.connect(self._update_preview)
        row1.addWidget(self.combo_folder)
        row1.addSpacing(20)
        row1.addWidget(QLabel("Archivos (JSON) por:"))
        self.combo_file = QComboBox(); self.combo_file.setMinimumWidth(180)
        self.combo_file.currentTextChanged.connect(self._update_preview)
        row1.addWidget(self.combo_file)
        row1.addStretch()
        layout.addLayout(row1)

        layout.addWidget(hline())

        lbl_struct = QLabel("Estructura:  JSON / <carpeta> / <archivo>.json  →  [ {…}, … ]")
        lbl_struct.setFont(QFont("Consolas", 9))
        lbl_struct.setStyleSheet("color: gray;")
        layout.addWidget(lbl_struct)

        self.txt_preview = QTextEdit()
        self.txt_preview.setReadOnly(True)
        self.txt_preview.setFont(QFont("Consolas", 9))
        self.txt_preview.setFixedHeight(220)
        self.txt_preview.setPlaceholderText("Vista previa del formato JSON…")
        layout.addWidget(self.txt_preview)

        self.lbl_files_info = QLabel("")
        self.lbl_files_info.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        self.lbl_files_info.setWordWrap(True)
        layout.addWidget(self.lbl_files_info)

        layout.addWidget(hline())

        dest_row = QHBoxLayout()
        dest_row.addWidget(QLabel("📁 Destino:"))
        self.lbl_dest = QLabel("—"); self.lbl_dest.setStyleSheet("color: gray;")
        dest_row.addWidget(self.lbl_dest, stretch=1)
        layout.addLayout(dest_row)

        actions = QHBoxLayout()
        self.btn_export = QPushButton("🗂  Exportar JSON")
        self.btn_export.setObjectName("success"); self.btn_export.setFixedHeight(38)
        self.btn_cancel = QPushButton("✕  Cancelar")
        self.btn_cancel.setObjectName("danger"); self.btn_cancel.setFixedHeight(38); self.btn_cancel.setEnabled(False)
        self.btn_open_dest = QPushButton("📂  Abrir carpeta")
        self.btn_open_dest.setFixedHeight(38); self.btn_open_dest.setEnabled(False)
        self.btn_export.clicked.connect(self._start)
        self.btn_cancel.clicked.connect(self._cancel)
        self.btn_open_dest.clicked.connect(self._open_dest)
        actions.addWidget(self.btn_export); actions.addWidget(self.btn_cancel)
        actions.addWidget(self.btn_open_dest); actions.addStretch()
        layout.addLayout(actions)

        self.progress = ProgressRow(); layout.addWidget(self.progress)
        self.lbl_status = QLabel(""); self.lbl_status.setWordWrap(True); layout.addWidget(self.lbl_status)
        layout.addStretch()

    _FOLDER_CANDIDATES = ["PHONEMODEL_NAME", "PHONE_MODEL", "MODEL_NAME", "MODEL", "PHONEMODEL"]
    _FILE_CANDIDATES   = ["SN", "STR_PSN_1", "SN_1", "PSN", "SERIAL"]

    def setup(self, filepath, enc, delim, columns, get_selected_fn, rename_map: dict = None, df=None):
        self._filepath             = filepath
        self._enc                  = enc
        self._delim                = delim
        self._all_cols             = columns
        self._get_selected_cols_fn = get_selected_fn
        rename_map = rename_map or {}
        preset_to_col = {v: k for k, v in rename_map.items()}

        for combo in (self.combo_folder, self.combo_file):
            combo.blockSignals(True); combo.clear(); combo.addItems(columns); combo.blockSignals(False)

        folder_col = self._find_col(columns, self._FOLDER_CANDIDATES, preset_to_col)
        if folder_col:
            self.combo_folder.setCurrentText(folder_col)

        file_col = self._find_col(columns, self._FILE_CANDIDATES, preset_to_col)
        if file_col:
            self.combo_file.setCurrentText(file_col)

        self._df = df
        dest = str(Path(filepath).parent / "json_output") if filepath else "—"
        self.lbl_dest.setText(dest); self._last_dir = dest
        self._created_files = []
        self.lbl_files_info.setText("")
        self._update_preview()

    def _find_col(self, columns: list, candidates: list, preset_to_col: dict) -> str | None:
        for c in candidates:
            if c in columns:
                return c
            if c in preset_to_col and preset_to_col[c] in columns:
                return preset_to_col[c]
        return None

    def _update_preview(self):
        import json as _json
        folder_col = self.combo_folder.currentText()
        file_col   = self.combo_file.currentText()
        cols = self._get_selected_cols_fn() if self._get_selected_cols_fn else self._all_cols

        # Tomar hasta 3 filas reales del df agrupadas por file_col
        sample_rows = []
        if self._df is not None and len(self._df) > 0:
            grp_col = file_col if file_col and file_col in self._df.columns else None
            if grp_col:
                # Primera SN con hasta 3 filas
                first_val = self._df[grp_col].dropna().iloc[0] if len(self._df) else None
                if first_val is not None:
                    subset = self._df[self._df[grp_col] == first_val].head(3)
                    for _, row in subset.iterrows():
                        obj = {}
                        for c in cols:
                            if c in self._df.columns:
                                obj[PRESET_TO_JSON_KEY.get(c, c)] = str(row.get(c, ""))
                        if obj:
                            sample_rows.append(obj)
            if not sample_rows:
                for _, row in self._df.head(3).iterrows():
                    obj = {}
                    for c in cols:
                        if c in self._df.columns:
                            obj[PRESET_TO_JSON_KEY.get(c, c)] = str(row.get(c, ""))
                    if obj:
                        sample_rows.append(obj)

        if not sample_rows:
            # Datos de ejemplo estáticos
            sample_rows = [
                {PRESET_TO_JSON_KEY.get(c, c): f"VALOR_{c}_{i+1}" for c in (cols or list(PRESET_TO_JSON_KEY.keys()))}
                for i in range(3)
            ]

        preview = _json.dumps(sample_rows, ensure_ascii=False, indent=2)
        self.txt_preview.setPlainText(preview)

    def _is_dark(self) -> bool:
        app = QApplication.instance()
        return bool(app and app.property("dark_mode"))

    def _add_created_file(self, name: str):
        if name in self._created_files:
            return
        self._created_files.append(name)
        total    = len(self._created_files)
        MAX_SHOW = 5
        shown    = " · ".join(self._created_files[:MAX_SHOW])
        suffix   = f"  …+{total - MAX_SHOW} más" if total > MAX_SHOW else ""
        self.lbl_files_info.setText(f"📂 {total} carpeta(s):  {shown}{suffix}")
        self.lbl_files_info.setStyleSheet(
            "color: #4ade80; padding: 2px 0;" if self._is_dark()
            else "color: #166534; padding: 2px 0;"
        )

    def _start(self):
        if not self._filepath:
            QMessageBox.warning(self, "Sin archivo", "Cargá un archivo CSV primero."); return
        folder_col = self.combo_folder.currentText()
        file_col   = self.combo_file.currentText()
        if not folder_col:
            QMessageBox.warning(self, "Sin columna", "Seleccioná la columna para carpetas."); return
        columns = self._get_selected_cols_fn() if self._get_selected_cols_fn else self._all_cols
        out = str(Path(self._filepath).parent / "json_output")

        def export_fn(fp, enc, delim, fcol, filecol, cols, out, cancel_fn, progress_cb):
            import csv as _csv, json as _json

            # Streaming: buffer rows per (folder, file) group
            groups: dict = {}   # {folder_val: {file_val: [rows]}}
            seen_folders: set = set()

            with open(fp, encoding=enc, newline="") as f:
                reader = _csv.DictReader(f, delimiter=delim)
                for i, row in enumerate(reader):
                    if cancel_fn(): return None
                    folder_val = row.get(fcol, "sin_grupo")
                    file_val   = row.get(filecol, "sin_archivo")
                    if folder_val not in groups:
                        groups[folder_val] = {}
                        if folder_val not in seen_folders:
                            seen_folders.add(folder_val)
                            progress_cb(0, f"FILE:{sanitize_filename(folder_val)}")
                    if file_val not in groups[folder_val]:
                        groups[folder_val][file_val] = []
                    groups[folder_val][file_val].append({c: row.get(c, "") for c in cols})
                    if i % 1000 == 0:
                        progress_cb(i % 80, f"Procesando fila {i:,}…")

            total_rows = sum(len(rows) for fg in groups.values() for rows in fg.values())
            done = 0
            Path(out).mkdir(parents=True, exist_ok=True)
            for folder_val, file_groups in groups.items():
                if cancel_fn(): return None
                folder_path = Path(out) / sanitize_filename(folder_val)
                folder_path.mkdir(parents=True, exist_ok=True)
                for file_val, rows in file_groups.items():
                    if cancel_fn(): return None
                    fpath = folder_path / f"{sanitize_filename(file_val)}.json"
                    data = [{PRESET_TO_JSON_KEY.get(c, c): r.get(c, "") for c in cols} for r in rows]
                    with open(fpath, "w", encoding="utf-8") as f:
                        _json.dump(data, f, ensure_ascii=False, indent=2)
                    done += len(rows)
                    progress_cb(int(done / max(total_rows, 1) * 100), f"{folder_val}/{file_val}.json")
            return out

        self._created_files = []
        self.lbl_files_info.setText("")
        self._worker = GenericWorker(export_fn, self._filepath, self._enc, self._delim,
                                     folder_col, file_col, columns, out)

        def _on_progress(p, m):
            if m.startswith("FILE:"):
                self._add_created_file(m[5:])
            else:
                self.progress.set(p)

        self._worker.progress.connect(_on_progress)
        self._worker.done.connect(lambda d: self._finish(f"✓ Exportado en: {d}", "success", d))
        self._worker.error.connect(lambda e: self._finish(f"✗ {e}", "error", ""))
        self._worker.cancelled.connect(lambda: self._finish("⚠ Cancelado.", "warning", ""))
        self.btn_export.setEnabled(False); self.btn_cancel.setEnabled(True)
        self.progress.reset(); self._worker.start()

    def _cancel(self):
        if self._worker: self._worker.cancel()

    def _finish(self, msg, state, out_dir):
        self.btn_export.setEnabled(True); self.btn_cancel.setEnabled(False)
        colors = {"success": "#22c55e", "error": "#ef4444", "warning": "#f59e0b"}
        self.lbl_status.setText(msg); self.lbl_status.setStyleSheet(f"color: {colors.get(state,'gray')};")
        if out_dir: self._last_dir = out_dir; self.btn_open_dest.setEnabled(True)

    def _open_dest(self):
        if self._last_dir and os.path.isdir(self._last_dir): os.startfile(self._last_dir)

# ── Tab: Buscar ───────────────────────────────────────────────────────────────

class SearchTab(QWidget):
    def __init__(self, files_panel: "SearchFilesPanel", parent=None):
        super().__init__(parent)
        self._files_panel = files_panel
        self._files_panel.files_changed.connect(self._on_files_changed)
        self._worker: SearchWorker | None = None
        self._export_worker: GenericWorker | None = None
        self._results: list = []
        self._result_df: pd.DataFrame | None = None
        self._result_row_colors: list = []
        self._last_export_dir = ""
        self._all_detected_cols: list = []

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 10, 12, 10)
        layout.setSpacing(6)

        title = QLabel("Buscar en archivos CSV")
        title.setFont(QFont("Segoe UI", 13, QFont.Weight.Bold))
        layout.addWidget(title)

        input_cols = QHBoxLayout()
        input_cols.setSpacing(12)

        left_col = QVBoxLayout()
        left_col.setSpacing(4)
        left_col.addWidget(QLabel("Códigos a buscar:"))
        self.txt_input = QPlainTextEdit()
        self.txt_input.setFixedHeight(90)
        self.txt_input.setPlaceholderText("ej: ZY32MJ3LZH\nVarios separados por coma o Enter")
        left_col.addWidget(self.txt_input)
        QShortcut(QKeySequence("Ctrl+Return"), self.txt_input).activated.connect(self._start)

        right_col = QVBoxLayout()
        right_col.setSpacing(4)
        right_col.addWidget(QLabel("Códigos no encontrados:"))
        self.txt_not_found = QPlainTextEdit()
        self.txt_not_found.setFixedHeight(90)
        self.txt_not_found.setReadOnly(True)
        self.txt_not_found.setPlaceholderText("—")
        right_col.addWidget(self.txt_not_found)

        input_cols.addLayout(left_col)
        input_cols.addLayout(right_col)
        layout.addLayout(input_cols)

        ctrl_row = QHBoxLayout()
        ctrl_row.addWidget(QLabel("Columna:"))
        self.combo_col = QComboBox(); self.combo_col.setMinimumWidth(160)
        ctrl_row.addWidget(self.combo_col)
        ctrl_row.addSpacing(12)
        self.btn_search = QPushButton("🔍  Buscar"); self.btn_search.setObjectName("success"); self.btn_search.setFixedHeight(38)
        self.btn_cancel = QPushButton("✕  Cancelar"); self.btn_cancel.setObjectName("danger"); self.btn_cancel.setFixedHeight(38); self.btn_cancel.setEnabled(False)
        self.btn_clear  = QPushButton("🗑  Limpiar"); self.btn_clear.setFixedHeight(38)
        self.btn_search.clicked.connect(self._start)
        self.btn_cancel.clicked.connect(self._cancel_search)
        self.btn_clear.clicked.connect(self._clear)
        ctrl_row.addWidget(self.btn_search); ctrl_row.addWidget(self.btn_cancel); ctrl_row.addWidget(self.btn_clear); ctrl_row.addStretch()
        layout.addLayout(ctrl_row)

        # Filtro de columnas en resultados
        col_filter_row = QHBoxLayout()
        col_filter_row.addWidget(QLabel("Columnas en resultados:"))
        self._col_filter_group = QButtonGroup(self)
        self.rb_cols_preset = QRadioButton("Preseleccionadas"); self.rb_cols_preset.setChecked(True)
        self.rb_cols_all    = QRadioButton("Todas")
        for rb in (self.rb_cols_preset, self.rb_cols_all):
            self._col_filter_group.addButton(rb); col_filter_row.addWidget(rb)
        self.rb_cols_preset.clicked.connect(self._apply_col_filter)
        self.rb_cols_all.clicked.connect(self._apply_col_filter)
        col_filter_row.addStretch()
        layout.addLayout(col_filter_row)

        self.progress = ProgressRow(); layout.addWidget(self.progress)
        self.lbl_status = QLabel("Agregá archivos CSV en el panel izquierdo e ingresá un valor a buscar.")
        self.lbl_status.setWordWrap(True); self.lbl_status.setStyleSheet("color: gray;")
        layout.addWidget(self.lbl_status)

        self.table = DataTable()
        layout.addWidget(self.table, stretch=1)

        layout.addWidget(hline())

        # Export panel
        exp_lbl = QLabel("Exportar resultados")
        exp_lbl.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        layout.addWidget(exp_lbl)

        exp_row1 = QHBoxLayout()
        exp_row1.addWidget(QLabel("Carpetas por:"))
        self.combo_exp_folder = QComboBox(); self.combo_exp_folder.setMinimumWidth(150)
        exp_row1.addWidget(self.combo_exp_folder)
        exp_row1.addSpacing(16)
        exp_row1.addWidget(QLabel("Archivos por:"))
        self.combo_exp_file = QComboBox(); self.combo_exp_file.setMinimumWidth(150)
        exp_row1.addWidget(self.combo_exp_file)
        exp_row1.addStretch()
        layout.addLayout(exp_row1)

        exp_row2 = QHBoxLayout()
        self.btn_exp_json = QPushButton("💾  Exportar JSON"); self.btn_exp_json.setObjectName("success"); self.btn_exp_json.setFixedHeight(38)
        self.btn_exp_csv  = QPushButton("📄  Exportar CSV");  self.btn_exp_csv.setFixedHeight(38)
        self.btn_exp_open = QPushButton("📂  Abrir carpeta"); self.btn_exp_open.setFixedHeight(38); self.btn_exp_open.setEnabled(False)
        for b in (self.btn_exp_json, self.btn_exp_csv, self.btn_exp_open):
            exp_row2.addWidget(b)
        exp_row2.addStretch()
        self.btn_exp_json.clicked.connect(lambda: self._export("json"))
        self.btn_exp_csv.clicked.connect(lambda:  self._export("csv"))
        self.btn_exp_open.clicked.connect(self._open_export_dir)
        layout.addLayout(exp_row2)

        self.exp_progress = ProgressRow(); layout.addWidget(self.exp_progress)

    _SN_CANDIDATES = ["SN", "STR_PSN_1", "SN_1", "PSN", "SERIAL"]

    def set_columns(self, columns: list):
        self._all_detected_cols = columns
        self._refresh_col_combo()

    def _populate_col_combo(self, columns: list):
        self.combo_col.blockSignals(True)
        self.combo_col.clear()
        self.combo_col.addItems(columns)
        for c in self._SN_CANDIDATES:
            if c in columns:
                self.combo_col.setCurrentText(c)
                break
        self.combo_col.blockSignals(False)

    def _on_files_changed(self):
        """Actualiza el combo de columna con la unión de columnas de todos los archivos del panel."""
        files = self._files_panel.get_files()
        if not files:
            self._all_detected_cols = []
            self.combo_col.clear()
            return
        seen = set()
        all_cols = []
        for fp in files:
            try:
                enc   = detect_encoding(fp)
                delim = detect_delimiter(fp, enc)
                for c in get_columns(fp, enc, delim):
                    if c not in seen:
                        seen.add(c)
                        all_cols.append(c)
            except Exception:
                pass
        self._all_detected_cols = all_cols
        self._refresh_col_combo()

    # Todos los nombres conocidos que consideramos "preseleccionados" para buscar
    _PRESET_SEARCH_COLS = set(PRESET_COLUMNS) | {
        "STR_PSN_1", "SN_1", "PSN", "SERIAL",
        "PHONE_MODEL", "MODEL_NAME", "MODEL", "PHONEMODEL",
    }

    def _refresh_col_combo(self):
        """Filtra el combo según el radio activo: preset o todas."""
        if not self._all_detected_cols:
            return
        if self.rb_cols_preset.isChecked():
            cols = [c for c in self._all_detected_cols if c in self._PRESET_SEARCH_COLS]
            if not cols:
                cols = self._all_detected_cols
        else:
            cols = self._all_detected_cols
        self._populate_col_combo(cols)

    def _parse_values(self) -> list:
        raw = self.txt_input.toPlainText()
        vals = [v.strip() for v in raw.replace("\n", ",").split(",") if v.strip()]
        return vals

    def _start(self):
        files = self._files_panel.get_files()
        if not files:
            QMessageBox.warning(self, "Sin archivos", "Agregá archivos CSV en el panel izquierdo."); return
        values = self._parse_values()
        if not values:
            QMessageBox.warning(self, "Sin valor", "Ingresá al menos un valor a buscar."); return
        col = self.combo_col.currentText()
        self._last_searched = values
        self.txt_not_found.clear()
        self._worker = SearchWorker(files, values, col)
        self._worker.progress.connect(self._on_progress)
        self._worker.done.connect(self._on_done)
        self._worker.error.connect(self._on_error)
        self._worker.cancelled.connect(lambda: self._finish_search("⚠ Cancelado.", "warning"))
        self.btn_search.setEnabled(False); self.btn_cancel.setEnabled(True)
        self.progress.reset()
        self._set_status("Buscando…", "muted")
        QApplication.setOverrideCursor(QCursor(Qt.CursorShape.WaitCursor))
        self._worker.start()

    def _cancel_search(self):
        if self._worker: self._worker.cancel()

    def _on_progress(self, pct, msg):
        self.progress.set(pct); self._set_status(msg, "muted")

    def _on_done(self, results: list):
        self._results = results
        self._populate_table(results)
        count = len(results)
        self._finish_search(f"✓ {count} resultado(s) encontrado(s).", "success")
        self._show_not_found(results)
        QApplication.restoreOverrideCursor()

    def _show_not_found(self, results: list):
        searched = getattr(self, "_last_searched", [])
        if not searched:
            return
        col = self.combo_col.currentText()
        found = {str(r.get(col, "")).strip().lower() for r in results}
        not_found = [v for v in searched if v.strip().lower() not in found]
        p = DARK if QApplication.instance().property("dark_mode") else LIGHT
        if not_found:
            self.txt_not_found.setPlainText("\n".join(not_found))
            self.txt_not_found.setStyleSheet(
                f"color: {p['error']}; background-color: {p['surface']}; border: 1px solid {p['border']};"
            )
        else:
            self.txt_not_found.setPlainText("✓ Todos encontrados")
            self.txt_not_found.setStyleSheet(
                f"color: {p['success']}; background-color: {p['surface']}; border: 1px solid {p['border']};"
            )

    def _on_error(self, msg):
        self._finish_search(f"✗ Error: {msg}", "error")
        QApplication.restoreOverrideCursor()

    def _finish_search(self, msg, state):
        self.btn_search.setEnabled(True); self.btn_cancel.setEnabled(False)
        self._set_status(msg, state)
        QApplication.restoreOverrideCursor()

    # Orden deseado para "Preseleccionadas": cada sub-lista = candidatos para esa posición
    _PRESET_COL_ORDER = [
        ["PHONEMODEL_NAME", "PHONE_MODEL", "MODEL_NAME", "MODEL", "PHONEMODEL"],
        ["SN", "STR_PSN_1", "SN_1", "PSN", "SERIAL"],
        ["KEYUNITBARCODE"],
        ["CLASSCODE"],
        ["CREATETIME"],
        ["KEYMATERIAL"],
    ]
    _PRESET_RESULT_COLS = {c for group in _PRESET_COL_ORDER for c in group}

    def _populate_table(self, results: list):
        if not results:
            self._result_df = pd.DataFrame()
            self._result_row_colors = []
            return
        skip = {"__file__", "__filepath__", "__palette__"}
        data_cols = [k for k in results[0] if k not in skip]
        rows_data  = []
        row_colors = []
        for r in results:
            rows_data.append({c: r.get(c, "") for c in data_cols})
            pal_idx = r.get("__palette__", 0)
            row_colors.append(SEARCH_FILE_PALETTE[pal_idx % len(SEARCH_FILE_PALETTE)])

        df = pd.DataFrame(rows_data, columns=data_cols)
        df.insert(0, "Archivo", [r["__file__"] for r in results])

        self._result_df         = df
        self._result_row_colors = row_colors
        self._render_table()

    def _apply_col_filter(self):
        if self._result_df is not None and not self._result_df.empty:
            self._render_table()

    def _render_table(self):
        df = self._result_df
        if self.rb_cols_preset.isChecked():
            available = set(df.columns)
            ordered = []
            for candidates in self._PRESET_COL_ORDER:
                for c in candidates:
                    if c in available:
                        ordered.append(c)
                        break
            if not ordered:   # ningún preset en los resultados → mostrar todo
                pass
            else:
                df = df[["Archivo"] + ordered]

        palette = DARK if QApplication.instance().property("dark_mode") else LIGHT
        model = PandasTableModel(df, palette, self._result_row_colors)
        self.table.set_model(model)

        # Actualizar combos de exportación con auto-selección
        cols = df.columns.tolist()
        for combo in (self.combo_exp_folder, self.combo_exp_file):
            combo.blockSignals(True); combo.clear(); combo.addItems(cols); combo.blockSignals(False)
        _FOLDER = ["PHONEMODEL_NAME", "PHONE_MODEL", "MODEL_NAME", "MODEL", "PHONEMODEL"]
        _FILE   = ["SN", "STR_PSN_1", "SN_1", "PSN", "SERIAL"]
        for candidates, combo in ((_FOLDER, self.combo_exp_folder), (_FILE, self.combo_exp_file)):
            for c in candidates:
                if c in cols:
                    combo.setCurrentText(c); break

    def _clear(self):
        self._results = []
        self._result_df = None
        self.table.set_model(PandasTableModel(pd.DataFrame(), DARK if QApplication.instance().property("dark_mode") else LIGHT))
        self.progress.reset()
        self.txt_not_found.clear()
        self.txt_not_found.setStyleSheet("")
        self._set_status("Resultados limpiados.", "muted")

    def _set_status(self, msg, state):
        colors = {"success": "#22c55e", "error": "#ef4444", "warning": "#f59e0b", "muted": "gray"}
        self.lbl_status.setText(msg)
        self.lbl_status.setStyleSheet(f"color: {colors.get(state,'gray')};")

    def _export(self, fmt: str):
        if self._result_df is None or self._result_df.empty:
            QMessageBox.warning(self, "Sin resultados", "Realizá una búsqueda primero."); return
        folder_col = self.combo_exp_folder.currentText()
        file_col   = self.combo_exp_file.currentText()
        if not folder_col:
            QMessageBox.warning(self, "Sin columna", "Seleccioná la columna para carpetas."); return

        files = self._files_panel.get_files()
        base  = Path(files[0]).parent if files else Path.home()
        out_dir = str(base / "search_output")
        # Exportar con el mismo filtro de columnas que muestra la grilla
        if self.rb_cols_preset.isChecked():
            available = set(self._result_df.columns)
            ordered = []
            for candidates in self._PRESET_COL_ORDER:
                for c in candidates:
                    if c in available:
                        ordered.append(c)
                        break
            keep = ordered if ordered else [c for c in self._result_df.columns if c != "Archivo"]
            df = self._result_df[keep].copy()
        else:
            df = self._result_df.drop(columns=["Archivo"], errors="ignore").copy()

        def export_fn(df, folder_col, file_col, out_dir, fmt, cancel_fn, progress_cb):
            import json as _json
            Path(out_dir).mkdir(parents=True, exist_ok=True)
            groups = df.groupby(folder_col)
            total  = len(groups)
            for i, (folder_val, group_df) in enumerate(groups):
                if cancel_fn(): return None
                folder_path = Path(out_dir) / sanitize_filename(str(folder_val))
                folder_path.mkdir(parents=True, exist_ok=True)
                for file_val, file_df in group_df.groupby(file_col):
                    if cancel_fn(): return None
                    fname = sanitize_filename(str(file_val))
                    if fmt == "json":
                        fpath = folder_path / f"{fname}.json"
                        data = file_df.drop(columns=["Archivo"], errors="ignore").to_dict(orient="records")
                        with open(fpath, "w", encoding="utf-8") as f:
                            _json.dump(data, f, ensure_ascii=False, indent=2)
                    else:
                        fpath = folder_path / f"{fname}.csv"
                        file_df.to_csv(fpath, index=False, encoding="utf-8-sig")
                progress_cb(int((i + 1) / total * 100), f"{folder_val}…")
            return out_dir

        self._export_worker = GenericWorker(export_fn, df, folder_col, file_col, out_dir, fmt)
        self._export_worker.progress.connect(lambda p, _: self.exp_progress.set(p))
        self._export_worker.done.connect(lambda d: (self.exp_progress.set(100),
                                                     setattr(self, "_last_export_dir", d),
                                                     self.btn_exp_open.setEnabled(True)))
        self._export_worker.error.connect(lambda e: QMessageBox.critical(self, "Error", e))
        self._export_worker.start()

    def _open_export_dir(self):
        if hasattr(self, "_last_export_dir") and os.path.isdir(self._last_export_dir):
            os.startfile(self._last_export_dir)

    def apply_palette(self, p: dict):
        self.table.apply_palette(p)
        if self._result_df is not None and not self._result_df.empty:
            self.table.table.view.model().sourceModel().update_palette(p)
        # Re-aplicar color de txt_not_found con el nuevo tema
        text = self.txt_not_found.toPlainText()
        if text:
            color = p['success'] if text.startswith("✓") else p['error']
            self.txt_not_found.setStyleSheet(
                f"color: {color}; background-color: {p['surface']}; border: 1px solid {p['border']};"
            )
        else:
            self.txt_not_found.setStyleSheet(
                f"background-color: {p['surface']}; border: 1px solid {p['border']};"
            )

# ── Tab: Agregar Columna ──────────────────────────────────────────────────────

class AddColumnTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._worker: GenericWorker | None = None
        self._filepath = self._enc = self._delim = ""
        self._columns: list = []
        self._last_dir = ""

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)

        form = QFormLayout(); form.setSpacing(8)
        self.entry_name  = QLineEdit(); self.entry_name.setPlaceholderText("Ej: PHONEMODEL_NAME")
        self.entry_value = QLineEdit(); self.entry_value.setPlaceholderText("Ej: MODELO_DEFAULT")
        form.addRow("Nombre de columna:", self.entry_name)
        form.addRow("Valor constante:",   self.entry_value)
        layout.addLayout(form)

        layout.addWidget(hline())

        pos_row = QHBoxLayout()
        pos_row.addWidget(QLabel("Posición:"))
        self._pos_group = QButtonGroup(self)
        self.rb_start = QRadioButton("Al inicio")
        self.rb_end   = QRadioButton("Al final");  self.rb_end.setChecked(True)
        self.rb_after = QRadioButton("Después de:")
        for rb in (self.rb_start, self.rb_end, self.rb_after):
            self._pos_group.addButton(rb); pos_row.addWidget(rb)
        self.combo_after = QComboBox(); self.combo_after.setMinimumWidth(150); self.combo_after.setEnabled(False)
        self.rb_after.toggled.connect(self.combo_after.setEnabled)
        pos_row.addWidget(self.combo_after); pos_row.addStretch()
        layout.addLayout(pos_row)

        dest_row = QHBoxLayout()
        dest_row.addWidget(QLabel("📁 Destino:"))
        self.lbl_dest = QLabel("—"); self.lbl_dest.setStyleSheet("color: gray;")
        self.btn_open_dest = QPushButton("📂"); self.btn_open_dest.setFixedWidth(36); self.btn_open_dest.setEnabled(False)
        self.btn_open_dest.clicked.connect(self._open_dest)
        dest_row.addWidget(self.lbl_dest, stretch=1); dest_row.addWidget(self.btn_open_dest)
        layout.addLayout(dest_row)

        self.btn_add = QPushButton("➕  Agregar Columna"); self.btn_add.setObjectName("success"); self.btn_add.setFixedHeight(36)
        self.btn_add.clicked.connect(self._start)
        layout.addWidget(self.btn_add)

        self.progress = ProgressRow(); layout.addWidget(self.progress)
        self.lbl_status = QLabel("Listo para procesar."); self.lbl_status.setWordWrap(True)
        layout.addWidget(self.lbl_status)
        layout.addStretch()

    def setup(self, filepath, enc, delim, columns):
        self._filepath = filepath; self._enc = enc; self._delim = delim; self._columns = columns
        self.combo_after.clear(); self.combo_after.addItems(columns)
        dest = str(Path(filepath).parent / "CSV_con_columna_agregada") if filepath else "—"
        self.lbl_dest.setText(dest)

    def _get_position(self):
        if self.rb_start.isChecked(): return "start", None
        if self.rb_after.isChecked(): return "after", self.combo_after.currentText()
        return "end", None

    def _start(self):
        if not self._filepath:
            QMessageBox.warning(self, "Sin archivo", "Cargá un archivo CSV primero."); return
        col_name = self.entry_name.text().strip()
        col_val  = self.entry_value.text().strip()
        if not col_name:
            QMessageBox.warning(self, "Sin nombre", "Ingresá el nombre de la columna."); return
        pos, after_col = self._get_position()
        out = str(Path(self._filepath).parent / "CSV_con_columna_agregada")

        def add_col_fn(fp, enc, delim, col_name, col_val, pos, after_col, out, cancel_fn, progress_cb):
            import csv as _csv
            Path(out).mkdir(parents=True, exist_ok=True)
            with open(fp, encoding=enc, newline="") as f:
                reader = _csv.DictReader(f, delimiter=delim)
                rows   = list(reader)
                orig_cols = reader.fieldnames or []
            if cancel_fn(): return None
            # Insertar columna
            if pos == "start":
                new_cols = [col_name] + orig_cols
            elif pos == "after" and after_col in orig_cols:
                idx = orig_cols.index(after_col) + 1
                new_cols = orig_cols[:idx] + [col_name] + orig_cols[idx:]
            else:
                new_cols = orig_cols + [col_name]
            out_path = Path(out) / Path(fp).name
            with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
                writer = _csv.DictWriter(f, fieldnames=new_cols)
                writer.writeheader()
                total = len(rows)
                for i, row in enumerate(rows):
                    if cancel_fn(): return None
                    row[col_name] = col_val
                    writer.writerow({c: row.get(c, "") for c in new_cols})
                    if i % 1000 == 0:
                        progress_cb(int(i / total * 100), f"Fila {i}/{total}…")
            return out

        self._worker = GenericWorker(add_col_fn, self._filepath, self._enc, self._delim,
                                     col_name, col_val, pos, after_col, out)
        self._worker.progress.connect(lambda p, m: (self.progress.set(p), self.lbl_status.setText(m)))
        self._worker.done.connect(lambda d: self._finish(f"✓ Guardado en: {d}", "success", d))
        self._worker.error.connect(lambda e: self._finish(f"✗ {e}", "error", ""))
        self._worker.cancelled.connect(lambda: self._finish("⚠ Cancelado.", "warning", ""))
        self.btn_add.setEnabled(False); self.progress.reset()
        self._worker.start()

    def _finish(self, msg, state, out_dir):
        self.btn_add.setEnabled(True)
        colors = {"success": "#22c55e", "error": "#ef4444", "warning": "#f59e0b"}
        self.lbl_status.setText(msg); self.lbl_status.setStyleSheet(f"color: {colors.get(state,'gray')};")
        if out_dir: self._last_dir = out_dir; self.btn_open_dest.setEnabled(True)

    def _open_dest(self):
        if self._last_dir and os.path.isdir(self._last_dir): os.startfile(self._last_dir)

# ── Tab: Part Name ────────────────────────────────────────────────────────────

class PartNameTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._df: pd.DataFrame | None = None

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)

        row1 = QHBoxLayout()
        row1.addWidget(QLabel("CLASSCODE:"))
        self.combo_classcode = QComboBox(); self.combo_classcode.setMinimumWidth(200)
        self.btn_analyze = QPushButton("🔍  Analizar"); self.btn_analyze.setObjectName("accent"); self.btn_analyze.setFixedHeight(34)
        self.btn_analyze.clicked.connect(self._analyze)
        row1.addWidget(self.combo_classcode); row1.addWidget(self.btn_analyze); row1.addStretch()
        layout.addLayout(row1)

        layout.addWidget(hline())

        hdr_row = QHBoxLayout()
        hdr_row.addWidget(QLabel("📝  RegEx para KEYUNITBARCODE:"))
        self.btn_copy_regex = QPushButton("📋  Copiar RegEx"); self.btn_copy_regex.setFixedHeight(28)
        self.btn_copy_regex.clicked.connect(self._copy_regex)
        hdr_row.addStretch(); hdr_row.addWidget(self.btn_copy_regex)
        layout.addLayout(hdr_row)

        self.regex_text = QTextEdit()
        self.regex_text.setReadOnly(True)
        self.regex_text.setFont(QFont("Consolas", 10))
        self.regex_text.setFixedHeight(140)
        self.regex_text.setPlaceholderText("Seleccioná un CLASSCODE y presioná 'Analizar'")
        layout.addWidget(self.regex_text)

        layout.addWidget(QLabel("📊  KEYMATERIAL (agrupados):"))

        scroll = QScrollArea(); scroll.setWidgetResizable(True); scroll.setFixedHeight(180)
        self._mat_widget = QWidget(); self._mat_layout = QVBoxLayout(self._mat_widget)
        self._mat_layout.setContentsMargins(4, 4, 4, 4); self._mat_layout.setSpacing(3)
        self._mat_layout.addStretch()
        scroll.setWidget(self._mat_widget)
        layout.addWidget(scroll)

        self.lbl_stats = QLabel("Esperando análisis…")
        self.lbl_stats.setFont(QFont("Segoe UI", 10))
        self.lbl_stats.setStyleSheet("color: gray;")
        layout.addWidget(self.lbl_stats)
        layout.addStretch()

    def setup(self, df: pd.DataFrame, columns: list):
        self._df = df
        if "CLASSCODE" in columns:
            codes = sorted(df["CLASSCODE"].dropna().unique().tolist()) if self._df is not None and "CLASSCODE" in df.columns else []
            self.combo_classcode.clear()
            self.combo_classcode.addItems(codes)

    def _analyze(self):
        if self._df is None: return
        code = self.combo_classcode.currentText()
        if not code: return
        subset = self._df[self._df.get("CLASSCODE", pd.Series()) == code] if "CLASSCODE" in self._df.columns else self._df

        # RegEx
        if "KEYUNITBARCODE" in self._df.columns:
            values = subset["KEYUNITBARCODE"].dropna().unique().tolist()
            pattern = self._generate_regex(values)
            self.regex_text.setPlainText(pattern)
        else:
            self.regex_text.setPlainText("Columna KEYUNITBARCODE no encontrada.")

        # KEYMATERIAL
        while self._mat_layout.count() > 1:
            item = self._mat_layout.takeAt(0)
            if item.widget(): item.widget().deleteLater()

        if "KEYMATERIAL" in self._df.columns:
            counts = subset["KEYMATERIAL"].value_counts().head(50)
            for mat, cnt in counts.items():
                row_w = QWidget(); row_h = QHBoxLayout(row_w); row_h.setContentsMargins(0, 0, 0, 0)
                lbl_m = QLabel(str(mat)); lbl_m.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
                lbl_c = QLabel(f"× {cnt}"); lbl_c.setStyleSheet("color: gray;"); lbl_c.setFixedWidth(60)
                row_h.addWidget(lbl_m); row_h.addWidget(lbl_c)
                self._mat_layout.insertWidget(self._mat_layout.count() - 1, row_w)

        desc = CLASS_CODE_MAP.get(code, code)
        self.lbl_stats.setText(f"{desc}  ·  {len(subset)} registros")

    def _generate_regex(self, values: list) -> str:
        if not values: return ""
        import re
        # Prefijos comunes
        if len(values) == 1:
            return re.escape(values[0])
        # Encontrar prefijo común
        prefix = values[0]
        for v in values[1:]:
            while not v.startswith(prefix):
                prefix = prefix[:-1]
                if not prefix: break
        if len(prefix) >= 3:
            rest_lens = set(len(v) - len(prefix) for v in values)
            if len(rest_lens) == 1:
                return f"^{re.escape(prefix)}[A-Z0-9]{{{list(rest_lens)[0]}}}$"
            min_l, max_l = min(rest_lens), max(rest_lens)
            return f"^{re.escape(prefix)}[A-Z0-9]{{{min_l},{max_l}}}$"
        # Sin prefijo común: alternancia de sufijos únicos
        unique = sorted(set(values))
        if len(unique) <= 20:
            return "^(" + "|".join(re.escape(v) for v in unique) + ")$"
        lens = set(len(v) for v in unique)
        if len(lens) == 1:
            return f"^[A-Z0-9]{{{list(lens)[0]}}}$"
        return f"^[A-Z0-9]{{{min(lens)},{max(lens)}}}$"

    def _copy_regex(self):
        text = self.regex_text.toPlainText().strip()
        if text: QApplication.clipboard().setText(text)

# ── Ventana principal ─────────────────────────────────────────────────────────

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self._dark      = True
        self._p         = DARK
        self._filepath  = ""
        self._enc       = ""
        self._delim     = ""
        self._columns:  list = []
        self._df_preview: pd.DataFrame | None = None
        self._rename_map: dict = {}
        self._loader: FileLoaderWorker | None = None

        self.setWindowTitle("CSV Processor")
        self.resize(1280, 780)
        self.setMinimumSize(1000, 660)

        self._build_ui()
        self._apply_palette()

    # ── UI ────────────────────────────────────────────────────────────────────

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # Top bar
        self._top_bar = QWidget()
        top_layout = QHBoxLayout(self._top_bar)
        top_layout.setContentsMargins(12, 8, 12, 8)
        top_layout.setSpacing(10)
        self.btn_open = QPushButton("📂  Abrir CSV")
        self.btn_open.setFixedHeight(36)
        self.btn_open.clicked.connect(self._load_file)
        self.lbl_file = QLabel("Ningún archivo cargado")
        self.lbl_file.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.lbl_file.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        self._top_spacer = QWidget()
        self._top_spacer.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.btn_theme = QToolButton()
        self.btn_theme.setText("☀")
        self.btn_theme.setFixedSize(36, 36)
        self.btn_theme.clicked.connect(self._toggle_theme)
        top_layout.addWidget(self.btn_open)
        top_layout.addWidget(self.lbl_file)
        top_layout.addWidget(self._top_spacer)
        top_layout.addWidget(self.btn_theme)
        root.addWidget(self._top_bar)

        root.addWidget(hline())

        # Splitter: panel izquierdo | tabs
        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.setHandleWidth(4)

        # Panel izquierdo (stack: columnas / archivos búsqueda)
        self._left_stack = QStackedWidget()
        self._left_stack.setFixedWidth(270)
        self.col_panel    = ColumnPanel()
        self.files_panel  = SearchFilesPanel()
        self._left_stack.addWidget(self.col_panel)
        self._left_stack.addWidget(self.files_panel)
        splitter.addWidget(self._left_stack)

        # Tabs
        self.tabs = QTabWidget()
        self.tab_csv      = ExportCSVTab()
        self.tab_txt      = ExportTXTTab()
        self.tab_json     = ExportJSONTab()
        self.tab_search   = SearchTab(self.files_panel)
        self.tab_addcol   = AddColumnTab()
        self.tab_partname = PartNameTab()

        self.tabs.addTab(self.tab_csv,      "  Exportar CSV  ")
        self.tabs.addTab(self.tab_txt,      "  Exportar TXT  ")
        self.tabs.addTab(self.tab_json,     "  Exportar JSON  ")
        self.tabs.addTab(self.tab_search,   "  Buscar  ")
        self.tabs.addTab(self.tab_addcol,   "  Agregar Columna  ")
        self.tabs.addTab(self.tab_partname, "  Part Name  ")
        self.tabs.currentChanged.connect(self._on_tab_changed)
        splitter.addWidget(self.tabs)
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)

        root.addWidget(splitter, stretch=1)

        # Status bar
        self.status = QStatusBar()
        self.setStatusBar(self.status)

        # Conectar señal de columnas
        self.col_panel.columns_changed.connect(self._on_columns_changed)

    def _on_tab_changed(self, index: int):
        is_search = (self.tabs.tabText(index).strip() == "Buscar")
        self._left_stack.setCurrentIndex(1 if is_search else 0)
        self.btn_open.setVisible(not is_search)
        self.lbl_file.setVisible(not is_search)
        self._top_spacer.setVisible(is_search)

    # ── Carga de archivo ──────────────────────────────────────────────────────

    def _load_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Abrir CSV", "", "CSV (*.csv);;Todos (*)")
        if not path: return
        self.lbl_file.setText(f"Cargando {Path(path).name}…")
        QApplication.setOverrideCursor(QCursor(Qt.CursorShape.WaitCursor))
        self._loader = FileLoaderWorker(path)
        self._loader.done.connect(self._on_file_loaded)
        self._loader.error.connect(self._on_file_error)
        self._loader.start()

    def _on_file_loaded(self, path, enc, delim, columns, df):
        QApplication.restoreOverrideCursor()
        self._filepath    = path
        self._enc         = enc
        self._delim       = delim
        self._columns     = columns
        self._df_preview  = df
        self._rename_map  = {}

        # Detectar columnas preset faltantes
        missing = [c for c in PRESET_COLUMNS if c not in columns]
        if missing:
            dlg = ColumnMapDialog(missing, columns, self)
            dlg.exec()
            if not dlg.cancelled:
                self._rename_map = {v: k for k, v in dlg.result_map.items() if v}

        name = Path(path).name
        self.lbl_file.setText(f"📄  {name}  —  {len(df):,} filas × {len(df.columns)} columnas  ({enc})")

        # Actualizar panel de columnas
        self.col_panel.set_columns(columns, self._rename_map)

        # Setup de cada tab
        self.tab_csv.setup(path, enc, delim,
                           self.col_panel.get_selected,
                           self.col_panel.get_rename_map)
        self.tab_csv.set_preview(df, self._p)
        self.tab_txt.setup(path, enc, delim, columns, self._rename_map, df)
        self.tab_json.setup(path, enc, delim, columns, self.col_panel.get_selected, self._rename_map, df)
        self.tab_addcol.setup(path, enc, delim, columns)
        self.tab_search.set_columns(columns)
        self.tab_partname.setup(df, columns)

        self.status.showMessage(f"Cargado: {path}", 4000)

    def _on_file_error(self, msg):
        QApplication.restoreOverrideCursor()
        self.lbl_file.setText("Error al cargar el archivo.")
        QMessageBox.critical(self, "Error", f"No se pudo cargar el archivo:\n{msg}")

    def _on_columns_changed(self):
        """Refresca el preview cuando cambian las columnas seleccionadas."""
        if self._df_preview is not None:
            selected   = self.col_panel.get_selected()
            cols_in_df = [c for c in selected if c in self._df_preview.columns]
            filtered   = self._df_preview[cols_in_df] if cols_in_df else self._df_preview
            self.tab_csv.set_preview(filtered, self._p)
        self.tab_json._update_preview()

    # ── Tema ──────────────────────────────────────────────────────────────────

    def _toggle_theme(self):
        self._dark = not self._dark
        self._p    = DARK if self._dark else LIGHT
        self.btn_theme.setText("☀" if self._dark else "🌙")
        QApplication.instance().setProperty("dark_mode", self._dark)
        self._apply_palette()

    def _apply_palette(self):
        p = self._p
        QApplication.instance().setStyleSheet(app_stylesheet(p))
        self.tab_csv.apply_palette(p)
        self.tab_search.apply_palette(p)
        if self._df_preview is not None:
            self._on_columns_changed()

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setProperty("dark_mode", True)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())
