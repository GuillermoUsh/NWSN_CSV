"""
ui/tabs/export_csv.py — Tab de exportación y filtrado de CSV.
"""

import os
from pathlib import Path

import pandas as pd
from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtGui import QFont, QCursor
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QLineEdit, QRadioButton, QButtonGroup, QMessageBox, QApplication,
)

from constants import PAGE_SIZE, PREVIEW_ROWS
from ui.theme import DARK
from ui.workers import CSVExportWorker
from ui.widgets import DataTable, PandasTableModel, ProgressRow, hline


class ExportCSVTab(QWidget):
    request_columns = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self._worker: CSVExportWorker | None = None
        self._last_out_dir: str = ""
        self._df_full: pd.DataFrame | None = None
        self._page: int = 0
        self._palette: dict = DARK

        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 6, 10, 6)
        layout.setSpacing(4)

        # Paginación
        page_bar = QHBoxLayout()
        self.btn_first = QPushButton("|◀"); self.btn_first.setFixedSize(34, 26)
        self.btn_prev  = QPushButton("◀");  self.btn_prev.setFixedSize(34, 26)
        self.btn_next  = QPushButton("▶");  self.btn_next.setFixedSize(34, 26)
        self.btn_last  = QPushButton("▶|"); self.btn_last.setFixedSize(34, 26)
        for b in (self.btn_first, self.btn_prev, self.btn_next, self.btn_last):
            b.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
            b.setEnabled(False)
        self.lbl_page = QLabel("—")
        self.lbl_page.setFont(QFont("Segoe UI", 9))
        self.lbl_page.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_page.setMinimumWidth(160)
        self.btn_first.clicked.connect(lambda: self._go_page(0))
        self.btn_prev.clicked.connect(lambda: self._go_page(self._page - 1))
        self.btn_next.clicked.connect(lambda: self._go_page(self._page + 1))
        self.btn_last.clicked.connect(lambda: self._go_page(self._total_pages() - 1))
        page_bar.addStretch()
        for w in (self.btn_first, self.btn_prev, self.lbl_page, self.btn_next, self.btn_last):
            page_bar.addWidget(w)
        page_bar.addStretch()
        layout.addLayout(page_bar)

        self.table = DataTable()
        layout.addWidget(self.table, stretch=1)
        layout.addWidget(hline())

        # Carpeta + delimitador
        opts = QHBoxLayout()
        opts.addWidget(QLabel("📁 Salida:"))
        self.entry_out = QLineEdit()
        self.entry_out.setPlaceholderText("Carpeta de salida...")
        opts.addWidget(self.entry_out, stretch=1)
        opts.addSpacing(16)
        opts.addWidget(QLabel("Delimitador:"))
        self._delim_group = QButtonGroup(self)
        for label, val in [("Coma", "comma"), ("Punto y coma", "semicolon"), ("Tab", "tab")]:
            rb = QRadioButton(label)
            rb.setProperty("delim_val", val)
            self._delim_group.addButton(rb)
            opts.addWidget(rb)
            if val == "comma":
                rb.setChecked(True)
        layout.addLayout(opts)

        actions = QHBoxLayout()
        self.btn_process = QPushButton("▶  PROCESAR")
        self.btn_process.setObjectName("success")
        self.btn_process.setFixedHeight(32)
        self.btn_cancel = QPushButton("✕  Cancelar")
        self.btn_cancel.setObjectName("danger")
        self.btn_cancel.setFixedHeight(32)
        self.btn_cancel.setEnabled(False)
        self.btn_open = QPushButton("📂  Abrir carpeta")
        self.btn_open.setFixedHeight(32)
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

        self._filepath = ""
        self._enc = ""
        self._delim = ""
        self._get_columns_fn  = None
        self._get_rename_fn   = None

    def setup(self, filepath, enc, delim, get_columns_fn, get_rename_fn):
        self._filepath        = filepath
        self._enc             = enc
        self._delim           = delim
        self._get_columns_fn  = get_columns_fn
        self._get_rename_fn   = get_rename_fn
        self.entry_out.setText(str(Path(filepath).parent / "csv_output"))

    def set_preview(self, df: pd.DataFrame, palette: dict):
        self._df_full = df
        self._page    = 0
        self._render_page(palette)

    def _total_pages(self) -> int:
        if self._df_full is None or len(self._df_full) == 0:
            return 1
        return max(1, -(-len(self._df_full) // PAGE_SIZE))

    def _go_page(self, page: int):
        self._page = max(0, min(page, self._total_pages() - 1))
        self._render_page()

    def _render_page(self, palette: dict = None):
        if self._df_full is None:
            return
        p = palette or self._palette
        if palette:
            self._palette = palette
        start   = self._page * PAGE_SIZE
        end     = start + PAGE_SIZE
        page_df = self._df_full.iloc[start:end]
        model = PandasTableModel(page_df, p)
        self.table.set_model(model)
        self.table.apply_palette(p)
        total      = self._total_pages()
        row_start  = start + 1
        row_end    = min(end, len(self._df_full))
        total_rows = len(self._df_full)
        suffix = (f" (preview {total_rows:,})" if total_rows >= PREVIEW_ROWS
                  else f" de {total_rows:,}")
        self.lbl_page.setText(
            f"Pág. {self._page + 1} / {total}  ·  filas {row_start}–{row_end}{suffix}"
        )
        self.btn_first.setEnabled(self._page > 0)
        self.btn_prev.setEnabled(self._page > 0)
        self.btn_next.setEnabled(self._page < total - 1)
        self.btn_last.setEnabled(self._page < total - 1)

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
            columns, {}, out_dir, self._get_delim(), rename_map,
        )
        self._worker.progress.connect(self._on_progress)
        self._worker.done.connect(self._on_done)
        self._worker.error.connect(self._on_error)
        self._worker.cancelled.connect(lambda: self._finish("⚠ Cancelado.", "warning"))
        self.btn_process.setEnabled(False)
        self.btn_cancel.setEnabled(True)
        self.btn_open.setEnabled(False)
        self.progress.reset()
        self._set_status("Procesando…", "muted")
        QApplication.setOverrideCursor(QCursor(Qt.CursorShape.WaitCursor))
        self._worker.start()

    def _cancel(self):
        if self._worker:
            self._worker.cancel()

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
        self.btn_cancel.setEnabled(False)
        self._set_status(msg, state)
        QApplication.restoreOverrideCursor()

    def _set_status(self, msg, state):
        colors = {"success": "#22c55e", "error": "#ef4444", "warning": "#f59e0b", "muted": "gray"}
        self.lbl_status.setText(msg)
        self.lbl_status.setStyleSheet(f"color: {colors.get(state, 'gray')};")

    def _open_folder(self):
        if self._last_out_dir and os.path.isdir(self._last_out_dir):
            os.startfile(self._last_out_dir)

    def apply_palette(self, p: dict):
        self._palette = p
        self.table.apply_palette(p)
