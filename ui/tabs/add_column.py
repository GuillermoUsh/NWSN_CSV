"""
ui/tabs/add_column.py — Tab para agregar una columna constante a un CSV.
"""

import os
from pathlib import Path

import pandas as pd
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QFormLayout, QLabel, QPushButton,
    QComboBox, QLineEdit, QRadioButton, QButtonGroup, QMessageBox,
)

from constants import PREVIEW_ROWS
from ui.workers import GenericWorker
from ui.widgets import DataTable, PandasTableModel, ProgressRow, hline


class AddColumnTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._worker: GenericWorker | None = None
        self._filepath = self._enc = self._delim = ""
        self._columns: list = []
        self._last_dir = ""
        self._palette: dict = {}

        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(6)

        self.table = DataTable()
        self.table.setVisible(False)
        layout.addWidget(self.table, stretch=1)

        layout.addWidget(hline())

        form = QFormLayout()
        form.setSpacing(8)
        self.entry_name  = QLineEdit()
        self.entry_name.setPlaceholderText("Ej: PHONEMODEL_NAME")
        self.entry_value = QLineEdit()
        self.entry_value.setPlaceholderText("Ej: MODELO_DEFAULT")
        self.entry_name.textChanged.connect(lambda: self._to_upper(self.entry_name))
        self.entry_value.textChanged.connect(lambda: self._to_upper(self.entry_value))
        form.addRow("Nombre de columna:", self.entry_name)
        form.addRow("Valor constante:",   self.entry_value)
        layout.addLayout(form)

        pos_row = QHBoxLayout()
        pos_row.addWidget(QLabel("Posición:"))
        self._pos_group = QButtonGroup(self)
        self.rb_start = QRadioButton("Al inicio")
        self.rb_end   = QRadioButton("Al final")
        self.rb_end.setChecked(True)
        self.rb_after = QRadioButton("Después de:")
        for rb in (self.rb_start, self.rb_end, self.rb_after):
            self._pos_group.addButton(rb)
            pos_row.addWidget(rb)
        self.combo_after = QComboBox()
        self.combo_after.setMinimumWidth(150)
        self.combo_after.setEnabled(False)
        self.rb_after.toggled.connect(self.combo_after.setEnabled)
        pos_row.addWidget(self.combo_after)
        pos_row.addStretch()
        layout.addLayout(pos_row)

        dest_row = QHBoxLayout()
        dest_row.addWidget(QLabel("📁 Destino:"))
        self.lbl_dest = QLabel("—")
        self.lbl_dest.setStyleSheet("color: gray;")
        dest_row.addWidget(self.lbl_dest, stretch=1)
        layout.addLayout(dest_row)

        actions = QHBoxLayout()
        self.btn_preview = QPushButton("👁  Vista Previa")
        self.btn_preview.setObjectName("accent")
        self.btn_preview.setFixedHeight(38)
        self.btn_preview.clicked.connect(self._show_preview)
        self.btn_add = QPushButton("➕  Agregar Columna")
        self.btn_add.setObjectName("success")
        self.btn_add.setFixedHeight(38)
        self.btn_add.clicked.connect(self._start)
        self.btn_open = QPushButton("📂  Abrir carpeta")
        self.btn_open.setFixedHeight(38)
        self.btn_open.setEnabled(False)
        self.btn_open.clicked.connect(self._open_dest)
        actions.addWidget(self.btn_preview)
        actions.addWidget(self.btn_add)
        actions.addWidget(self.btn_open)
        actions.addStretch()
        layout.addLayout(actions)

        self.progress = ProgressRow()
        layout.addWidget(self.progress)
        self.lbl_status = QLabel("Listo para procesar.")
        self.lbl_status.setWordWrap(True)
        layout.addWidget(self.lbl_status)

    def setup(self, filepath, enc, delim, columns, palette: dict = None):
        self._filepath = filepath
        self._enc      = enc
        self._delim    = delim
        self._columns  = columns
        if palette:
            self._palette = palette
        self.combo_after.clear()
        self.combo_after.addItems(columns)
        dest = str(Path(filepath).parent / "CSV_con_columna_agregada") if filepath else "—"
        self.lbl_dest.setText(dest)

    def set_preview(self, df: pd.DataFrame, palette: dict):
        self._palette = palette
        if df is not None and not df.empty:
            model = PandasTableModel(df, palette)
            self.table.set_model(model)
            self.table.apply_palette(palette)
            self.table.setVisible(True)
            self.lbl_status.setText(f"✓ Archivo cargado: {len(df)} filas")
        else:
            self.table.setVisible(False)

    def _to_upper(self, line_edit: QLineEdit):
        text       = line_edit.text()
        upper_text = text.upper()
        if text != upper_text:
            cursor_pos = line_edit.cursorPosition()
            line_edit.setText(upper_text)
            line_edit.setCursorPosition(cursor_pos)

    def _get_position(self):
        if self.rb_start.isChecked(): return "start", None
        if self.rb_after.isChecked(): return "after", self.combo_after.currentText()
        return "end", None

    def _show_preview(self):
        if not self._filepath:
            QMessageBox.warning(self, "Sin archivo", "Cargá un archivo CSV primero.")
            return
        col_name = self.entry_name.text().strip()
        col_val  = self.entry_value.text().strip()
        if not col_name:
            QMessageBox.warning(self, "Sin nombre", "Ingresá el nombre de la columna.")
            return
        try:
            df = pd.read_csv(
                self._filepath, encoding=self._enc, delimiter=self._delim, nrows=PREVIEW_ROWS,
            )
            pos, after_col = self._get_position()
            if pos == "start":
                df.insert(0, col_name, col_val)
            elif pos == "after" and after_col in df.columns:
                idx = df.columns.get_loc(after_col) + 1
                df.insert(idx, col_name, col_val)
            else:
                df[col_name] = col_val
            model = PandasTableModel(df, self._palette)
            self.table.set_model(model)
            self.table.apply_palette(self._palette)
            self.table.setVisible(True)
            self.lbl_status.setText(f"✓ Preview: se agregará columna '{col_name}' = '{col_val}'")
            self.lbl_status.setStyleSheet("color: #3b82f6;")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo generar preview: {e}")

    def _start(self):
        if not self._filepath:
            QMessageBox.warning(self, "Sin archivo", "Cargá un archivo CSV primero.")
            return
        col_name = self.entry_name.text().strip()
        col_val  = self.entry_value.text().strip()
        if not col_name:
            QMessageBox.warning(self, "Sin nombre", "Ingresá el nombre de la columna.")
            return
        pos, after_col = self._get_position()
        out = str(Path(self._filepath).parent / "CSV_con_columna_agregada")

        def add_col_fn(fp, enc, delim, col_name, col_val, pos, after_col, out, cancel_fn, progress_cb):
            import csv as _csv
            Path(out).mkdir(parents=True, exist_ok=True)
            with open(fp, encoding=enc, newline="") as f:
                reader    = _csv.DictReader(f, delimiter=delim)
                rows      = list(reader)
                orig_cols = reader.fieldnames or []
            if cancel_fn(): return None
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

        self._worker = GenericWorker(
            add_col_fn, self._filepath, self._enc, self._delim,
            col_name, col_val, pos, after_col, out,
        )
        self._worker.progress.connect(lambda p, m: (self.progress.set(p), self.lbl_status.setText(m)))
        self._worker.done.connect(lambda d: self._finish(f"✓ Guardado en: {d}", "success", d))
        self._worker.error.connect(lambda e: self._finish(f"✗ {e}", "error", ""))
        self._worker.cancelled.connect(lambda: self._finish("⚠ Cancelado.", "warning", ""))
        self.btn_add.setEnabled(False)
        self.progress.reset()
        self._worker.start()

    def _finish(self, msg, state, out_dir):
        self.btn_add.setEnabled(True)
        colors = {"success": "#22c55e", "error": "#ef4444", "warning": "#f59e0b"}
        self.lbl_status.setText(msg)
        self.lbl_status.setStyleSheet(f"color: {colors.get(state, 'gray')};")
        if out_dir:
            self._last_dir = out_dir
            self.btn_open.setEnabled(True)
            try:
                out_file = Path(out_dir) / Path(self._filepath).name
                if out_file.exists():
                    df_result = pd.read_csv(str(out_file), encoding="utf-8-sig", nrows=PREVIEW_ROWS)
                    model = PandasTableModel(df_result, self._palette)
                    self.table.set_model(model)
                    self.table.apply_palette(self._palette)
                    self.table.setVisible(True)
            except Exception:
                pass  # si falla la carga del resultado, no mostrar grilla

    def _open_dest(self):
        if self._last_dir and os.path.isdir(self._last_dir):
            os.startfile(self._last_dir)

    def apply_palette(self, p: dict):
        self._palette = p
        self.table.apply_palette(p)
