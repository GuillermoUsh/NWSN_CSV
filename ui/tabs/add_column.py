"""
ui/tabs/add_column.py — Tab para agregar una columna constante a un CSV.
"""

import os
from pathlib import Path

import pandas as pd
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QFont, QCursor
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QFormLayout, QLabel, QPushButton,
    QComboBox, QLineEdit, QRadioButton, QButtonGroup, QMessageBox, QApplication,
    QFileDialog,
)

from constants import PREVIEW_ROWS
from processor import validate_uniform_column, add_column_to_csv
from ui.workers import FileLoaderWorker, GenericWorker
from ui.widgets import DataTable, PandasTableModel, ProgressRow, hline


# Candidatos para la columna de modelo (en orden de prioridad)
_MODEL_CANDIDATES = [
    "PHONEMODEL_NAME", "PHONE_MODEL", "MODEL_NAME", "MODEL", "PHONEMODEL", "PHONENAME",
]


class AddColumnTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._loader: FileLoaderWorker | None = None
        self._worker: GenericWorker  | None = None
        self._filepath = self._enc = self._delim = ""
        self._columns: list = []
        self._last_dir = ""
        self._palette: dict = {}

        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(6)

        # ── Carga de archivo ──────────────────────────────────────────────────
        file_row = QHBoxLayout()
        self.btn_open_file = QPushButton("📂  Abrir CSV")
        self.btn_open_file.setFixedHeight(34)
        self.btn_open_file.clicked.connect(self._load_file)
        self.lbl_file = QLabel("Ningún archivo cargado")
        self.lbl_file.setFont(QFont("Segoe UI", 9))
        self.lbl_file.setStyleSheet("color: gray;")
        file_row.addWidget(self.btn_open_file)
        file_row.addWidget(self.lbl_file, stretch=1)
        layout.addLayout(file_row)

        # ── Preview ───────────────────────────────────────────────────────────
        self.table = DataTable()
        self.table.setVisible(False)
        layout.addWidget(self.table, stretch=1)

        layout.addWidget(hline())

        # ── Columna de validación ─────────────────────────────────────────────
        val_row = QHBoxLayout()
        val_row.addWidget(QLabel("Columna de modelo:"))
        self.combo_model = QComboBox()
        self.combo_model.setMinimumWidth(180)
        self.combo_model.currentTextChanged.connect(self._on_model_col_changed)
        val_row.addWidget(self.combo_model)
        self.lbl_model_badge = QLabel("")
        self.lbl_model_badge.setFont(QFont("Segoe UI", 9))
        self.lbl_model_badge.setWordWrap(True)
        val_row.addWidget(self.lbl_model_badge, stretch=1)
        layout.addLayout(val_row)

        layout.addWidget(hline())

        # ── Configuración de columna nueva ────────────────────────────────────
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
        self.lbl_status = QLabel("Cargá un archivo CSV para comenzar.")
        self.lbl_status.setWordWrap(True)
        layout.addWidget(self.lbl_status)

    # ── Carga de archivo ──────────────────────────────────────────────────────

    def _load_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Abrir CSV", "", "CSV (*.csv);;Todos (*)")
        if not path:
            return
        self.lbl_file.setText(f"Cargando {Path(path).name}…")
        self.lbl_file.setStyleSheet("color: gray;")
        QApplication.setOverrideCursor(QCursor(Qt.CursorShape.WaitCursor))
        self._loader = FileLoaderWorker(path)
        self._loader.done.connect(self._on_file_loaded)
        self._loader.error.connect(self._on_file_error)
        self._loader.start()

    def _on_file_loaded(self, path, enc, delim, columns, df):
        QApplication.restoreOverrideCursor()
        self._filepath = path
        self._enc      = enc
        self._delim    = delim
        self._columns  = columns

        name = Path(path).name
        self.lbl_file.setText(f"📄  {name}  —  {len(df):,} filas × {len(columns)} columnas  ({enc})")
        self.lbl_file.setStyleSheet("")

        self._populate_controls(columns)

        model = PandasTableModel(df, self._palette)
        self.table.set_model(model)
        self.table.apply_palette(self._palette)
        self.table.setVisible(True)
        self.lbl_status.setText(f"✓ Archivo cargado: {len(df):,} filas en preview.")
        self.lbl_status.setStyleSheet("")

    def _on_file_error(self, msg):
        QApplication.restoreOverrideCursor()
        self.lbl_file.setText("Error al cargar el archivo.")
        self.lbl_file.setStyleSheet("color: #ef4444;")
        QMessageBox.critical(self, "Error", f"No se pudo cargar el archivo:\n{msg}")

    def _populate_controls(self, columns: list):
        self.combo_model.blockSignals(True)
        self.combo_model.clear()
        self.combo_model.addItems(columns)
        model_col = next((c for c in _MODEL_CANDIDATES if c in columns), None)
        if model_col:
            self.combo_model.setCurrentText(model_col)
        self.combo_model.blockSignals(False)
        self._on_model_col_changed(self.combo_model.currentText())

        self.combo_after.clear()
        self.combo_after.addItems(columns)

        dest = str(Path(self._filepath).parent / "CSV_con_columna_agregada")
        self.lbl_dest.setText(dest)

    def _on_model_col_changed(self, col: str):
        if not col or not self._filepath:
            self.lbl_model_badge.setText("")
            return
        if col in _MODEL_CANDIDATES:
            self.lbl_model_badge.setText(
                f"Se validará que '{col}' sea uniforme en todas las filas antes de agregar."
            )
            self.lbl_model_badge.setStyleSheet("color: #3b82f6; font-size: 9pt;")
        else:
            self.lbl_model_badge.setText(
                f"⚠ '{col}' no es una columna de modelo conocida — se validará igual."
            )
            self.lbl_model_badge.setStyleSheet("color: #f59e0b; font-size: 9pt;")

    # ── Compatibilidad con app.py (carga desde botón principal) ───────────────

    def setup(self, filepath, enc, delim, columns, palette: dict = None):
        self._filepath = filepath
        self._enc      = enc
        self._delim    = delim
        self._columns  = columns
        if palette:
            self._palette = palette
        self._populate_controls(columns)
        name = Path(filepath).name
        self.lbl_file.setText(f"📄  {name}  ({enc})")
        self.lbl_file.setStyleSheet("")

    def set_preview(self, df: pd.DataFrame, palette: dict):
        self._palette = palette
        if df is not None and not df.empty:
            model = PandasTableModel(df, palette)
            self.table.set_model(model)
            self.table.apply_palette(palette)
            self.table.setVisible(True)

    # ── Utilidades ────────────────────────────────────────────────────────────

    def _to_upper(self, line_edit: QLineEdit):
        text = line_edit.text()
        upper = text.upper()
        if text != upper:
            pos = line_edit.cursorPosition()
            line_edit.setText(upper)
            line_edit.setCursorPosition(pos)

    def _get_position(self):
        if self.rb_start.isChecked(): return "start", None
        if self.rb_after.isChecked(): return "after", self.combo_after.currentText()
        return "end", None

    # ── Vista previa ──────────────────────────────────────────────────────────

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
                self._filepath, encoding=self._enc, delimiter=self._delim,
                nrows=PREVIEW_ROWS, engine="c",
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
            self.lbl_status.setText(f"✓ Preview: columna '{col_name}' = '{col_val}'")
            self.lbl_status.setStyleSheet("color: #3b82f6;")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo generar preview: {e}")

    # ── Proceso principal: validar → agregar columna ──────────────────────────

    def _start(self):
        if not self._filepath:
            QMessageBox.warning(self, "Sin archivo", "Cargá un archivo CSV primero.")
            return
        col_name  = self.entry_name.text().strip()
        col_val   = self.entry_value.text().strip()
        if not col_name:
            QMessageBox.warning(self, "Sin nombre", "Ingresá el nombre de la columna.")
            return
        model_col = self.combo_model.currentText()
        if not model_col:
            QMessageBox.warning(self, "Sin columna", "Seleccioná la columna de modelo a validar.")
            return

        pos, after_col = self._get_position()
        out = str(Path(self._filepath).parent / "CSV_con_columna_agregada")

        fp, enc, delim = self._filepath, self._enc, self._delim

        def validate_and_add(cancel_fn, progress_cb):
            # ── Fase 1: validar uniformidad del modelo ────────────────────────
            progress_cb(0, f"Validando '{model_col}'…")
            try:
                ok, first_val, bad_val = validate_uniform_column(fp, enc, delim, model_col)
            except Exception as e:
                raise RuntimeError(f"Error al validar columna '{model_col}': {e}")

            if not ok:
                raise RuntimeError(
                    f"La columna '{model_col}' tiene valores diferentes.\n"
                    f"  Primer valor encontrado:  '{first_val}'\n"
                    f"  Valor distinto encontrado: '{bad_val}'\n\n"
                    f"El archivo debe tener un único modelo por archivo para poder agregar la columna."
                )

            if cancel_fn():
                return None

            progress_cb(5, f"✓ '{model_col}' uniforme: '{first_val}'. Procesando…")

            # ── Fase 2: agregar columna ───────────────────────────────────────
            def _cb(frac, msg):
                # mapear 0-1 de add_column_to_csv al rango 5-100
                pct = 5 + int(frac * 95)
                progress_cb(pct, msg)

            add_column_to_csv(
                filepath=fp, encoding=enc, delimiter=delim,
                column_name=col_name, column_value=col_val,
                position=pos, after_column=after_col,
                output_dir=out,
                progress_callback=_cb,
            )
            return out

        self._worker = GenericWorker(validate_and_add)
        self._worker.progress.connect(lambda p, m: (self.progress.set(p), self.lbl_status.setText(m)))
        self._worker.done.connect(lambda d: self._finish(f"✓ Guardado en: {d}", "success", d))
        self._worker.error.connect(lambda e: self._on_error(e))
        self._worker.cancelled.connect(lambda: self._finish("⚠ Cancelado.", "warning", ""))
        self.btn_add.setEnabled(False)
        self.progress.reset()
        self.lbl_status.setStyleSheet("")
        QApplication.setOverrideCursor(QCursor(Qt.CursorShape.WaitCursor))
        self._worker.start()

    def _on_error(self, msg: str):
        QApplication.restoreOverrideCursor()
        self.btn_add.setEnabled(True)
        if "valores diferentes" in msg or "uniforme" in msg.lower():
            # Error de validación: mostrar como warning prominente
            self.lbl_status.setText(msg.split("\n")[0])
            self.lbl_status.setStyleSheet("color: #ef4444;")
            QMessageBox.warning(self, "No se puede agregar la columna", msg)
        else:
            self._finish(f"✗ {msg}", "error", "")

    def _finish(self, msg, state, out_dir):
        QApplication.restoreOverrideCursor()
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
                    df_result = pd.read_csv(str(out_file), encoding="utf-8-sig",
                                            nrows=PREVIEW_ROWS, engine="c")
                    model = PandasTableModel(df_result, self._palette)
                    self.table.set_model(model)
                    self.table.apply_palette(self._palette)
                    self.table.setVisible(True)
            except Exception:
                pass

    def _open_dest(self):
        if self._last_dir and os.path.isdir(self._last_dir):
            os.startfile(self._last_dir)

    def apply_palette(self, p: dict):
        self._palette = p
        self.table.apply_palette(p)
