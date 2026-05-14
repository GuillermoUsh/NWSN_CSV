"""
ui/tabs/export_json.py — Tab de exportación a archivos JSON agrupados.
"""

import os
from pathlib import Path

import pandas as pd
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QComboBox, QTextEdit, QMessageBox, QApplication,
)

from constants import PRESET_TO_JSON_KEY
from processor import sanitize_filename
from ui.workers import GenericWorker
from ui.widgets import ProgressRow, hline


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
        layout.setContentsMargins(10, 8, 10, 8)
        layout.setSpacing(6)

        row1 = QHBoxLayout()
        row1.addWidget(QLabel("Carpetas por:"))
        self.combo_folder = QComboBox()
        self.combo_folder.setMinimumWidth(180)
        self.combo_folder.currentTextChanged.connect(self._update_preview)
        row1.addWidget(self.combo_folder)
        row1.addSpacing(20)
        row1.addWidget(QLabel("Archivos (JSON) por:"))
        self.combo_file = QComboBox()
        self.combo_file.setMinimumWidth(180)
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
        self.txt_preview.setFixedHeight(150)
        self.txt_preview.setPlaceholderText("Vista previa del formato JSON…")
        layout.addWidget(self.txt_preview)

        self.lbl_files_info = QLabel("")
        self.lbl_files_info.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        self.lbl_files_info.setWordWrap(True)
        layout.addWidget(self.lbl_files_info)

        layout.addWidget(hline())

        dest_row = QHBoxLayout()
        dest_row.addWidget(QLabel("📁 Destino:"))
        self.lbl_dest = QLabel("—")
        self.lbl_dest.setStyleSheet("color: gray;")
        dest_row.addWidget(self.lbl_dest, stretch=1)
        layout.addLayout(dest_row)

        actions = QHBoxLayout()
        self.btn_export = QPushButton("🗂  Exportar JSON")
        self.btn_export.setObjectName("success")
        self.btn_export.setFixedHeight(32)
        self.btn_cancel = QPushButton("✕  Cancelar")
        self.btn_cancel.setObjectName("danger")
        self.btn_cancel.setFixedHeight(32)
        self.btn_cancel.setEnabled(False)
        self.btn_open_dest = QPushButton("📂  Abrir carpeta")
        self.btn_open_dest.setFixedHeight(32)
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

    _FOLDER_CANDIDATES = ["PHONEMODEL_NAME", "PHONE_MODEL", "MODEL_NAME", "MODEL", "PHONEMODEL"]
    _FILE_CANDIDATES   = ["SN", "STR_PSN_1", "SN_1", "PSN", "SERIAL"]

    def setup(self, filepath, enc, delim, columns, get_selected_fn, rename_map: dict = None, df=None):
        self._filepath             = filepath
        self._enc                  = enc
        self._delim                = delim
        self._all_cols             = columns
        self._get_selected_cols_fn = get_selected_fn
        rename_map    = rename_map or {}
        preset_to_col = {v: k for k, v in rename_map.items()}

        for combo in (self.combo_folder, self.combo_file):
            combo.blockSignals(True)
            combo.clear()
            combo.addItems(columns)
            combo.blockSignals(False)

        folder_col = self._find_col(columns, self._FOLDER_CANDIDATES, preset_to_col)
        if folder_col:
            self.combo_folder.setCurrentText(folder_col)

        file_col = self._find_col(columns, self._FILE_CANDIDATES, preset_to_col)
        if file_col:
            self.combo_file.setCurrentText(file_col)

        self._df = df
        dest = str(Path(filepath).parent / "json_output") if filepath else "—"
        self.lbl_dest.setText(dest)
        self._last_dir = dest
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

        sample_rows = []
        if self._df is not None and len(self._df) > 0:
            grp_col = file_col if file_col and file_col in self._df.columns else None
            if grp_col:
                first_val = self._df[grp_col].dropna().iloc[0] if len(self._df) else None
                if first_val is not None:
                    subset = self._df[self._df[grp_col] == first_val].head(3)
                    for _, row in subset.iterrows():
                        obj = {PRESET_TO_JSON_KEY.get(c, c): str(row.get(c, ""))
                               for c in cols if c in self._df.columns}
                        if obj:
                            sample_rows.append(obj)
            if not sample_rows:
                for _, row in self._df.head(3).iterrows():
                    obj = {PRESET_TO_JSON_KEY.get(c, c): str(row.get(c, ""))
                           for c in cols if c in self._df.columns}
                    if obj:
                        sample_rows.append(obj)

        if not sample_rows:
            sample_rows = [
                {PRESET_TO_JSON_KEY.get(c, c): f"VALOR_{c}_{i+1}"
                 for c in (cols or list(PRESET_TO_JSON_KEY.keys()))}
                for i in range(3)
            ]

        self.txt_preview.setPlainText(_json.dumps(sample_rows, ensure_ascii=False, indent=2))

    def _add_created_file(self, name: str):
        if name in self._created_files:
            return
        self._created_files.append(name)
        total    = len(self._created_files)
        MAX_SHOW = 5
        shown    = " · ".join(self._created_files[:MAX_SHOW])
        suffix   = f"  …+{total - MAX_SHOW} más" if total > MAX_SHOW else ""
        self.lbl_files_info.setText(f"📂 {total} carpeta(s):  {shown}{suffix}")
        app  = QApplication.instance()
        dark = bool(app and app.property("dark_mode"))
        self.lbl_files_info.setStyleSheet(
            "color: #4ade80; padding: 2px 0;" if dark else "color: #166534; padding: 2px 0;"
        )

    def _start(self):
        if not self._filepath:
            QMessageBox.warning(self, "Sin archivo", "Cargá un archivo CSV primero.")
            return
        folder_col = self.combo_folder.currentText()
        if not folder_col:
            QMessageBox.warning(self, "Sin columna", "Seleccioná la columna para carpetas.")
            return
        file_col = self.combo_file.currentText()
        columns  = self._get_selected_cols_fn() if self._get_selected_cols_fn else self._all_cols
        out      = str(Path(self._filepath).parent / "json_output")

        def export_fn(fp, enc, delim, fcol, filecol, cols, out, cancel_fn, progress_cb):
            import csv as _csv, json as _json
            groups: dict      = {}
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
                    data  = [{PRESET_TO_JSON_KEY.get(c, c): r.get(c, "") for c in cols}
                             for r in rows]
                    with open(fpath, "w", encoding="utf-8") as f:
                        _json.dump(data, f, ensure_ascii=False, indent=2)
                    done += len(rows)
                    progress_cb(int(done / max(total_rows, 1) * 100),
                                f"{folder_val}/{file_val}.json")
            return out

        self._created_files = []
        self.lbl_files_info.setText("")
        self._worker = GenericWorker(
            export_fn, self._filepath, self._enc, self._delim,
            folder_col, file_col, columns, out,
        )

        def _on_progress(p, m):
            if m.startswith("FILE:"):
                self._add_created_file(m[5:])
            else:
                self.progress.set(p)

        self._worker.progress.connect(_on_progress)
        self._worker.done.connect(lambda d: self._finish(f"✓ Exportado en: {d}", "success", d))
        self._worker.error.connect(lambda e: self._finish(f"✗ {e}", "error", ""))
        self._worker.cancelled.connect(lambda: self._finish("⚠ Cancelado.", "warning", ""))
        self.btn_export.setEnabled(False)
        self.btn_cancel.setEnabled(True)
        self.progress.reset()
        self._worker.start()

    def _cancel(self):
        if self._worker:
            self._worker.cancel()

    def _finish(self, msg, state, out_dir):
        self.btn_export.setEnabled(True)
        self.btn_cancel.setEnabled(False)
        colors = {"success": "#22c55e", "error": "#ef4444", "warning": "#f59e0b"}
        self.lbl_status.setText(msg)
        self.lbl_status.setStyleSheet(f"color: {colors.get(state, 'gray')};")
        if out_dir:
            self._last_dir = out_dir
            self.btn_open_dest.setEnabled(True)

    def _open_dest(self):
        if self._last_dir and os.path.isdir(self._last_dir):
            os.startfile(self._last_dir)
