"""
ui/tabs/search.py — Tab de búsqueda multi-archivo con exportación de resultados.
"""

import os
from pathlib import Path

import pandas as pd
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QFont, QCursor, QKeySequence, QShortcut
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QComboBox, QPlainTextEdit, QRadioButton, QButtonGroup,
    QMessageBox, QApplication,
)

from constants import SEARCH_PRESET_CANONICAL, SEARCH_FILE_PALETTE
from processor import sanitize_filename
from ui.theme import DARK, LIGHT
from ui.workers import SearchWorker, GenericWorker
from ui.widgets import DataTable, PandasTableModel, ProgressRow, hline


class SearchTab(QWidget):
    def __init__(self, files_panel, parent=None):
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
        self._search_warnings: list = []

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
        self.combo_col = QComboBox()
        self.combo_col.setMinimumWidth(160)
        ctrl_row.addWidget(self.combo_col)
        ctrl_row.addSpacing(12)
        self.btn_search = QPushButton("🔍  Buscar")
        self.btn_search.setObjectName("success")
        self.btn_search.setFixedHeight(38)
        self.btn_cancel = QPushButton("✕  Cancelar")
        self.btn_cancel.setObjectName("danger")
        self.btn_cancel.setFixedHeight(38)
        self.btn_cancel.setEnabled(False)
        self.btn_clear = QPushButton("🗑  Limpiar")
        self.btn_clear.setFixedHeight(38)
        self.btn_search.clicked.connect(self._start)
        self.btn_cancel.clicked.connect(self._cancel_search)
        self.btn_clear.clicked.connect(self._clear)
        ctrl_row.addWidget(self.btn_search)
        ctrl_row.addWidget(self.btn_cancel)
        ctrl_row.addWidget(self.btn_clear)
        ctrl_row.addStretch()
        layout.addLayout(ctrl_row)

        col_filter_row = QHBoxLayout()
        col_filter_row.addWidget(QLabel("Columnas en resultados:"))
        self._col_filter_group = QButtonGroup(self)
        self.rb_cols_preset = QRadioButton("Preseleccionadas")
        self.rb_cols_preset.setChecked(True)
        self.rb_cols_all = QRadioButton("Todas")
        for rb in (self.rb_cols_preset, self.rb_cols_all):
            self._col_filter_group.addButton(rb)
            col_filter_row.addWidget(rb)
        self.rb_cols_preset.clicked.connect(self._apply_col_filter)
        self.rb_cols_all.clicked.connect(self._apply_col_filter)
        col_filter_row.addStretch()
        layout.addLayout(col_filter_row)

        self.progress = ProgressRow()
        layout.addWidget(self.progress)
        self.lbl_status = QLabel("Agregá archivos CSV en el panel izquierdo e ingresá un valor a buscar.")
        self.lbl_status.setWordWrap(True)
        self.lbl_status.setStyleSheet("color: gray;")
        layout.addWidget(self.lbl_status)

        self.table = DataTable()
        layout.addWidget(self.table, stretch=1)

        layout.addWidget(hline())

        exp_lbl = QLabel("Exportar resultados")
        exp_lbl.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        layout.addWidget(exp_lbl)

        exp_row1 = QHBoxLayout()
        exp_row1.addWidget(QLabel("Carpetas por:"))
        self.combo_exp_folder = QComboBox()
        self.combo_exp_folder.setMinimumWidth(150)
        exp_row1.addWidget(self.combo_exp_folder)
        exp_row1.addSpacing(16)
        exp_row1.addWidget(QLabel("Archivos por:"))
        self.combo_exp_file = QComboBox()
        self.combo_exp_file.setMinimumWidth(150)
        exp_row1.addWidget(self.combo_exp_file)
        exp_row1.addStretch()
        layout.addLayout(exp_row1)

        exp_row2 = QHBoxLayout()
        self.btn_exp_json = QPushButton("💾  Exportar JSON")
        self.btn_exp_json.setObjectName("success")
        self.btn_exp_json.setFixedHeight(38)
        self.btn_exp_csv = QPushButton("📄  Exportar CSV")
        self.btn_exp_csv.setFixedHeight(38)
        self.btn_exp_open = QPushButton("📂  Abrir carpeta")
        self.btn_exp_open.setFixedHeight(38)
        self.btn_exp_open.setEnabled(False)
        for b in (self.btn_exp_json, self.btn_exp_csv, self.btn_exp_open):
            exp_row2.addWidget(b)
        exp_row2.addStretch()
        self.btn_exp_json.clicked.connect(lambda: self._export("json"))
        self.btn_exp_csv.clicked.connect(lambda: self._export("csv"))
        self.btn_exp_open.clicked.connect(self._open_export_dir)
        layout.addLayout(exp_row2)

        self.exp_progress = ProgressRow()
        layout.addWidget(self.exp_progress)

    _PRESET_CANONICAL = SEARCH_PRESET_CANONICAL
    _CANONICAL_SET    = {canon for canon, _ in SEARCH_PRESET_CANONICAL}

    def _col_to_canonical(self, col: str) -> str:
        for canonical, candidates in self._PRESET_CANONICAL:
            if col in candidates:
                return canonical
        return col

    def set_columns(self, columns: list):
        self._all_detected_cols = columns
        self._refresh_col_combo()

    def _populate_col_combo(self, columns: list):
        self.combo_col.blockSignals(True)
        self.combo_col.clear()
        self.combo_col.addItems(columns)
        if "SN" in columns:
            self.combo_col.setCurrentText("SN")
        self.combo_col.blockSignals(False)

    def _on_files_changed(self):
        from processor import detect_encoding, detect_delimiter, get_columns
        files      = self._files_panel.get_files()
        extra_maps = self._files_panel.get_extra_maps()
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
                file_extra = extra_maps.get(fp, {})
                for c in get_columns(fp, enc, delim):
                    canonical = file_extra.get(c) or self._col_to_canonical(c)
                    if canonical not in seen:
                        seen.add(canonical)
                        all_cols.append(canonical)
            except Exception:
                pass
        self._all_detected_cols = all_cols
        self._refresh_col_combo()

    def _refresh_col_combo(self):
        if not self._all_detected_cols:
            return
        if self.rb_cols_preset.isChecked():
            cols = [c for c in self._all_detected_cols if c in self._CANONICAL_SET]
            if not cols:
                cols = self._all_detected_cols
        else:
            cols = self._all_detected_cols
        self._populate_col_combo(cols)

    def _parse_values(self) -> list:
        raw = self.txt_input.toPlainText()
        return [v.strip() for v in raw.replace("\n", ",").split(",") if v.strip()]

    def _start(self):
        files = self._files_panel.get_files()
        if not files:
            QMessageBox.warning(self, "Sin archivos", "Agregá archivos CSV en el panel izquierdo.")
            return
        values = self._parse_values()
        if not values:
            QMessageBox.warning(self, "Sin valor", "Ingresá al menos un valor a buscar.")
            return
        col = self.combo_col.currentText()
        col_candidates = next(
            (candidates for canonical, candidates in self._PRESET_CANONICAL if canonical == col),
            [col],
        )
        self._last_searched = values
        self._search_warnings = []
        self.txt_not_found.clear()
        extra_maps   = self._files_panel.get_extra_maps()
        self._worker = SearchWorker(files, values, col_candidates, self._PRESET_CANONICAL, extra_maps)
        self._worker.progress.connect(self._on_progress)
        self._worker.done.connect(self._on_done)
        self._worker.error.connect(self._on_error)
        self._worker.warning.connect(lambda msg: self._search_warnings.append(msg))
        self._worker.cancelled.connect(lambda: self._finish_search("⚠ Cancelado.", "warning"))
        self.btn_search.setEnabled(False)
        self.btn_cancel.setEnabled(True)
        self.progress.reset()
        self._set_status("Buscando…", "muted")
        QApplication.setOverrideCursor(QCursor(Qt.CursorShape.WaitCursor))
        self._worker.start()

    def _cancel_search(self):
        if self._worker:
            self._worker.cancel()

    def _on_progress(self, pct, msg):
        self.progress.set(pct)
        self._set_status(msg, "muted")

    def _on_done(self, results: list):
        self._results = results
        self._populate_table(results)
        count = len(results)
        if self._search_warnings:
            n = len(self._search_warnings)
            self._finish_search(f"✓ {count} resultado(s). ⚠ {n} archivo(s) con errores.", "warning")
        else:
            self._finish_search(f"✓ {count} resultado(s) encontrado(s).", "success")
        self._show_not_found(results)
        QApplication.restoreOverrideCursor()

    def _show_not_found(self, results: list):
        searched = getattr(self, "_last_searched", [])
        if not searched:
            return
        col   = self.combo_col.currentText()
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
        self.btn_search.setEnabled(True)
        self.btn_cancel.setEnabled(False)
        self._set_status(msg, state)
        QApplication.restoreOverrideCursor()

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
            self._result_df         = pd.DataFrame()
            self._result_row_colors = []
            return
        skip = {"__file__", "__filepath__", "__palette__"}
        seen_cols: set = set()
        data_cols: list = []
        for r in results:
            for k in r:
                if k not in skip and k not in seen_cols:
                    seen_cols.add(k)
                    data_cols.append(k)

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
            if ordered:
                df = df[["Archivo"] + ordered]

        palette = DARK if QApplication.instance().property("dark_mode") else LIGHT
        model = PandasTableModel(df, palette, self._result_row_colors)
        self.table.set_model(model)

        cols = df.columns.tolist()
        for combo in (self.combo_exp_folder, self.combo_exp_file):
            combo.blockSignals(True)
            combo.clear()
            combo.addItems(cols)
            combo.blockSignals(False)
        _FOLDER = ["PHONEMODEL_NAME", "PHONE_MODEL", "MODEL_NAME", "MODEL", "PHONEMODEL"]
        _FILE   = ["SN", "STR_PSN_1", "SN_1", "PSN", "SERIAL"]
        for candidates, combo in ((_FOLDER, self.combo_exp_folder), (_FILE, self.combo_exp_file)):
            for c in candidates:
                if c in cols:
                    combo.setCurrentText(c)
                    break

    def _clear(self):
        self._results       = []
        self._result_df     = None
        palette = DARK if QApplication.instance().property("dark_mode") else LIGHT
        self.table.set_model(PandasTableModel(pd.DataFrame(), palette))
        self.progress.reset()
        self.txt_not_found.clear()
        self.txt_not_found.setStyleSheet("")
        self._set_status("Resultados limpiados.", "muted")

    def _set_status(self, msg, state):
        colors = {"success": "#22c55e", "error": "#ef4444", "warning": "#f59e0b", "muted": "gray"}
        self.lbl_status.setText(msg)
        self.lbl_status.setStyleSheet(f"color: {colors.get(state, 'gray')};")

    def _export(self, fmt: str):
        if self._result_df is None or self._result_df.empty:
            QMessageBox.warning(self, "Sin resultados", "Realizá una búsqueda primero.")
            return
        folder_col = self.combo_exp_folder.currentText()
        file_col   = self.combo_exp_file.currentText()
        if not folder_col:
            QMessageBox.warning(self, "Sin columna", "Seleccioná la columna para carpetas.")
            return

        files   = self._files_panel.get_files()
        base    = Path(files[0]).parent if files else Path.home()
        out_dir = str(base / "search_output")

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
                        data  = file_df.drop(columns=["Archivo"], errors="ignore").to_dict(orient="records")
                        with open(fpath, "w", encoding="utf-8") as f:
                            _json.dump(data, f, ensure_ascii=False, indent=2)
                    else:
                        fpath = folder_path / f"{fname}.csv"
                        file_df.to_csv(fpath, index=False, encoding="utf-8-sig")
                progress_cb(int((i + 1) / total * 100), f"{folder_val}…")
            return out_dir

        self._export_worker = GenericWorker(export_fn, df, folder_col, file_col, out_dir, fmt)
        self._export_worker.progress.connect(lambda p, _: self.exp_progress.set(p))
        self._export_worker.done.connect(lambda d: (
            self.exp_progress.set(100),
            setattr(self, "_last_export_dir", d),
            self.btn_exp_open.setEnabled(True),
        ))
        self._export_worker.error.connect(lambda e: QMessageBox.critical(self, "Error", e))
        self._export_worker.start()

    def _open_export_dir(self):
        if self._last_export_dir and os.path.isdir(self._last_export_dir):
            os.startfile(self._last_export_dir)

    def apply_palette(self, p: dict):
        self.table.apply_palette(p)
        text = self.txt_not_found.toPlainText()
        if text:
            color = p["success"] if text.startswith("✓") else p["error"]
            self.txt_not_found.setStyleSheet(
                f"color: {color}; background-color: {p['surface']}; border: 1px solid {p['border']};"
            )
        else:
            self.txt_not_found.setStyleSheet(
                f"background-color: {p['surface']}; border: 1px solid {p['border']};"
            )
