"""
ui/tabs/export_txt.py — Tab de exportación de valores únicos a archivos TXT.
"""

import os
from pathlib import Path

from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QComboBox, QRadioButton, QButtonGroup, QTextEdit, QMessageBox, QApplication,
)

from processor import sanitize_filename
from ui.workers import GenericWorker
from ui.widgets import ProgressRow, hline


class ExportTXTTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._worker: GenericWorker | None = None
        self._filepath = self._enc = self._delim = ""
        self._last_dir    = ""
        self._df_preview  = None
        self._created_files: list = []

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)

        row1 = QHBoxLayout()
        row1.addWidget(QLabel("Columna:"))
        self.combo_col = QComboBox()
        self.combo_col.setMinimumWidth(200)
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
            if val == "plain":
                rb.setChecked(True)
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

        row2 = QHBoxLayout()
        row2.addWidget(QLabel("Agrupación:"))
        self._grp_group = QButtonGroup(self)
        self.rb_no_grp = QRadioButton("Sin agrupar")
        self.rb_no_grp.setChecked(True)
        self.rb_grp = QRadioButton("Agrupar por:")
        for rb in (self.rb_no_grp, self.rb_grp):
            self._grp_group.addButton(rb)
            row2.addWidget(rb)
        self.combo_grp = QComboBox()
        self.combo_grp.setMinimumWidth(150)
        self.combo_grp.setEnabled(False)
        self.rb_grp.toggled.connect(self.combo_grp.setEnabled)
        self.rb_grp.toggled.connect(lambda _: self._update_files_info())
        self.rb_no_grp.toggled.connect(lambda _: self._update_files_info())
        self.combo_grp.currentTextChanged.connect(lambda _: self._update_files_info())
        row2.addWidget(self.combo_grp)
        row2.addStretch()
        layout.addLayout(row2)

        dest_row = QHBoxLayout()
        dest_row.addWidget(QLabel("📁 Destino:"))
        self.lbl_dest = QLabel("—")
        self.lbl_dest.setStyleSheet("color: gray;")
        dest_row.addWidget(self.lbl_dest, stretch=1)
        layout.addLayout(dest_row)

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

    _SN_CANDIDATES  = ["SN", "STR_PSN_1", "SN_1", "PSN", "SERIAL"]
    _GRP_CANDIDATES = ["PHONEMODEL_NAME", "PHONE_MODEL", "MODEL_NAME", "MODEL", "PHONEMODEL"]

    def setup(self, filepath, enc, delim, columns, rename_map: dict = None, df=None):
        self._filepath   = filepath
        self._enc        = enc
        self._delim      = delim
        self._df_preview = df
        rename_map       = rename_map or {}
        preset_to_col    = {v: k for k, v in rename_map.items()}

        self.combo_col.blockSignals(True)
        self.combo_grp.blockSignals(True)
        self.combo_col.clear(); self.combo_col.addItems(columns)
        self.combo_grp.clear(); self.combo_grp.addItems(columns)

        sn_col = self._find_col(columns, self._SN_CANDIDATES, preset_to_col)
        if sn_col:
            self.combo_col.setCurrentText(sn_col)

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
        for c in candidates:
            if c in columns:
                return c
            if c in preset_to_col and preset_to_col[c] in columns:
                return preset_to_col[c]
        return None

    def _get_fmt(self):
        for b in self._fmt_group.buttons():
            if b.isChecked():
                return b.property("fmt_val")
        return "plain"

    def _update_preview(self):
        col     = self.combo_col.currentText()
        fmt     = self._get_fmt()
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
        self._created_files = []

    def _add_created_file(self, name: str):
        if name in self._created_files:
            return
        self._created_files.append(name)
        total    = len(self._created_files)
        MAX_SHOW = 5
        shown    = " · ".join(self._created_files[:MAX_SHOW])
        suffix   = f"  …+{total - MAX_SHOW} más" if total > MAX_SHOW else ""
        self.lbl_files_info.setText(f"📂 {total} archivo(s):  {shown}{suffix}")
        app = QApplication.instance()
        dark = bool(app and app.property("dark_mode"))
        self.lbl_files_info.setStyleSheet(
            "color: #4ade80; padding: 2px 0;" if dark else "color: #166534; padding: 2px 0;"
        )

    def _update_dest(self):
        if self._filepath:
            base = Path(self._filepath).parent / "txt_output"
            self.lbl_dest.setText(str(base))
            self._last_dir = str(base)
        else:
            self.lbl_dest.setText("—")

    def _start(self):
        if not self._filepath:
            QMessageBox.warning(self, "Sin archivo", "Cargá un archivo CSV primero.")
            return
        col = self.combo_col.currentText()
        fmt = self._get_fmt()
        grp = self.combo_grp.currentText() if self.rb_grp.isChecked() else None
        out = str(Path(self._filepath).parent / "txt_output")

        from processor import _estimate_total_rows
        total_rows = _estimate_total_rows(self._filepath)

        def export_fn(fp, enc, delim, col, fmt, grp, out, cancel_fn, progress_cb):
            import csv as _csv
            Path(out).mkdir(parents=True, exist_ok=True)

            def fmt_val(v):
                return f"'{v}',\n" if fmt == "quoted" else f"{v}\n"

            def _pct(i):
                return min(99, int(i / total_rows * 100)) if total_rows > 0 else 0

            if grp:
                handles: dict = {}
                seen: dict    = {}
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
                            if i % 5_000 == 0:
                                progress_cb(_pct(i), f"Procesando fila {i:,}…")
                finally:
                    for h in handles.values():
                        h.close()
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
                        if i % 5_000 == 0:
                            progress_cb(_pct(i), f"Procesando fila {i:,}…")
                progress_cb(100, "✓ Archivo generado")
            return out

        self._update_files_info()
        self._worker = GenericWorker(export_fn, self._filepath, self._enc, self._delim, col, fmt, grp, out)

        def _on_progress(p, m):
            if m.startswith("FILE:"):
                self._add_created_file(m[5:])
            else:
                self.progress.set(p)
                self.lbl_status.setText(m)

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

