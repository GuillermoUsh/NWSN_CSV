"""
ui/widgets.py — Widgets reutilizables: modelos de tabla, paneles y diálogos.
"""

from pathlib import Path

import pandas as pd
from PyQt6.QtCore import Qt, QAbstractTableModel, QModelIndex, QSortFilterProxyModel, pyqtSignal
from PyQt6.QtGui import QColor, QFont, QKeySequence, QCursor, QShortcut
from PyQt6.QtWidgets import (
    QWidget, QDialog, QVBoxLayout, QHBoxLayout, QGridLayout,
    QScrollArea, QTableView, QHeaderView, QAbstractItemView,
    QLabel, QPushButton, QComboBox, QCheckBox, QProgressBar,
    QFrame, QSizePolicy, QFileDialog, QApplication,
)

from constants import PRESET_COLUMNS, SEARCH_PRESET_CANONICAL
from ui.theme import DARK, table_stylesheet
from processor import detect_encoding, detect_delimiter, get_columns


# ── Modelo virtual para DataFrames grandes ────────────────────────────────────

class PandasTableModel(QAbstractTableModel):
    def __init__(self, df: pd.DataFrame, palette: dict, row_colors: list = None, parent=None):
        super().__init__(parent)
        self._df         = df
        self._p          = palette
        self._row_colors = row_colors or []

    def rowCount(self, parent=QModelIndex()): return len(self._df)
    def columnCount(self, parent=QModelIndex()): return len(self._df.columns)

    def data(self, index: QModelIndex, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None
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


# ── Tabla estilizada con sort y copiar al portapapeles ───────────────────────

class DataTable(QWidget):
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
        if self._last_src_index is None or self._model is None:
            return
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
        h.setContentsMargins(0, 2, 0, 2)
        h.setSpacing(8)
        self.bar = QProgressBar()
        self.bar.setRange(0, 100)
        self.bar.setValue(0)
        self.bar.setFixedHeight(8)
        self.bar.setTextVisible(False)
        self.lbl = QLabel("0%")
        self.lbl.setFixedWidth(36)
        self.lbl.setStyleSheet("font-size: 9pt; color: gray;")
        h.addWidget(self.bar, stretch=1)
        h.addWidget(self.lbl)

    def set(self, pct: int):
        self.bar.setValue(pct)
        self.lbl.setText(f"{pct}%")

    def reset(self):
        self.bar.setValue(0)
        self.lbl.setText("0%")


# ── Separador horizontal ──────────────────────────────────────────────────────

def hline() -> QFrame:
    f = QFrame()
    f.setFrameShape(QFrame.Shape.HLine)
    return f


# ── Diálogo: mapeo de columnas preset faltantes ───────────────────────────────

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


# ── Diálogo: mapeo extendido para búsqueda multi-archivo ─────────────────────

class ColumnMappingDialog(QDialog):
    def __init__(self, filename: str, missing: list[str], available_cols: list[str], parent=None):
        super().__init__(parent)
        self.setWindowTitle("Mapeo de columnas")
        self.setMinimumWidth(460)
        self._combos: dict[str, QComboBox] = {}

        layout = QVBoxLayout(self)
        layout.setSpacing(12)

        info = QLabel(
            f"<b>{filename}</b> no tiene algunas columnas conocidas.<br>"
            f"Seleccioná con qué columna del archivo corresponde cada una:"
        )
        info.setWordWrap(True)
        layout.addWidget(info)

        grid = QGridLayout()
        grid.setHorizontalSpacing(16)
        grid.setVerticalSpacing(8)
        options = ["(omitir)"] + available_cols

        for row, canonical in enumerate(missing):
            lbl = QLabel(f"<b>{canonical}</b>")
            combo = QComboBox()
            combo.addItems(options)
            combo.setMinimumWidth(220)
            grid.addWidget(lbl,   row, 0, Qt.AlignmentFlag.AlignRight)
            grid.addWidget(combo, row, 1)
            self._combos[canonical] = combo

        layout.addLayout(grid)
        layout.addWidget(hline())

        btn_row = QHBoxLayout()
        btn_skip = QPushButton("Omitir todo")
        btn_ok   = QPushButton("Aceptar")
        btn_ok.setObjectName("accent")
        btn_ok.setFixedHeight(34)
        btn_skip.setFixedHeight(34)
        btn_skip.clicked.connect(self.reject)
        btn_ok.clicked.connect(self.accept)
        btn_row.addStretch()
        btn_row.addWidget(btn_skip)
        btn_row.addWidget(btn_ok)
        layout.addLayout(btn_row)

    def get_mapping(self) -> dict[str, str]:
        """Retorna {col_real_del_archivo: nombre_canonico} para las selecciones no omitidas."""
        return {
            combo.currentText(): canonical
            for canonical, combo in self._combos.items()
            if combo.currentText() != "(omitir)"
        }


# ── Panel izquierdo: selección de columnas ────────────────────────────────────

class ColumnPanel(QWidget):
    columns_changed = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedWidth(270)
        self.setObjectName("ColumnPanel")
        self._checks: dict[str, QCheckBox] = {}
        self._rename_map: dict[str, str] = {}

        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(6)

        title = QLabel("Columnas de salida")
        title.setFont(QFont("Segoe UI", 11, QFont.Weight.Bold))
        layout.addWidget(title)

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

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        self._scroll_widget = QWidget()
        self._scroll_layout = QVBoxLayout(self._scroll_widget)
        self._scroll_layout.setContentsMargins(4, 4, 4, 4)
        self._scroll_layout.setSpacing(3)
        self._scroll_layout.addStretch()
        scroll.setWidget(self._scroll_widget)
        layout.addWidget(scroll, stretch=1)

    def set_columns(self, columns: list, rename_map: dict = None):
        self._rename_map = rename_map or {}
        while self._scroll_layout.count() > 1:
            item = self._scroll_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        self._checks.clear()
        for col in columns:
            label     = self._rename_map.get(col, col)
            is_mapped = label != col

            if is_mapped:
                # Fila con checkbox "PRESET →" + label coloreado con nombre real
                row = QWidget()
                row_h = QHBoxLayout(row)
                row_h.setContentsMargins(0, 0, 0, 0)
                row_h.setSpacing(0)
                cb = QCheckBox(f"{label}  →  ")
                cb.setChecked(True)
                cb.stateChanged.connect(lambda _: self.columns_changed.emit())
                lbl_actual = QLabel(col)
                lbl_actual.setStyleSheet("color: #f59e0b; font-weight: 600;")
                lbl_actual.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
                lbl_actual.mousePressEvent = lambda _e, c=cb: c.toggle()
                row_h.addWidget(cb)
                row_h.addWidget(lbl_actual)
                row_h.addStretch()
                self._checks[col] = cb
                self._scroll_layout.insertWidget(self._scroll_layout.count() - 1, row)
            else:
                cb = QCheckBox(col)
                cb.setChecked(True)
                cb.stateChanged.connect(lambda _: self.columns_changed.emit())
                self._checks[col] = cb
                self._scroll_layout.insertWidget(self._scroll_layout.count() - 1, cb)

    def get_selected(self) -> list:
        return [col for col, cb in self._checks.items() if cb.isChecked()]

    def get_rename_map(self) -> dict:
        return {col: self._rename_map[col] for col in self.get_selected() if col in self._rename_map}

    def _select_all(self):
        for cb in self._checks.values():
            cb.setChecked(True)

    def _apply_preset(self):
        for col, cb in self._checks.items():
            mapped = self._rename_map.get(col, col)
            cb.setChecked(mapped in PRESET_COLUMNS or col in PRESET_COLUMNS)


# ── Panel izquierdo: lista de archivos para búsqueda ─────────────────────────

class SearchFilesPanel(QWidget):
    files_changed = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedWidth(270)
        self.setObjectName("SearchFilesPanel")
        self._files: list[str] = []
        self._file_extra_maps: dict[str, dict[str, str]] = {}

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
    def get_extra_maps(self) -> dict: return dict(self._file_extra_maps)

    def _add_files(self):
        paths, _ = QFileDialog.getOpenFileNames(self, "Agregar CSVs", "", "CSV (*.csv);;Todos (*)")
        for p in paths:
            if p not in self._files:
                self._files.append(p)
                self._check_and_map(p)
        self._render()
        self.files_changed.emit()

    def _check_and_map(self, filepath: str):
        """Detecta columnas preset faltantes y muestra diálogo de mapeo si es necesario."""
        try:
            enc      = detect_encoding(filepath)
            delim    = detect_delimiter(filepath, enc)
            cols     = get_columns(filepath, enc, delim)
            cols_set = set(cols)
            missing  = [
                canonical
                for canonical, candidates in SEARCH_PRESET_CANONICAL
                if not any(c in cols_set for c in candidates)
            ]
            if not missing:
                return
            dlg = ColumnMappingDialog(Path(filepath).name, missing, cols, self)
            if dlg.exec():
                mapping = dlg.get_mapping()  # {col_real: canonical}
                if mapping:
                    self._file_extra_maps[filepath] = mapping
        except Exception:
            pass

    def _clear(self):
        self._files.clear()
        self._file_extra_maps.clear()
        self._render()
        self.files_changed.emit()

    def _remove(self, path: str):
        self._files = [f for f in self._files if f != path]
        self._file_extra_maps.pop(path, None)
        self._render()
        self.files_changed.emit()

    def _render(self):
        while self._list_layout.count() > 1:
            item = self._list_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        for fp in self._files:
            row_w = QWidget()
            row_h = QHBoxLayout(row_w)
            row_h.setContentsMargins(0, 0, 0, 0)
            lbl = QLabel(Path(fp).name)
            lbl.setToolTip(fp)
            lbl.setWordWrap(False)
            btn_x = QLabel("✕")
            btn_x.setFixedSize(22, 22)
            btn_x.setAlignment(Qt.AlignmentFlag.AlignCenter)
            btn_x.setStyleSheet("color: #f87171; font-weight: bold; font-size: 13pt;")
            btn_x.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
            btn_x.setToolTip("Quitar de la lista")
            btn_x.mousePressEvent = lambda e, f=fp: self._remove(f)
            row_h.addWidget(lbl, stretch=1)
            row_h.addWidget(btn_x)
            self._list_layout.insertWidget(self._list_layout.count() - 1, row_w)
