"""
prototype_qt.py — Prototipo del tab Exportar CSV con PyQt6.
Muestra: grilla virtual con pandas, copy de celda, modo claro/oscuro.
"""

import sys
import pandas as pd
from PyQt6.QtCore import (
    Qt, QAbstractTableModel, QModelIndex, QSortFilterProxyModel
)
from PyQt6.QtGui import QColor, QFont, QKeySequence, QShortcut, QClipboard
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTableView, QLabel, QPushButton, QFileDialog, QFrame,
    QHeaderView, QAbstractItemView, QSizePolicy, QToolButton,
    QStatusBar
)

# ── Paletas ──────────────────────────────────────────────────────────────────

DARK = {
    "bg":          "#0f172a",
    "surface":     "#1e293b",
    "surface2":    "#334155",
    "border":      "#475569",
    "text":        "#f1f5f9",
    "text_muted":  "#94a3b8",
    "accent":      "#3b82f6",
    "accent_hover":"#2563eb",
    "header_bg":   "#1e3a5f",
    "header_text": "#e2e8f0",
    "row_alt":     "#1a2744",
    "row_sel":     "#2563eb",
    "row_sel_text":"#ffffff",
    "btn_bg":      "#334155",
    "btn_hover":   "#475569",
    "btn_text":    "#f1f5f9",
    "copy_bar_bg": "#1e293b",
}

LIGHT = {
    "bg":          "#f8fafc",
    "surface":     "#ffffff",
    "surface2":    "#f1f5f9",
    "border":      "#cbd5e1",
    "text":        "#0f172a",
    "text_muted":  "#64748b",
    "accent":      "#2563eb",
    "accent_hover":"#1d4ed8",
    "header_bg":   "#1e40af",
    "header_text": "#ffffff",
    "row_alt":     "#f1f5f9",
    "row_sel":     "#3b82f6",
    "row_sel_text":"#ffffff",
    "btn_bg":      "#e2e8f0",
    "btn_hover":   "#cbd5e1",
    "btn_text":    "#1e293b",
    "copy_bar_bg": "#f1f5f9",
}

# ── Modelo virtual ────────────────────────────────────────────────────────────

class PandasTableModel(QAbstractTableModel):
    """
    Modelo virtual sobre un DataFrame de pandas.
    Solo las filas visibles se renderizan — performance con 100k+ filas.
    """

    def __init__(self, df: pd.DataFrame, palette: dict, parent=None):
        super().__init__(parent)
        self._df = df
        self._p = palette

    def rowCount(self, parent=QModelIndex()) -> int:
        return len(self._df)

    def columnCount(self, parent=QModelIndex()) -> int:
        return len(self._df.columns)

    def data(self, index: QModelIndex, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None

        row, col = index.row(), index.column()

        if role == Qt.ItemDataRole.DisplayRole:
            val = self._df.iat[row, col]
            return "" if pd.isna(val) else str(val)

        if role == Qt.ItemDataRole.BackgroundRole:
            if row % 2 == 1:
                return QColor(self._p["row_alt"])
            return QColor(self._p["surface"])

        if role == Qt.ItemDataRole.ForegroundRole:
            return QColor(self._p["text"])

        if role == Qt.ItemDataRole.FontRole:
            f = QFont("Segoe UI", 10)
            return f

        return None

    def headerData(self, section: int, orientation: Qt.Orientation, role=Qt.ItemDataRole.DisplayRole):
        if role == Qt.ItemDataRole.DisplayRole:
            if orientation == Qt.Orientation.Horizontal:
                return str(self._df.columns[section])
            return str(section + 1)
        if role == Qt.ItemDataRole.FontRole:
            f = QFont("Segoe UI", 10, QFont.Weight.Bold)
            return f
        return None

    def update_palette(self, palette: dict):
        self._p = palette
        self.layoutChanged.emit()


# ── Tabla con estilo ──────────────────────────────────────────────────────────

class StyledTableView(QTableView):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectItems)
        self.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.horizontalHeader().setStretchLastSection(True)
        self.verticalHeader().setDefaultSectionSize(28)
        self.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        self.setShowGrid(True)
        self.setSortingEnabled(True)

    def apply_palette(self, p: dict):
        self.setStyleSheet(f"""
            QTableView {{
                background-color: {p['surface']};
                color: {p['text']};
                gridline-color: {p['border']};
                border: 1px solid {p['border']};
                border-radius: 6px;
                font-family: "Segoe UI";
                font-size: 10pt;
                outline: none;
            }}
            QTableView::item:selected {{
                background-color: {p['row_sel']};
                color: {p['row_sel_text']};
            }}
            QHeaderView::section {{
                background-color: {p['header_bg']};
                color: {p['header_text']};
                padding: 6px 8px;
                border: none;
                border-right: 1px solid {p['border']};
                font-family: "Segoe UI";
                font-size: 10pt;
                font-weight: bold;
            }}
            QHeaderView::section:hover {{
                background-color: {p['accent_hover']};
            }}
            QScrollBar:vertical {{
                background: {p['surface2']};
                width: 10px;
                border-radius: 5px;
            }}
            QScrollBar::handle:vertical {{
                background: {p['border']};
                border-radius: 5px;
                min-height: 30px;
            }}
            QScrollBar::handle:vertical:hover {{
                background: {p['accent']};
            }}
            QScrollBar:horizontal {{
                background: {p['surface2']};
                height: 10px;
                border-radius: 5px;
            }}
            QScrollBar::handle:horizontal {{
                background: {p['border']};
                border-radius: 5px;
                min-width: 30px;
            }}
            QScrollBar::handle:horizontal:hover {{
                background: {p['accent']};
            }}
            QScrollBar::add-line, QScrollBar::sub-line {{ width: 0; height: 0; }}
        """)


# ── Ventana principal ─────────────────────────────────────────────────────────

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self._dark = True
        self._p = DARK
        self._df: pd.DataFrame | None = None
        self._model: PandasTableModel | None = None
        self._last_cell: tuple[int, int] | None = None  # (row, col)

        self.setWindowTitle("CSV Processor — Prototipo PyQt6")
        self.resize(1100, 680)
        self._build_ui()
        self._apply_palette()

    # ── Construcción UI ───────────────────────────────────────────────────────

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(12, 10, 12, 10)
        root.setSpacing(8)

        # Top bar
        top = QHBoxLayout()
        top.setSpacing(8)

        self.btn_open = QPushButton("📂  Abrir CSV")
        self.btn_open.setFixedHeight(36)
        self.btn_open.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_open.clicked.connect(self._open_csv)

        self.lbl_file = QLabel("Ningún archivo cargado")
        self.lbl_file.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)

        self.btn_theme = QToolButton()
        self.btn_theme.setText("☀")
        self.btn_theme.setFixedSize(36, 36)
        self.btn_theme.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_theme.clicked.connect(self._toggle_theme)

        top.addWidget(self.btn_open)
        top.addWidget(self.lbl_file)
        top.addWidget(self.btn_theme)
        root.addLayout(top)

        # Separador
        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        root.addWidget(sep)

        # Tabla
        self.table = StyledTableView()
        self.table.selectionModel  # se conecta después de setModel
        root.addWidget(self.table, stretch=1)

        # Barra de copia
        copy_bar = QHBoxLayout()
        copy_bar.setSpacing(8)

        self.lbl_copy = QLabel("")
        self.lbl_copy.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.lbl_copy.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))

        self.btn_copy = QPushButton("⎘  Copiar")
        self.btn_copy.setFixedHeight(30)
        self.btn_copy.setFixedWidth(90)
        self.btn_copy.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_copy.setEnabled(False)
        self.btn_copy.clicked.connect(self._copy_cell)

        copy_bar.addWidget(self.lbl_copy)
        copy_bar.addWidget(self.btn_copy)
        root.addLayout(copy_bar)

        # Status bar
        self.status = QStatusBar()
        self.setStatusBar(self.status)

        # Ctrl+C shortcut
        sc = QShortcut(QKeySequence("Ctrl+C"), self.table)
        sc.activated.connect(self._copy_cell)

    # ── Carga de CSV ──────────────────────────────────────────────────────────

    def _open_csv(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Abrir CSV", "", "CSV Files (*.csv);;All Files (*)"
        )
        if not path:
            return

        try:
            # Detectar encoding básico
            import chardet
            with open(path, "rb") as f:
                enc = chardet.detect(f.read(50_000))["encoding"] or "utf-8"

            self._df = pd.read_csv(path, encoding=enc, dtype=str, keep_default_na=False)
            self._load_model()
            name = path.split("/")[-1].split("\\")[-1]
            self.lbl_file.setText(f"📄  {name}  —  {len(self._df):,} filas × {len(self._df.columns)} columnas")
            self.status.showMessage(f"Cargado: {path}", 4000)
        except Exception as e:
            self.status.showMessage(f"Error al abrir: {e}", 6000)

    def _load_model(self):
        if self._df is None:
            return
        self._model = PandasTableModel(self._df, self._p)
        proxy = QSortFilterProxyModel()
        proxy.setSourceModel(self._model)
        self.table.setModel(proxy)
        self.table.selectionModel().currentChanged.connect(self._on_cell_changed)
        self.table.apply_palette(self._p)
        # Reset copy bar
        self.lbl_copy.setText("")
        self.btn_copy.setEnabled(False)
        self._last_cell = None

    # ── Selección / copia ────────────────────────────────────────────────────

    def _on_cell_changed(self, current: QModelIndex, _previous: QModelIndex):
        if not current.isValid():
            self.lbl_copy.setText("")
            self.btn_copy.setEnabled(False)
            self._last_cell = None
            return

        # Mapear proxy → source
        proxy = self.table.model()
        src = proxy.mapToSource(current)
        row, col = src.row(), src.column()
        self._last_cell = (row, col)

        col_name = str(self._df.columns[col])
        value = str(self._df.iat[row, col])

        p = self._p
        self.lbl_copy.setText(f"{col_name}: \"{value}\"")
        self.lbl_copy.setStyleSheet(f"color: {p['accent']}; padding: 2px 6px;")
        self.btn_copy.setEnabled(True)

    def _copy_cell(self):
        if self._last_cell is None:
            return
        row, col = self._last_cell
        value = str(self._df.iat[row, col])
        QApplication.clipboard().setText(value)
        self.status.showMessage(f"Copiado: {value}", 2000)

    # ── Tema ─────────────────────────────────────────────────────────────────

    def _toggle_theme(self):
        self._dark = not self._dark
        self._p = DARK if self._dark else LIGHT
        self.btn_theme.setText("☀" if self._dark else "🌙")
        self._apply_palette()
        if self._model:
            self._model.update_palette(self._p)
            self.table.apply_palette(self._p)

    def _apply_palette(self):
        p = self._p
        self.setStyleSheet(f"""
            QMainWindow, QWidget {{
                background-color: {p['bg']};
                color: {p['text']};
                font-family: "Segoe UI";
            }}
            QPushButton {{
                background-color: {p['btn_bg']};
                color: {p['btn_text']};
                border: 1px solid {p['border']};
                border-radius: 6px;
                padding: 4px 14px;
                font-size: 10pt;
            }}
            QPushButton:hover {{
                background-color: {p['btn_hover']};
            }}
            QPushButton:disabled {{
                background-color: {p['surface2']};
                color: {p['text_muted']};
            }}
            QToolButton {{
                background-color: {p['btn_bg']};
                color: {p['btn_text']};
                border: 1px solid {p['border']};
                border-radius: 6px;
                font-size: 14pt;
            }}
            QToolButton:hover {{
                background-color: {p['btn_hover']};
            }}
            QLabel {{
                color: {p['text']};
                font-size: 10pt;
            }}
            QFrame[frameShape="4"] {{
                color: {p['border']};
            }}
            QStatusBar {{
                background-color: {p['surface']};
                color: {p['text_muted']};
                font-size: 9pt;
            }}
        """)
        if self._model:
            self.lbl_copy.setStyleSheet(f"color: {p['accent']}; padding: 2px 6px;")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    win = MainWindow()
    win.show()
    sys.exit(app.exec())
