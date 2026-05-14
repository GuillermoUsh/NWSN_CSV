"""
app.py — CSV Processor · Ventana principal.
"""

__version__ = "3.0.0"

from pathlib import Path

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QFont, QCursor
from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QSplitter, QTabWidget, QStackedWidget, QFrame,
    QLabel, QPushButton, QToolButton, QSizePolicy,
    QFileDialog, QMessageBox, QStatusBar, QApplication,
)

from constants import PRESET_COLUMNS
from ui.theme import DARK, LIGHT, app_stylesheet
from ui.workers import FileLoaderWorker
from ui.widgets import ColumnPanel, SearchFilesPanel, ColumnMapDialog
from ui.tabs.export_csv  import ExportCSVTab
from ui.tabs.export_txt  import ExportTXTTab
from ui.tabs.export_json import ExportJSONTab
from ui.tabs.search      import SearchTab
from ui.tabs.add_column  import AddColumnTab
from ui.tabs.part_name   import PartNameTab


def _hline() -> QFrame:
    f = QFrame()
    f.setFrameShape(QFrame.Shape.HLine)
    return f


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self._dark         = True
        self._p            = DARK
        self._filepath     = ""
        self._enc          = ""
        self._delim        = ""
        self._columns: list = []
        self._df_preview   = None
        self._rename_map: dict = {}
        self._loader: FileLoaderWorker | None = None

        self.setWindowTitle(f"CSV Processor v{__version__}")
        self.resize(1280, 780)
        self.setMinimumSize(1000, 660)

        self._build_ui()
        self._apply_palette()

    # ── Construcción de la UI ─────────────────────────────────────────────────

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # Barra superior
        self._top_bar = QWidget()
        top_layout    = QHBoxLayout(self._top_bar)
        top_layout.setContentsMargins(12, 8, 12, 8)
        top_layout.setSpacing(10)
        self.btn_open = QPushButton("📂  Abrir CSV")
        self.btn_open.setFixedHeight(32)
        self.btn_open.clicked.connect(self._load_file)
        self.lbl_file = QLabel("Ningún archivo cargado")
        self.lbl_file.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.lbl_file.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        self._top_spacer = QWidget()
        self._top_spacer.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.btn_theme = QToolButton()
        self.btn_theme.setText("☽")
        self.btn_theme.setObjectName("btn_theme")
        self.btn_theme.setFixedSize(36, 36)
        self.btn_theme.clicked.connect(self._toggle_theme)
        top_layout.addWidget(self.btn_open)
        top_layout.addWidget(self.lbl_file)
        top_layout.addWidget(self._top_spacer)
        top_layout.addWidget(self.btn_theme)
        root.addWidget(self._top_bar)
        root.addWidget(_hline())

        # Splitter: panel izquierdo | tabs
        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.setHandleWidth(4)

        self._left_stack = QStackedWidget()
        self._left_stack.setFixedWidth(270)
        self.col_panel   = ColumnPanel()
        self.files_panel = SearchFilesPanel()
        self._left_stack.addWidget(self.col_panel)
        self._left_stack.addWidget(self.files_panel)
        splitter.addWidget(self._left_stack)

        self.tabs         = QTabWidget()
        self.tab_csv      = ExportCSVTab()
        self.tab_txt      = ExportTXTTab()
        self.tab_json     = ExportJSONTab()
        self.tab_search   = SearchTab(self.files_panel)
        self.tab_addcol   = AddColumnTab()
        self.tab_partname = PartNameTab()

        self.tabs.addTab(self.tab_csv,      "Exportar CSV")
        self.tabs.addTab(self.tab_txt,      "Exportar SN a TXT")
        self.tabs.addTab(self.tab_json,     "Exportar JSON")
        self.tabs.addTab(self.tab_search,   "Buscar")
        self.tabs.addTab(self.tab_addcol,   "Agregar Columna")
        self.tabs.addTab(self.tab_partname, "Part Name")
        self.tabs.currentChanged.connect(self._on_tab_changed)
        splitter.addWidget(self.tabs)
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)

        root.addWidget(splitter, stretch=1)

        self.status = QStatusBar()
        self.setStatusBar(self.status)

        self.col_panel.columns_changed.connect(self._on_columns_changed)

    def _on_tab_changed(self, index: int):
        tab_name    = self.tabs.tabText(index).strip()
        is_search   = tab_name == "Buscar"
        is_addcol   = tab_name == "Agregar Columna"
        hide_loader = is_search or is_addcol

        if is_search:
            self._left_stack.setCurrentIndex(1)
            self._left_stack.setVisible(True)
        elif is_addcol:
            self._left_stack.setVisible(False)
        else:
            self._left_stack.setCurrentIndex(0)
            self._left_stack.setVisible(True)

        self.btn_open.setVisible(not hide_loader)
        self.lbl_file.setVisible(not hide_loader)
        self._top_spacer.setVisible(hide_loader)

    # ── Carga de archivo ──────────────────────────────────────────────────────

    def _load_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Abrir CSV", "", "CSV (*.csv);;Todos (*)")
        if not path:
            return
        self.lbl_file.setText(f"Cargando {Path(path).name}…")
        QApplication.setOverrideCursor(QCursor(Qt.CursorShape.WaitCursor))
        self._loader = FileLoaderWorker(path)
        self._loader.done.connect(self._on_file_loaded)
        self._loader.error.connect(self._on_file_error)
        self._loader.start()

    def _on_file_loaded(self, path, enc, delim, columns, df):
        QApplication.restoreOverrideCursor()
        self._filepath   = path
        self._enc        = enc
        self._delim      = delim
        self._columns    = columns
        self._df_preview = df
        self._rename_map = {}

        missing = [c for c in PRESET_COLUMNS if c not in columns]
        if missing:
            dlg = ColumnMapDialog(missing, columns, self)
            dlg.exec()
            if not dlg.cancelled:
                self._rename_map = {v: k for k, v in dlg.result_map.items() if v}

        name = Path(path).name
        self.lbl_file.setText(
            f"📄  {name}  —  {len(df):,} filas × {len(df.columns)} columnas  ({enc})"
        )

        self.col_panel.set_columns(columns, self._rename_map)
        self.tab_csv.setup(path, enc, delim, self.col_panel.get_selected, self.col_panel.get_rename_map)
        self.tab_csv.set_preview(df, self._p)
        self.tab_txt.setup(path, enc, delim, columns, self._rename_map, df)
        self.tab_json.setup(path, enc, delim, columns, self.col_panel.get_selected, self._rename_map, df)
        self.tab_search.set_columns(columns)
        self.tab_partname.setup(df, columns, self._p, path, enc, delim)

        self.status.showMessage(f"Cargado: {path}", 4000)

    def _on_file_error(self, msg):
        QApplication.restoreOverrideCursor()
        self.lbl_file.setText("Error al cargar el archivo.")
        QMessageBox.critical(self, "Error", f"No se pudo cargar el archivo:\n{msg}")

    def _on_columns_changed(self):
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
        self.btn_theme.setText("☽" if self._dark else "☀")
        QApplication.instance().setProperty("dark_mode", self._dark)
        self._apply_palette()

    def _apply_palette(self):
        p = self._p
        QApplication.instance().setStyleSheet(app_stylesheet(p))
        self.tab_csv.apply_palette(p)
        self.tab_search.apply_palette(p)
        self.tab_addcol.apply_palette(p)
        self.tab_partname.apply_palette(p)
        if self._df_preview is not None:
            self._on_columns_changed()
