"""
app.py — Interfaz gráfica con CustomTkinter para el CSV Processor.
"""

import csv
import json
import os
import queue
import threading
import tkinter as tk
from datetime import date as _date, datetime as _datetime
from tkinter import filedialog, messagebox, ttk
from pathlib import Path

import customtkinter as ctk

from processor import (
    detect_encoding,
    detect_delimiter,
    get_columns,
    get_preview,
    get_unique_column_values,
    get_unique_values_by_group,
    collect_rows_by_group,
    collect_rows_by_two_groups,
    sanitize_filename,
    process_csv,
    search_value_in_csv,
    add_column_to_csv,
)

PREVIEW_ROWS = 200
APP_VERSION  = "1.1"
HEADER_H     = 24                    # altura del canvas de encabezado en px
CENTERED_COLUMNS = {"CLASSCODE"}     # columnas cuyo contenido se centra en la preview

PRESET_COLUMNS = [
    "PHONEMODEL_NAME",
    "SN",
    "KEYUNITBARCODE",
    "CLASSCODE",
    "CREATETIME",
    "KEYMATERIAL",
]

# Mapa preset → clave camelCase que se usa como key en el JSON exportado
PRESET_TO_JSON_KEY: dict[str, str] = {
    "PHONEMODEL_NAME": "phoneModelName",
    "SN":              "sn",
    "KEYUNITBARCODE":  "keyUnitBarcode",
    "CLASSCODE":       "classCode",
    "CREATETIME":      "createTime",
    "KEYMATERIAL":     "keyMaterial",
}

# Traducción de códigos de clase → descripción completa para el JSON
CLASS_CODE_MAP: dict[str, str] = {
    "AT":    "Battery_AT",
    "KTL":   "Front_Housing_KTL",
    "HS":    "Rear_Camera_HS",
    "XB":    "Sub_PCB_XB",
    "CDQ":   "CargadorCP_CDQ",
    "BF":    "CableUsbCP_BF",
    "QS":    "Front_Camera_QS",
    "BLB":   "Modulo_Speaker_BLB",
    "BC":    "Rear_Housing_BC",
    "MFP":   "Main FPC",
    "INLAY": "Battery_Cover_INLAY",
    "AD":    "Placa_Main_AD",
    "FP":    "Finger_Print_FP",
    "MT":    "Vibrator_MT",
    "CY":    "Receiver_CY",
    "SZT":   "BracketSuperiorPN",
    "XZT":   "BracketInferiorPN",
}

# Paleta de colores para el Treeview según el modo claro/oscuro
# Paleta de colores para diferenciar resultados por archivo de origen (dark, light)
SEARCH_FILE_PALETTE = [
    ("#1e3d28", "#d4edda"),   # verde
    ("#1a2f4a", "#cce0f5"),   # azul
    ("#3d2a10", "#fde8c8"),   # naranja
    ("#2d1a3d", "#ead5f5"),   # violeta
    ("#3d1a1f", "#f5d5d8"),   # rojo/rosa
    ("#1a3d3a", "#c8ede9"),   # teal
    ("#3a3200", "#f5f0c0"),   # amarillo oscuro
    ("#1a1a3d", "#d5d5f5"),   # índigo
]

TREE_COLORS = {
    "dark": {
        "bg":         "#2b2b2b",
        "fg":         "#dce4ee",
        "header_bg":  "#1f538d",
        "selected":   "#1f538d",
        "odd_row":    "#2b2b2b",
        "even_row":   "#333333",
    },
    "light": {
        "bg":         "#ffffff",
        "fg":         "#1a1a1a",
        "header_bg":  "#3a7ebf",
        "selected":   "#3a7ebf",
        "odd_row":    "#ffffff",
        "even_row":   "#f0f4f8",
    },
}


class CSVProcessorApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title(f"CSV Processor v{APP_VERSION}")
        self.geometry("1340x820")
        self.minsize(1000, 680)

        # ── Estado interno de la app ──────────────────────────────────────────
        self.filepath           = tk.StringVar()
        self.detected_delimiter = ","
        self.detected_encoding  = "utf-8"
        self.columns:           list[str]              = []
        self.column_vars:       dict[str, tk.BooleanVar] = {}
        self.filters:           dict[str, str]         = {}
        self.output_dir         = tk.StringVar(value=str(Path.home() / "Desktop" / "csv_output"))
        self.out_delimiter      = tk.StringVar(value="comma")
        self.processing:        bool                   = False
        self._closing:          bool                   = False   # señal de cierre de ventana
        self.progress_queue:    queue.Queue            = queue.Queue()
        self.df_preview                                = None
        self.column_rename_map: dict[str, str]         = {}      # {col_real → nombre_preset}
        self.date_transforms:   dict[str, str]         = {}      # {columna  → fecha_base YYYY-MM-DD}
        self._overlay:          ctk.CTkFrame | None    = None    # panel de bloqueo durante procesamiento
        self._overlay_bar:      ctk.CTkProgressBar | None = None
        self._json_cancel:      bool                   = False   # señal de cancelación JSON
        self._txt_cancel:       bool                   = False   # señal de cancelación TXT
        self._csv_cancel:       bool                   = False   # señal de cancelación CSV
        # ── Estado pestaña Buscar ──────────────────────────────────────────────
        self.search_files:      list[str]              = []      # rutas de CSVs cargados
        self._search_cancel:    bool                   = False   # señal de cancelación búsqueda
        self.search_results:    list[dict]             = []      # resultados de la última búsqueda
        self.search_rename_map: dict[str, str]         = {}      # {col_real → nombre_preset} búsqueda
        self._search_json_last_dir: "Path | None"      = None    # última carpeta JSON exportada (buscar)

        self._setup_treeview_style()
        self._build_ui()

        # Interceptar cierre de ventana para terminar limpiamente
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    # ─────────────────────────────────────────────────────────────────────────
    # Cierre controlado
    # ─────────────────────────────────────────────────────────────────────────

    def _on_close(self):
        """Marca el cierre, detiene el polling y destruye la ventana."""
        self._closing  = True
        self.processing = False   # evita que _poll_queue re-schedule después del destroy
        self.destroy()

    # ─────────────────────────────────────────────────────────────────────────
    # Estilos Treeview
    # ─────────────────────────────────────────────────────────────────────────

    def _setup_treeview_style(self):
        """Configura colores y fuentes del Treeview según el modo claro/oscuro activo."""
        mode = ctk.get_appearance_mode().lower()
        c = TREE_COLORS.get(mode, TREE_COLORS["light"])
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure(
            "Treeview",
            background=c["bg"],
            foreground=c["fg"],
            fieldbackground=c["bg"],
            rowheight=22,
            font=("Segoe UI", 9),
        )
        style.configure(
            "Treeview.Heading",
            background=c["header_bg"],
            foreground="white",
            font=("Segoe UI", 9, "bold"),
            relief="flat",
        )
        style.map(
            "Treeview",
            background=[("selected", c["selected"])],
            foreground=[("selected", "white")],
        )
        self._tree_colors = c

    # ─────────────────────────────────────────────────────────────────────────
    # Construcción de la UI
    # ─────────────────────────────────────────────────────────────────────────

    def _build_ui(self):
        """Arma los tres bloques principales: barra superior, área central y barra inferior."""
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)
        self._build_top_bar()
        self._build_main_area()
        self._build_bottom_bar()

    # ── Barra superior: selección de archivo ─────────────────────────────────

    def _build_top_bar(self):
        """Barra superior: botón abrir CSV, etiqueta de ruta, badge de metadatos y toggle de tema."""
        top = ctk.CTkFrame(self)
        top.grid(row=0, column=0, sticky="ew", padx=12, pady=(12, 4))
        top.grid_columnconfigure(2, weight=1)
        self.top_bar = top

        ctk.CTkButton(
            top, text="📂  Abrir CSV", command=self.load_file,
            width=130, height=36, font=("Segoe UI", 12, "bold"),
        ).grid(row=0, column=0, padx=(12, 8), pady=10)

        self.file_label = ctk.CTkLabel(
            top, textvariable=self.filepath,
            anchor="w", font=("Segoe UI", 13, "bold"),
        )
        self.file_label.grid(row=0, column=1, columnspan=2, sticky="ew", padx=4)

        # Botón toggle claro/oscuro — ícono cambia según el modo actual
        _icon = "☀️" if ctk.get_appearance_mode().lower() == "dark" else "🌙"
        self._theme_btn = ctk.CTkButton(
            top, text=_icon, command=self._toggle_theme,
            width=42, height=36,
            font=("Segoe UI", 17),
            fg_color=("gray80", "gray25"),
            hover_color=("gray70", "gray35"),
            text_color=("gray10", "gray90"),
        )
        self._theme_btn.grid(row=0, column=3, padx=(4, 12), pady=10)

        self.info_badge = ctk.CTkLabel(
            top, text="Sin archivo cargado",
            font=("Segoe UI", 12), text_color="gray",
        )
        self.info_badge.grid(row=1, column=0, columnspan=4, sticky="w", padx=14, pady=(0, 6))

    # ── Área principal: panel de columnas + tabs ──────────────────────────────

    def _build_main_area(self):
        """Área central: panel de columnas a la izquierda y tabs a la derecha."""
        main = ctk.CTkFrame(self, fg_color="transparent")
        main.grid(row=1, column=0, sticky="nsew", padx=12, pady=4)
        main.grid_columnconfigure(1, weight=1)
        main.grid_rowconfigure(0, weight=1)
        self.main_frame = main                          # referencia para toggle de paneles
        self._build_column_panel(main)
        self._build_search_files_panel(main)            # panel alternativo para tab Buscar
        self._build_right_tabs(main)

    def _build_column_panel(self, parent):
        """Panel izquierdo con checkboxes para seleccionar las columnas de salida."""
        panel = ctk.CTkFrame(parent, width=275)
        self.col_panel = panel                                          # referencia para toggle
        panel.grid(row=0, column=0, sticky="nsew", padx=(0, 6))
        panel.grid_propagate(False)
        panel.grid_columnconfigure(0, weight=1)
        panel.grid_rowconfigure(3, weight=1)

        ctk.CTkLabel(
            panel, text="Columnas de salida",
            font=("Segoe UI", 13, "bold"),
        ).grid(row=0, column=0, padx=10, pady=(10, 4), sticky="w")

        # Botones de selección rápida
        btn_row = ctk.CTkFrame(panel, fg_color="transparent")
        btn_row.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 4))
        ctk.CTkButton(
            btn_row, text="✓ Todas", width=90, height=28,
            command=self.select_all_columns,
        ).pack(side="left", padx=(0, 4))
        ctk.CTkButton(
            btn_row, text="✗ Ninguna", width=90, height=28,
            command=self.deselect_all_columns,
        ).pack(side="left")

        # Botón preset predefinido
        ctk.CTkButton(
            panel,
            text="⭐  Selección rápida",
            height=30,
            command=self.apply_preset,
            fg_color="#5a4a8a",
            hover_color="#3d3060",
        ).grid(row=2, column=0, sticky="ew", padx=8, pady=(0, 6))

        # Lista scrollable de checkboxes
        self.col_scroll = ctk.CTkScrollableFrame(panel, label_text="")
        self.col_scroll.grid(row=3, column=0, sticky="nsew", padx=6, pady=(0, 6))

        # Nota que indica por qué columna se divide la salida
        self.split_note = ctk.CTkLabel(
            panel, text="",
            font=("Segoe UI", 11), text_color="gray",
            wraplength=255, justify="left",
        )
        self.split_note.grid(row=4, column=0, sticky="w", padx=10, pady=(0, 8))

    def _build_right_tabs(self, parent):
        """Crea el tabview con las pestañas Exportar CSV, Exportar TXT, Exportar JSON, Buscar y Agregar Columna."""
        self.tabview = ctk.CTkTabview(parent)
        self.tabview.grid(row=0, column=1, sticky="nsew")

        self.tabview.add("   Exportar CSV   ")
        self.tabview.add("   Exportar TXT   ")
        self.tabview.add("   Exportar JSON   ")
        self.tabview.add("   Buscar   ")
        self.tabview.add("   Agregar Columna   ")
        # self.tabview.add("Filtros")       # TODO: descomentar para reactivar
        # self.tabview.add("Transformar")   # TODO: descomentar para reactivar

        # Hacer los botones de pestaña más grandes y visibles
        # y envolver el callback original para inyectar el toggle de panel
        try:
            _orig_cb = self.tabview._segmented_button_callback

            def _tab_cb_with_panel_toggle(value: str, _orig=_orig_cb):
                _orig(value)                  # cambia el tab normalmente
                self._on_tab_change(value)    # alterna el panel izquierdo

            self.tabview._segmented_button.configure(
                font=("Segoe UI", 13, "bold"),
                height=38,
                command=_tab_cb_with_panel_toggle,
            )
        except Exception:
            pass

        self._build_preview_tab(self.tabview.tab("   Exportar CSV   "))
        self._build_export_txt_tab(self.tabview.tab("   Exportar TXT   "))
        self._build_export_json_tab(self.tabview.tab("   Exportar JSON   "))
        self._build_search_tab(self.tabview.tab("   Buscar   "))
        self._build_add_column_tab(self.tabview.tab("   Agregar Columna   "))
        # self._build_filter_tab(self.tabview.tab("Filtros"))
        # self._build_transform_tab(self.tabview.tab("Transformar"))

    # ── Panel toggle: columnas ↔ archivos de búsqueda ────────────────────────

    # Tabs que NO usan la top bar (Abrir CSV) ni la bottom bar (PROCESAR)
    _TABS_NO_TOP    = {"Buscar"}
    _TABS_NO_BOTTOM = {"Buscar", "Exportar TXT", "Exportar JSON", "Agregar Columna"}

    def _on_tab_change(self, value: str):
        """Controla la visibilidad de paneles y barras según el tab activo:
          - Top bar (Abrir CSV): visible en todos excepto Buscar
          - Bottom bar (PROCESAR): solo visible en Exportar CSV
          - Panel izquierdo: columnas en tabs normales, lista de archivos en Buscar
        """
        tab = value.strip()

        # Panel izquierdo
        if tab == "Buscar":
            self.col_panel.grid_remove()
            self.search_files_panel.grid()
        else:
            self.search_files_panel.grid_remove()
            self.col_panel.grid()

        # Top bar
        if tab in self._TABS_NO_TOP:
            self.top_bar.grid_remove()
        else:
            self.top_bar.grid()

        # Bottom bar
        if tab in self._TABS_NO_BOTTOM:
            self.bottom_bar.grid_remove()
        else:
            self.bottom_bar.grid()

    def _build_search_files_panel(self, parent):
        """Panel izquierdo alternativo para la pestaña Buscar: lista de archivos CSV."""
        panel = ctk.CTkFrame(parent, width=275)
        panel.grid(row=0, column=0, sticky="nsew", padx=(0, 6))
        panel.grid_propagate(False)
        panel.grid_columnconfigure(0, weight=1)
        panel.grid_rowconfigure(1, weight=1)
        panel.grid_remove()                          # oculto por defecto (visible solo en Buscar)
        self.search_files_panel = panel

        # Encabezado
        ctk.CTkLabel(
            panel, text="Archivos CSV",
            font=("Segoe UI", 13, "bold"),
        ).grid(row=0, column=0, padx=10, pady=(10, 4), sticky="w")

        # Lista scrollable de archivos
        self.search_files_listbox = ctk.CTkScrollableFrame(panel, label_text="")
        self.search_files_listbox.grid(row=1, column=0, sticky="nsew", padx=6, pady=4)
        self.search_files_listbox.grid_columnconfigure(0, weight=1)

        # Botones Agregar / Limpiar
        btn_frame = ctk.CTkFrame(panel, fg_color="transparent")
        btn_frame.grid(row=2, column=0, sticky="ew", padx=6, pady=(0, 8))
        btn_frame.grid_columnconfigure(0, weight=1)
        btn_frame.grid_columnconfigure(1, weight=1)

        ctk.CTkButton(
            btn_frame, text="＋  Agregar CSV",
            command=self._add_search_files, height=32,
        ).grid(row=0, column=0, sticky="ew", padx=(0, 3))
        ctk.CTkButton(
            btn_frame, text="✕  Limpiar",
            command=self._clear_search_files, height=32,
            fg_color="#7d2d2d", hover_color="#5c1f1f",
        ).grid(row=0, column=1, sticky="ew", padx=(3, 0))

    def _add_search_files(self):
        """Abre el diálogo de selección y agrega los archivos elegidos a la lista."""
        paths = filedialog.askopenfilenames(
            title="Seleccionar archivo(s) CSV",
            filetypes=[("CSV", "*.csv"), ("Texto", "*.txt"), ("Todos", "*.*")],
        )
        changed = False
        for p in paths:
            if p not in self.search_files:
                self.search_files.append(p)
                changed = True
        if changed:
            self.search_rename_map = {}
            self._render_search_files_list()
            self._refresh_search_col_menu()
            self._check_search_preset_columns()

    def _clear_search_files(self):
        """Elimina todos los archivos de la lista de búsqueda."""
        self.search_files.clear()
        self.search_rename_map = {}
        self._render_search_files_list()
        self._refresh_search_col_menu()

    def _remove_search_file(self, path: str):
        """Elimina un archivo específico de la lista."""
        if path in self.search_files:
            self.search_files.remove(path)
            self.search_rename_map = {}
            self._render_search_files_list()
            self._refresh_search_col_menu()

    def _render_search_files_list(self):
        """Redibuja la lista de archivos CSV cargados para búsqueda."""
        for w in self.search_files_listbox.winfo_children():
            w.destroy()
        if not self.search_files:
            ctk.CTkLabel(
                self.search_files_listbox,
                text="Sin archivos. Usá «＋ Agregar CSV».",
                text_color="gray", font=("Segoe UI", 10),
                wraplength=230, justify="left",
            ).grid(row=0, column=0, pady=10, padx=4)
            return
        for i, path in enumerate(self.search_files):
            row_f = ctk.CTkFrame(self.search_files_listbox, fg_color="transparent")
            row_f.grid(row=i, column=0, sticky="ew", pady=1)
            row_f.grid_columnconfigure(0, weight=1)
            ctk.CTkLabel(
                row_f, text=Path(path).name,
                anchor="w", font=("Segoe UI", 10),
                wraplength=200,
            ).grid(row=0, column=0, sticky="ew", padx=4)
            ctk.CTkButton(
                row_f, text="✕", width=24, height=24,
                command=lambda p=path: self._remove_search_file(p),
                fg_color="transparent", hover_color="#5c1f1f",
                font=("Segoe UI", 10),
            ).grid(row=0, column=1, padx=(2, 0))

    # ── Tab: Exportar CSV ────────────────────────────────────────────────────

    def _build_preview_tab(self, parent):
        """Canvas de encabezado custom + Treeview de datos con scrollbars sincronizadas."""
        parent.grid_columnconfigure(0, weight=1)
        parent.grid_rowconfigure(1, weight=1)

        self.preview_label = ctk.CTkLabel(
            parent,
            text="Cargá un archivo CSV para ver la vista previa.",
            text_color="gray", font=("Segoe UI", 10),
        )
        self.preview_label.grid(row=0, column=0, sticky="w", padx=6, pady=(4, 2))

        # Contenedor del canvas de encabezado + Treeview + scrollbars
        tree_frame = tk.Frame(parent, bg=self._tree_colors["bg"])
        tree_frame.grid(row=1, column=0, sticky="nsew", padx=4, pady=(0, 4))
        tree_frame.grid_columnconfigure(0, weight=1)
        tree_frame.grid_rowconfigure(1, weight=1)

        # Canvas custom (fila 0): reemplaza el header nativo de ttk para permitir colores por columna
        self._header_canvas = tk.Canvas(
            tree_frame,
            height=HEADER_H,
            bg=self._tree_colors["header_bg"],
            highlightthickness=0,
        )
        self._header_canvas.grid(row=0, column=0, sticky="ew")

        # Treeview sin header nativo (show="") — fila 1
        self.tree = ttk.Treeview(tree_frame, show="", selectmode="none")
        vsb = ttk.Scrollbar(tree_frame, orient="vertical",   command=self.tree.yview)
        self._hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self._on_hsb_move)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=self._on_tree_xscroll)

        self.tree.grid(row=1, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, rowspan=2, sticky="ns")
        self._hsb.grid(row=2, column=0, sticky="ew")

        # Reajustar anchos de columna al redimensionar la ventana
        self.tree.bind("<Configure>", lambda e: self._auto_fit_columns())

    # ── Tab: Filtros ─────────────────────────────────────────────────────────

    def _build_filter_tab(self, parent):
        """Panel para agregar filtros de texto por columna y lista de filtros activos."""
        parent.grid_columnconfigure(0, weight=1)
        parent.grid_rowconfigure(2, weight=1)

        add_panel = ctk.CTkFrame(parent)
        add_panel.grid(row=0, column=0, sticky="ew", padx=6, pady=(6, 4))
        add_panel.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(add_panel, text="Columna:", width=80, anchor="e").grid(
            row=0, column=0, padx=(10, 4), pady=8
        )
        self.filter_col_var = tk.StringVar(value="(sin columnas)")
        self.filter_col_menu = ctk.CTkOptionMenu(
            add_panel, variable=self.filter_col_var, values=["(sin columnas)"]
        )
        self.filter_col_menu.grid(row=0, column=1, sticky="ew", padx=(0, 10), pady=8)

        ctk.CTkLabel(add_panel, text="Contiene:", width=80, anchor="e").grid(
            row=1, column=0, padx=(10, 4), pady=(0, 8)
        )
        self.filter_val_entry = ctk.CTkEntry(
            add_panel, placeholder_text="Valor a buscar (texto parcial)..."
        )
        self.filter_val_entry.grid(row=1, column=1, sticky="ew", padx=(0, 10), pady=(0, 8))
        self.filter_val_entry.bind("<Return>", lambda _: self.add_filter())

        ctk.CTkButton(
            add_panel, text="+ Agregar Filtro", command=self.add_filter, height=30,
        ).grid(row=2, column=0, columnspan=2, pady=(0, 10))

        ctk.CTkLabel(
            parent, text="Filtros activos:",
            font=("Segoe UI", 11, "bold"),
        ).grid(row=1, column=0, sticky="w", padx=10, pady=(4, 2))

        self.filters_scroll = ctk.CTkScrollableFrame(parent)
        self.filters_scroll.grid(row=2, column=0, sticky="nsew", padx=6, pady=(0, 6))
        self.filters_scroll.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(
            self.filters_scroll, text="Sin filtros activos", text_color="gray"
        ).grid(row=0, column=0, pady=10)

    # ── Tab: Transformar ─────────────────────────────────────────────────────

    def _build_transform_tab(self, parent):
        """Panel para normalizar columnas de fecha/hora a formato YYYY-MM-DD HH:MM:SS.mmm."""
        parent.grid_columnconfigure(0, weight=1)
        parent.grid_rowconfigure(2, weight=1)

        add_panel = ctk.CTkFrame(parent)
        add_panel.grid(row=0, column=0, sticky="ew", padx=6, pady=(6, 4))
        add_panel.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(add_panel, text="Columna:", width=100, anchor="e").grid(
            row=0, column=0, padx=(10, 4), pady=8
        )
        self.transform_col_var = tk.StringVar(value="(sin columnas)")
        self.transform_col_menu = ctk.CTkOptionMenu(
            add_panel, variable=self.transform_col_var, values=["(sin columnas)"]
        )
        self.transform_col_menu.grid(row=0, column=1, sticky="ew", padx=(0, 10), pady=8)

        ctk.CTkLabel(add_panel, text="Fecha base:", width=100, anchor="e").grid(
            row=1, column=0, padx=(10, 4), pady=(0, 8)
        )
        self.transform_date_entry = ctk.CTkEntry(
            add_panel, placeholder_text="YYYY-MM-DD", width=160
        )
        # Precarga con la fecha de hoy
        self.transform_date_entry.insert(0, _date.today().strftime("%Y-%m-%d"))
        self.transform_date_entry.grid(row=1, column=1, sticky="w", padx=(0, 10), pady=(0, 8))
        self.transform_date_entry.bind("<Return>", lambda _: self.add_date_transform())

        ctk.CTkLabel(
            add_panel,
            text="Los valores con solo hora (ej: 11:46.0) se combinarán con esta fecha.",
            font=("Segoe UI", 9), text_color="gray",
        ).grid(row=2, column=0, columnspan=2, sticky="w", padx=10, pady=(0, 8))

        ctk.CTkButton(
            add_panel, text="+ Agregar transformación",
            command=self.add_date_transform, height=30,
        ).grid(row=3, column=0, columnspan=2, pady=(0, 10))

        ctk.CTkLabel(
            parent, text="Transformaciones activas:",
            font=("Segoe UI", 11, "bold"),
        ).grid(row=1, column=0, sticky="w", padx=10, pady=(4, 2))

        self.transforms_scroll = ctk.CTkScrollableFrame(parent)
        self.transforms_scroll.grid(row=2, column=0, sticky="nsew", padx=6, pady=(0, 6))
        self.transforms_scroll.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(
            self.transforms_scroll, text="Sin transformaciones activas", text_color="gray"
        ).grid(row=0, column=0, pady=10)

    # ── Barra inferior: salida + progreso ────────────────────────────────────

    def _build_bottom_bar(self):
        """Barra inferior: carpeta de salida, delimitador, botón procesar y barra de progreso."""
        bottom = ctk.CTkFrame(self)
        bottom.grid(row=2, column=0, sticky="ew", padx=12, pady=(4, 12))
        bottom.grid_columnconfigure(1, weight=1)
        self.bottom_bar = bottom

        ctk.CTkLabel(bottom, text="Carpeta de salida:", anchor="e", width=130).grid(
            row=0, column=0, padx=(12, 4), pady=8
        )
        ctk.CTkEntry(bottom, textvariable=self.output_dir).grid(
            row=0, column=1, sticky="ew", padx=(0, 4), pady=8
        )
        ctk.CTkButton(
            bottom, text="📁", width=36, command=self.choose_output_dir
        ).grid(row=0, column=2, padx=(0, 12), pady=8)

        delim_frame = ctk.CTkFrame(bottom, fg_color="transparent")
        delim_frame.grid(row=1, column=0, columnspan=2, sticky="w", padx=12, pady=(0, 6))
        ctk.CTkLabel(delim_frame, text="Delimitador salida:").pack(side="left", padx=(0, 12))
        for label, val in [("Coma (,)", "comma"), ("Punto y coma (;)", "semicolon"), ("Tab (\\t)", "tab")]:
            ctk.CTkRadioButton(
                delim_frame, text=label, variable=self.out_delimiter, value=val
            ).pack(side="left", padx=10)

        proc_btns_frame = ctk.CTkFrame(bottom, fg_color="transparent")
        proc_btns_frame.grid(row=0, column=3, rowspan=2, padx=12, pady=8)
        self.process_btn = ctk.CTkButton(
            proc_btns_frame, text="▶  PROCESAR",
            command=self.start_processing,
            font=("Segoe UI", 13, "bold"),
            width=160, height=40,
            fg_color="#2d7d46", hover_color="#1f5c32",
        )
        self.process_btn.pack(pady=(0, 4))
        self.csv_cancel_btn = ctk.CTkButton(
            proc_btns_frame,
            text="✕  Cancelar",
            command=self._cancel_processing,
            font=("Segoe UI", 11, "bold"),
            width=160, height=30,
            fg_color="#7d2d2d", hover_color="#5c1f1f",
            state="disabled",
        )
        self.csv_cancel_btn.pack(pady=(0, 4))

        self.open_output_btn = ctk.CTkButton(
            proc_btns_frame,
            text="📂  Abrir carpeta",
            command=self._open_output_folder,
            font=("Segoe UI", 11, "bold"),
            width=160, height=30,
            fg_color="#5a3a8a", hover_color="#3d2560",
            state="disabled",
        )
        self.open_output_btn.pack()

        main_prog_frame = ctk.CTkFrame(bottom, fg_color="transparent")
        main_prog_frame.grid(row=2, column=0, columnspan=4, sticky="ew", padx=12, pady=(0, 4))
        main_prog_frame.grid_columnconfigure(0, weight=1)
        self.progress_bar = ctk.CTkProgressBar(main_prog_frame, height=20)
        self.progress_bar.grid(row=0, column=0, sticky="ew", padx=(0, 8))
        self.progress_bar.set(0)
        self.progress_pct_label = ctk.CTkLabel(
            main_prog_frame, text="0%",
            font=("Segoe UI", 16, "bold"), width=56, anchor="e",
        )
        self.progress_pct_label.grid(row=0, column=1)

        self.status_label = ctk.CTkLabel(
            bottom, text="Listo.", text_color="gray", anchor="w",
            font=("Segoe UI", 13, "bold"),
        )
        self.status_label.grid(row=3, column=0, columnspan=4, sticky="w", padx=12, pady=(0, 8))

    # ─────────────────────────────────────────────────────────────────────────
    # Carga del archivo
    # ─────────────────────────────────────────────────────────────────────────

    def load_file(self):
        """Abre el diálogo, muestra overlay y lanza la carga en hilo de fondo."""
        path = filedialog.askopenfilename(
            title="Seleccionar archivo CSV",
            filetypes=[("CSV files", "*.csv *.txt"), ("Todos los archivos", "*.*")],
        )
        if not path:
            return

        # Resetear estado derivado del archivo anterior
        self.filepath.set(path)
        self.column_rename_map = {}
        self.date_transforms   = {}
        self.df_preview        = None
        self.output_dir.set(str(Path(path).parent / "ForModelsCSV"))

        self.info_badge.configure(text="Detectando codificación y delimitador...", text_color="orange")
        self.status_label.configure(text="Cargando archivo...", text_color="gray")

        # Mostrar overlay bloqueante y procesar en segundo plano
        self._show_overlay("📂  Abriendo archivo...")
        threading.Thread(target=self._load_file_thread, args=(path,), daemon=True).start()

    def _load_file_thread(self, path: str):
        """Detecta encoding/delimitador y carga la preview en hilo de fondo."""
        try:
            encoding   = detect_encoding(path)
            delimiter  = detect_delimiter(path, encoding)
            columns    = get_columns(path, encoding, delimiter)
            df_preview = get_preview(path, encoding, delimiter, nrows=PREVIEW_ROWS)
            self.after(0, lambda: self._load_file_done(path, encoding, delimiter, columns, df_preview))
        except Exception as exc:
            self.after(0, lambda e=exc: self._load_file_error(e))

    def _load_file_done(self, path: str, encoding: str, delimiter: str, columns: list, df_preview):
        """Actualiza la UI tras una carga exitosa del archivo (se ejecuta en el hilo principal)."""
        self._hide_overlay()
        self.detected_encoding  = encoding
        self.detected_delimiter = delimiter
        self.columns            = columns
        self.df_preview         = df_preview

        delim_names  = {",": "coma", ";": "punto y coma", "\t": "tabulación"}
        delim_str    = delim_names.get(delimiter, repr(delimiter))
        file_size_mb = os.path.getsize(path) / 1_048_576

        self.info_badge.configure(
            text=(
                f"Codificación: {encoding}  |  "
                f"Delimitador: {delim_str}  |  "
                f"Columnas: {len(columns)}  |  "
                f"Tamaño: {file_size_mb:.1f} MB"
            ),
            text_color=("gray30", "gray70"),
        )

        self._refresh_column_checkboxes()
        self._refresh_preview_table()
        self._refresh_txt_col_menu()
        self._refresh_json_col_menu()
        self._update_add_col_columns()
        # self._refresh_filter_col_menu()      # TODO: descomentar al reactivar Filtros
        # self._refresh_transform_col_menu()   # TODO: descomentar al reactivar Transformar
        # self._refresh_transform_list()       # TODO: descomentar al reactivar Transformar

        first_col = columns[0] if columns else "?"
        self.split_note.configure(
            text=f"ℹ Los archivos se dividirán por los valores de la columna: «{first_col}»"
        )
        self.status_label.configure(
            text=f"Archivo cargado. Vista previa de {len(df_preview)} filas.",
            text_color="gray",
        )

    def _load_file_error(self, exc: Exception):
        """Muestra error de carga y oculta el overlay (se ejecuta en el hilo principal)."""
        self._hide_overlay()
        messagebox.showerror("Error al cargar", f"No se pudo leer el archivo:\n\n{exc}")
        self.info_badge.configure(text="Error al cargar el archivo.", text_color="red")
        self.status_label.configure(text="Error.", text_color="red")

    # ─────────────────────────────────────────────────────────────────────────
    # Overlay de procesamiento (bloquea la UI con un panel semitransparente)
    # ─────────────────────────────────────────────────────────────────────────

    def _show_overlay(self, text: str = "⏳  Procesando..."):
        """Muestra un panel semitransparente sobre la ventana; la UI de fondo sigue visible."""
        if self._overlay and self._overlay.winfo_exists():
            return  # ya hay un overlay activo

        # Obtener posición y tamaño actuales de la ventana principal
        self.update_idletasks()
        x = self.winfo_rootx()
        y = self.winfo_rooty()
        w = self.winfo_width()
        h = self.winfo_height()

        # Toplevel sin decoración de ventana, posicionado exactamente encima
        overlay = tk.Toplevel(self)
        overlay.overrideredirect(True)           # sin barra de título ni bordes
        overlay.geometry(f"{w}x{h}+{x}+{y}")
        overlay.lift()
        overlay.attributes("-topmost", True)
        overlay.attributes("-alpha", 0.52)       # 52% opaco → UI de fondo visible al 48%
        overlay.configure(bg="#0d1117")

        # Caja de texto posicionada en el tercio superior para no tapar la barra de progreso
        box = tk.Frame(overlay, bg="#1c3557", relief="flat", bd=0)
        box.place(relx=0.5, rely=0.28, anchor="center")

        tk.Label(
            box, text=text,
            font=("Segoe UI", 16, "bold"),
            bg="#1c3557", fg="white",
            padx=52, pady=24,
        ).pack()

        self._overlay     = overlay
        self._overlay_bar = None   # sin barra interna; la barra del tab queda visible detrás
        self.update_idletasks()

    def _hide_overlay(self):
        """Destruye el overlay semitransparente."""
        self._overlay_bar = None
        if self._overlay:
            try:
                self._overlay.destroy()
            except Exception:
                pass
            self._overlay = None

    # ─────────────────────────────────────────────────────────────────────────
    # Toggle de tema claro / oscuro
    # ─────────────────────────────────────────────────────────────────────────

    def _toggle_theme(self):
        """Alterna entre modo claro y oscuro, actualizando colores del Treeview y el canvas."""
        current  = ctk.get_appearance_mode().lower()
        new_mode = "Light" if current == "dark" else "Dark"
        ctk.set_appearance_mode(new_mode)

        # Actualizar ícono del botón: sol en modo claro, luna en modo oscuro
        self._theme_btn.configure(text="☀️" if new_mode == "Light" else "🌙")

        # Actualizar paleta del Treeview
        self._setup_treeview_style()

        # Actualizar fondo del frame contenedor del árbol y el canvas de encabezado
        try:
            self.tree.master.configure(bg=self._tree_colors["bg"])
            self._header_canvas.configure(bg=self._tree_colors["header_bg"])
        except Exception:
            pass

        # Refrescar la vista previa si hay datos cargados
        if self.df_preview is not None:
            self._refresh_preview_table()
        else:
            self._redraw_header_canvas()

    # ─────────────────────────────────────────────────────────────────────────
    # Checkboxes de columnas
    # ─────────────────────────────────────────────────────────────────────────

    def _refresh_column_checkboxes(self):
        """Destruye y recrea los checkboxes según las columnas del CSV cargado."""
        for w in self.col_scroll.winfo_children():
            w.destroy()
        self.column_vars.clear()

        for col in self.columns:
            var = tk.BooleanVar(value=True)
            cb = ctk.CTkCheckBox(
                self.col_scroll, text=col, variable=var,
                font=("Segoe UI", 10),
                command=self._on_column_change,
            )
            cb.pack(anchor="w", padx=6, pady=2)
            self.column_vars[col] = var

    def select_all_columns(self):
        """Marca todas las columnas y actualiza la preview."""
        for v in self.column_vars.values():
            v.set(True)
        self._on_column_change()

    def deselect_all_columns(self):
        """Desmarca todas las columnas y actualiza la preview."""
        for v in self.column_vars.values():
            v.set(False)
        self._on_column_change()

    def apply_preset(self):
        """Selecciona el conjunto preset; si alguna columna falta, abre el diálogo de mapeo."""
        if not self.columns:
            messagebox.showwarning("Sin archivo", "Cargá un archivo CSV primero.")
            return

        found   = [col for col in PRESET_COLUMNS if col in self.column_vars]
        missing = [col for col in PRESET_COLUMNS if col not in self.column_vars]

        mapping = {}
        if missing:
            dialog = ColumnMapDialog(self, missing, self.columns)
            self.wait_window(dialog)
            if dialog.cancelled:
                return
            mapping = dialog.result  # {preset_col: col_real o None}

        # Deseleccionar todo y resetear el mapa de renombrado
        for v in self.column_vars.values():
            v.set(False)
        self.column_rename_map = {}

        # Activar columnas encontradas directamente
        for col in found:
            self.column_vars[col].set(True)

        # Activar columnas mapeadas y registrar el renombrado {col_real → preset_name}
        for preset_col, real_col in mapping.items():
            if real_col and real_col in self.column_vars:
                self.column_vars[real_col].set(True)
                if real_col != preset_col:
                    self.column_rename_map[real_col] = preset_col

        self._on_column_change()
        # Refrescar menús de TXT y JSON para que usen el nuevo mapeo (ej. SN → STR_PSN)
        self._refresh_txt_col_menu()
        self._refresh_json_col_menu()

    # ─────────────────────────────────────────────────────────────────────────
    # Preview table
    # ─────────────────────────────────────────────────────────────────────────

    def _on_column_change(self):
        """Callback al marcar/desmarcar columnas: actualiza la preview en tiempo real."""
        self._refresh_preview_table()

    def _refresh_preview_table(self):
        """Actualiza el Treeview mostrando solo las columnas seleccionadas."""
        if self.df_preview is None:
            return

        # Columnas activas en el orden original del CSV
        selected = [
            col for col in self.columns
            if col in self.column_vars and self.column_vars[col].get()
        ]

        if not selected:
            self.tree.configure(columns=[])
            self.tree.delete(*self.tree.get_children())
            self.preview_label.configure(text="Sin columnas seleccionadas.", text_color="orange")
            return

        df = self.df_preview[selected]

        self.tree.configure(columns=selected)
        for col in selected:
            preset_name = self.column_rename_map.get(col)
            # Ancho mínimo basado en la longitud del texto del encabezado
            label_len = len(col) + (len(f"  →  {preset_name}") if preset_name else 0)
            min_w  = max(60, label_len * 8 + 16)
            anchor = "center" if col in CENTERED_COLUMNS or preset_name in CENTERED_COLUMNS else "w"
            self.tree.column(col, width=min_w, minwidth=min_w, stretch=True, anchor=anchor)

        # Distribuir el espacio disponible tras el render inicial
        self.tree.after(30, self._auto_fit_columns)

        # Cargar filas con colores alternados
        self.tree.delete(*self.tree.get_children())
        c = self._tree_colors
        for i, (_, row) in enumerate(df.iterrows()):
            tag = "even" if i % 2 == 0 else "odd"
            self.tree.insert("", "end", values=list(row), tags=(tag,))

        self.tree.tag_configure("even", background=c["even_row"])
        self.tree.tag_configure("odd",  background=c["odd_row"])

        self.preview_label.configure(
            text=(
                f"Vista previa: {len(df)} filas  |  "
                f"{len(selected)} columna(s) seleccionada(s) de {len(self.columns)}"
            ),
            text_color=("gray30", "gray70"),
        )
        self.tree.after(10, self._redraw_header_canvas)

    def _auto_fit_columns(self):
        """Distribuye el ancho disponible del Treeview entre las columnas proporcionalmente."""
        cols = self.tree["columns"]
        if not cols:
            return
        total = self.tree.winfo_width()
        if total <= 1:
            return

        available  = total - 18  # reservar ~18 px para la scrollbar vertical
        n          = len(cols)
        min_widths = [self.tree.column(c, "minwidth") for c in cols]
        total_min  = sum(min_widths)

        if available >= total_min:
            # Espacio suficiente: distribuir el sobrante proporcionalmente al mínimo
            extra = available - total_min
            for c, mw in zip(cols, min_widths):
                share = int(extra * (mw / total_min)) if total_min > 0 else extra // n
                self.tree.column(c, width=mw + share)
        else:
            # Menos espacio del mínimo: repartir por igual con ancho mínimo garantizado
            per_col = max(60, available // n)
            for c in cols:
                self.tree.column(c, width=per_col)

        self._redraw_header_canvas()

    # ── Scroll sync ──────────────────────────────────────────────────────────

    def _on_tree_xscroll(self, *args):
        """Sincroniza la scrollbar horizontal y redibuja el canvas al scrollear el Treeview."""
        self._hsb.set(*args)
        self._redraw_header_canvas()

    def _on_hsb_move(self, *args):
        """Mueve el Treeview y redibuja el canvas cuando el usuario mueve la scrollbar."""
        self.tree.xview(*args)
        self._redraw_header_canvas()

    # ── Canvas header ─────────────────────────────────────────────────────────

    def _redraw_header_canvas(self):
        """Dibuja el encabezado custom; columnas mapeadas usan texto bicolor (blanco + dorado)."""
        canvas = self._header_canvas
        canvas.delete("all")
        cols = self.tree["columns"]
        if not cols:
            return

        # Calcular offset horizontal por scroll
        try:
            x_frac = self.tree.xview()[0]
        except Exception:
            x_frac = 0
        total_col_w = sum(self.tree.column(c, "width") for c in cols)
        x_off = -int(x_frac * total_col_w)

        bg          = self._tree_colors["header_bg"]
        x           = x_off
        font_normal = ("Segoe UI", 9, "bold")

        for col in cols:
            cw          = self.tree.column(col, "width")
            preset_name = self.column_rename_map.get(col)

            # Fondo de la celda de encabezado
            canvas.create_rectangle(x, 0, x + cw, HEADER_H,
                                     fill=bg, outline="#1a3a6a", width=1)

            if preset_name:
                # Texto bicolor: "ORIGINAL  →  " en blanco + "PRESET" en dorado
                orig_part = f"{col}  →  "
                orig_px   = int(len(orig_part) * 7.2)   # estimación de px del texto original
                cx        = x + 8
                canvas.create_text(cx, HEADER_H // 2,
                                   text=orig_part, fill="white",
                                   anchor="w", font=font_normal)
                canvas.create_text(cx + orig_px, HEADER_H // 2,
                                   text=preset_name, fill="#FFD700",
                                   anchor="w", font=("Segoe UI", 9, "bold"))
            else:
                # Texto normal centrado
                canvas.create_text(x + cw // 2, HEADER_H // 2,
                                   text=col, fill="white",
                                   anchor="center", font=font_normal)
            x += cw

        # Actualizar scrollregion para que el canvas refleje el ancho total
        canvas_w = max(total_col_w, canvas.winfo_width())
        canvas.configure(scrollregion=(0, 0, canvas_w, HEADER_H))

    # ─────────────────────────────────────────────────────────────────────────
    # Filtros
    # ─────────────────────────────────────────────────────────────────────────

    def _refresh_filter_col_menu(self):
        """Actualiza las opciones del dropdown de columnas en el tab Filtros."""
        values = self.columns if self.columns else ["(sin columnas)"]
        self.filter_col_menu.configure(values=values)
        self.filter_col_var.set(values[0])

    def add_filter(self):
        """Valida y agrega el filtro ingresado al diccionario de filtros activos."""
        col = self.filter_col_var.get()
        val = self.filter_val_entry.get().strip()

        if not col or col == "(sin columnas)":
            messagebox.showwarning("Filtro", "Seleccioná una columna.")
            return
        if not val:
            messagebox.showwarning("Filtro", "Ingresá un valor de búsqueda.")
            return

        self.filters[col] = val
        self.filter_val_entry.delete(0, "end")
        self._refresh_filter_list()

    def remove_filter(self, col: str):
        """Elimina el filtro asociado a la columna indicada."""
        self.filters.pop(col, None)
        self._refresh_filter_list()

    def clear_all_filters(self):
        """Elimina todos los filtros activos."""
        self.filters.clear()
        self._refresh_filter_list()

    def _refresh_filter_list(self):
        """Redibuja la lista de filtros activos en el scroll frame."""
        for w in self.filters_scroll.winfo_children():
            w.destroy()

        if not self.filters:
            ctk.CTkLabel(
                self.filters_scroll, text="Sin filtros activos", text_color="gray"
            ).grid(row=0, column=0, pady=10)
            return

        for i, (col, val) in enumerate(self.filters.items()):
            row_f = ctk.CTkFrame(self.filters_scroll)
            row_f.grid(row=i, column=0, sticky="ew", pady=2, padx=4)
            row_f.grid_columnconfigure(0, weight=1)

            ctk.CTkLabel(
                row_f,
                text=f"  {col}  →  contiene: \"{val}\"",
                anchor="w", font=("Segoe UI", 10),
            ).grid(row=0, column=0, sticky="ew", padx=6, pady=6)

            ctk.CTkButton(
                row_f, text="✕", width=30, height=26,
                fg_color="transparent", hover_color="#cc3333",
                command=lambda c=col: self.remove_filter(c),
            ).grid(row=0, column=1, padx=6)

        # Botón "Limpiar todos" si hay más de un filtro
        if len(self.filters) > 1:
            ctk.CTkButton(
                self.filters_scroll,
                text="Limpiar todos", height=28,
                fg_color="transparent", border_width=1,
                command=self.clear_all_filters,
            ).grid(row=len(self.filters), column=0, pady=(6, 2))

    # ─────────────────────────────────────────────────────────────────────────
    # Transformaciones de fecha
    # ─────────────────────────────────────────────────────────────────────────

    def _refresh_transform_col_menu(self):
        """Actualiza las opciones del dropdown de columnas en el tab Transformar."""
        values = self.columns if self.columns else ["(sin columnas)"]
        self.transform_col_menu.configure(values=values)
        self.transform_col_var.set(values[0])

    def add_date_transform(self):
        """Valida el formato de fecha y agrega la transformación para la columna seleccionada."""
        col       = self.transform_col_var.get()
        base_date = self.transform_date_entry.get().strip()

        if not col or col == "(sin columnas)":
            messagebox.showwarning("Transformación", "Seleccioná una columna.")
            return
        if not base_date:
            messagebox.showwarning("Transformación", "Ingresá una fecha base (YYYY-MM-DD).")
            return

        # Validar el formato antes de guardar
        try:
            _datetime.strptime(base_date, "%Y-%m-%d")
        except ValueError:
            messagebox.showwarning("Transformación", "Formato de fecha inválido. Usá YYYY-MM-DD.")
            return

        self.date_transforms[col] = base_date
        self._refresh_transform_list()

    def remove_date_transform(self, col: str):
        """Elimina la transformación de fecha asociada a la columna indicada."""
        self.date_transforms.pop(col, None)
        self._refresh_transform_list()

    def _refresh_transform_list(self):
        """Redibuja la lista de transformaciones activas en el scroll frame."""
        for w in self.transforms_scroll.winfo_children():
            w.destroy()

        if not self.date_transforms:
            ctk.CTkLabel(
                self.transforms_scroll, text="Sin transformaciones activas", text_color="gray"
            ).grid(row=0, column=0, pady=10)
            return

        for i, (col, base_date) in enumerate(self.date_transforms.items()):
            row_f = ctk.CTkFrame(self.transforms_scroll)
            row_f.grid(row=i, column=0, sticky="ew", pady=2, padx=4)
            row_f.grid_columnconfigure(0, weight=1)

            ctk.CTkLabel(
                row_f,
                text=f"  {col}  →  fecha base: \"{base_date}\"",
                anchor="w", font=("Segoe UI", 10),
            ).grid(row=0, column=0, sticky="ew", padx=6, pady=6)

            ctk.CTkButton(
                row_f, text="✕", width=30, height=26,
                fg_color="transparent", hover_color="#cc3333",
                command=lambda c=col: self.remove_date_transform(c),
            ).grid(row=0, column=1, padx=6)

        # Botón "Limpiar todas" si hay más de una transformación
        if len(self.date_transforms) > 1:
            ctk.CTkButton(
                self.transforms_scroll,
                text="Limpiar todas", height=28,
                fg_color="transparent", border_width=1,
                command=lambda: [self.date_transforms.clear(), self._refresh_transform_list()],
            ).grid(row=len(self.date_transforms), column=0, pady=(6, 2))

    # ─────────────────────────────────────────────────────────────────────────
    # Exportar TXT — tab y lógica
    # ─────────────────────────────────────────────────────────────────────────

    def _build_export_txt_tab(self, parent):
        """Tab para exportar valores únicos de una columna a archivos TXT."""
        parent.grid_columnconfigure(0, weight=1)
        parent.grid_rowconfigure(1, weight=1)   # fila espaciadora → empuja controles al fondo

        # ── Panel de configuración ────────────────────────────────────────────
        cfg = ctk.CTkFrame(parent)
        cfg.grid(row=0, column=0, sticky="ew", padx=6, pady=(6, 4))
        cfg.grid_columnconfigure(1, weight=1)

        # Selector de columna de valores
        ctk.CTkLabel(cfg, text="Columna:", width=120, anchor="e").grid(
            row=0, column=0, padx=(10, 4), pady=8
        )
        self.txt_col_var  = tk.StringVar(value="(sin columnas)")
        self.txt_col_menu = ctk.CTkOptionMenu(
            cfg, variable=self.txt_col_var, values=["(sin columnas)"]
        )
        self.txt_col_menu.grid(row=0, column=1, sticky="ew", padx=(0, 10), pady=8)

        # Selector de formato
        ctk.CTkLabel(cfg, text="Formato:", width=120, anchor="e").grid(
            row=1, column=0, padx=(10, 4), pady=(0, 4)
        )
        self.txt_format_var = tk.StringVar(value="plain")
        fmt_frame = ctk.CTkFrame(cfg, fg_color="transparent")
        fmt_frame.grid(row=1, column=1, sticky="w", padx=(0, 10), pady=(0, 4))
        ctk.CTkRadioButton(
            fmt_frame, text="Valor solo",
            variable=self.txt_format_var, value="plain",
            command=self._update_txt_preview,
        ).pack(side="left", padx=(0, 20))
        ctk.CTkRadioButton(
            fmt_frame, text="'valor',  (SQL / Python)",
            variable=self.txt_format_var, value="quoted",
            command=self._update_txt_preview,
        ).pack(side="left")

        # Vista previa del formato — fuente grande y monoespaciada
        self.txt_preview_label = ctk.CTkLabel(
            cfg, text="",
            font=("Consolas", 13, "bold"),
            justify="left", anchor="w",
        )
        self.txt_preview_label.grid(
            row=2, column=0, columnspan=2, sticky="w", padx=22, pady=(6, 12)
        )
        self._update_txt_preview()

        # Separador
        ctk.CTkFrame(cfg, height=2, fg_color="gray40").grid(
            row=3, column=0, columnspan=2, sticky="ew", padx=10, pady=(0, 8)
        )

        # Agrupación
        ctk.CTkLabel(cfg, text="Agrupación:", width=120, anchor="e").grid(
            row=4, column=0, padx=(10, 4), pady=(0, 4)
        )
        self.txt_group_var = tk.StringVar(value="none")
        grp_frame = ctk.CTkFrame(cfg, fg_color="transparent")
        grp_frame.grid(row=4, column=1, sticky="w", padx=(0, 10), pady=(0, 4))
        ctk.CTkRadioButton(
            grp_frame, text="Sin agrupar (un solo archivo)",
            variable=self.txt_group_var, value="none",
            command=self._on_grouping_change,
        ).pack(anchor="w", pady=(0, 4))

        grp_col_frame = ctk.CTkFrame(grp_frame, fg_color="transparent")
        grp_col_frame.pack(anchor="w")
        ctk.CTkRadioButton(
            grp_col_frame, text="Agrupar por:",
            variable=self.txt_group_var, value="column",
            command=self._on_grouping_change,
        ).pack(side="left", padx=(0, 8))
        self.txt_grp_col_var  = tk.StringVar(value="(sin columnas)")
        self.txt_grp_col_menu = ctk.CTkOptionMenu(
            grp_col_frame,
            variable=self.txt_grp_col_var,
            values=["(sin columnas)"],
            width=200,
            state="disabled",
            command=lambda _: self._update_txt_dest_label(),
        )
        self.txt_grp_col_menu.pack(side="left")

        # ── Ruta de destino — recuadro prominente ─────────────────────────────
        dest_frame = ctk.CTkFrame(parent, fg_color=("#dbe8f5", "#1a3a5c"), corner_radius=8)
        dest_frame.grid(row=2, column=0, sticky="ew", padx=8, pady=(6, 4))
        dest_frame.grid_columnconfigure(0, weight=1)
        dest_frame.grid_columnconfigure(1, weight=0)

        ctk.CTkLabel(
            dest_frame, text="📁  Destino:",
            font=("Segoe UI", 13, "bold"),
            anchor="w",
        ).grid(row=0, column=0, sticky="w", padx=12, pady=(8, 0))

        self.txt_dest_label = ctk.CTkLabel(
            dest_frame,
            text="<carpeta del CSV> / TXT / <nombre_archivo>.txt",
            font=("Segoe UI", 13),
            anchor="w",
            wraplength=520,
            justify="left",
        )
        self.txt_dest_label.grid(row=1, column=0, sticky="w", padx=12, pady=(2, 10))

        # Botón "Abrir carpeta" con colores explícitos para ser visible en ambos modos
        ctk.CTkButton(
            dest_frame,
            text="📂  Abrir carpeta",
            command=self._open_txt_dest_folder,
            width=150, height=32,
            font=("Segoe UI", 11, "bold"),
            fg_color=("gray75", "gray30"),
            hover_color=("gray65", "gray40"),
            text_color=("gray10", "gray90"),
        ).grid(row=1, column=1, padx=(4, 12), pady=(2, 10), sticky="e")

        # ── Botones exportar/cancelar + barra de progreso + estado ─────────────
        txt_btns_frame = ctk.CTkFrame(parent, fg_color="transparent")
        txt_btns_frame.grid(row=3, column=0, padx=12, pady=(6, 4), sticky="w")
        ctk.CTkButton(
            txt_btns_frame,
            text="📄  Exportar TXT",
            command=self.export_txt_start,
            height=38,
            font=("Segoe UI", 12, "bold"),
            fg_color="#2d7d46", hover_color="#1f5c32",
        ).pack(side="left", padx=(0, 8))
        self.txt_cancel_btn = ctk.CTkButton(
            txt_btns_frame,
            text="✕  Cancelar",
            command=self._cancel_txt,
            height=38,
            font=("Segoe UI", 12, "bold"),
            fg_color="#7d2d2d", hover_color="#5c1f1f",
            state="disabled",
        )
        self.txt_cancel_btn.pack(side="left")

        # Barra de progreso para la exportación TXT
        txt_prog_frame = ctk.CTkFrame(parent, fg_color="transparent")
        txt_prog_frame.grid(row=4, column=0, sticky="ew", padx=12, pady=(0, 2))
        txt_prog_frame.grid_columnconfigure(0, weight=1)
        self.txt_progress_bar = ctk.CTkProgressBar(txt_prog_frame, height=20)
        self.txt_progress_bar.grid(row=0, column=0, sticky="ew", padx=(0, 8))
        self.txt_progress_bar.set(0)
        self.txt_pct_label = ctk.CTkLabel(
            txt_prog_frame, text="0%",
            font=("Segoe UI", 16, "bold"), width=56, anchor="e",
        )
        self.txt_pct_label.grid(row=0, column=1)

        self.txt_status_label = ctk.CTkLabel(
            parent, text="", text_color="gray",
            font=("Segoe UI", 13, "bold"), anchor="w", wraplength=580,
        )
        self.txt_status_label.grid(row=5, column=0, sticky="w", padx=14, pady=(4, 8))

    def _update_txt_preview(self):
        """Actualiza la vista previa del formato TXT al cambiar la opción de formato."""
        if self.txt_format_var.get() == "plain":
            preview = "ZY32MJ8RT1\nZY32MJ8RT3\nZY32MJ8RT4\nZY32MJ8RT5\n..."
        else:
            preview = "'ZY32MJ8RT1',\n'ZY32MJ8RT3',\n'ZY32MJ8RT4',\n'ZY32MJ8RT5',\n..."
        self.txt_preview_label.configure(text=preview)

    def _on_grouping_change(self):
        """Habilita o deshabilita el dropdown de columna de agrupación según la opción elegida."""
        is_grouped = self.txt_group_var.get() == "column"
        self.txt_grp_col_menu.configure(state="normal" if is_grouped else "disabled")
        self._update_txt_dest_label()

    def _open_txt_dest_folder(self):
        """Abre en el Explorador de Windows la carpeta TXT de destino (la crea si no existe)."""
        if not self.filepath.get():
            messagebox.showwarning("Sin archivo", "Cargá un archivo CSV primero.")
            return
        folder = Path(self.filepath.get()).parent / "Export_TXT"
        folder.mkdir(parents=True, exist_ok=True)
        os.startfile(str(folder))

    def _update_txt_dest_label(self):
        """Actualiza la etiqueta de ruta de destino según el modo de agrupación activo."""
        if not self.filepath.get():
            self.txt_dest_label.configure(
                text="<carpeta del CSV> / TXT / <nombre_archivo>.txt"
            )
            return
        p          = Path(self.filepath.get())
        txt_folder = p.parent / "Export_TXT"
        if self.txt_group_var.get() == "column":
            grp_col = self.txt_grp_col_var.get()
            self.txt_dest_label.configure(
                text=f"{txt_folder}\\<{grp_col}>.txt   (un archivo por valor)"
            )
        else:
            self.txt_dest_label.configure(
                text=str(txt_folder / (p.stem + ".txt"))
            )

    def _refresh_txt_col_menu(self):
        """Actualiza los dropdowns de columnas; auto-selecciona SN y PHONEMODEL_NAME por defecto."""
        values = self.columns if self.columns else ["(sin columnas)"]

        # ── Columna de valores (SN por defecto) ──
        self.txt_col_menu.configure(values=values)
        default_val = values[0]
        for real, preset in self.column_rename_map.items():
            if preset == "SN" and real in values:
                default_val = real
                break
        else:
            if "SN" in values:
                default_val = "SN"
        self.txt_col_var.set(default_val)

        # ── Columna de agrupación (PHONEMODEL_NAME por defecto) ──
        self.txt_grp_col_menu.configure(values=values)
        default_grp = values[0]
        for real, preset in self.column_rename_map.items():
            if preset == "PHONEMODEL_NAME" and real in values:
                default_grp = real
                break
        else:
            if "PHONEMODEL_NAME" in values:
                default_grp = "PHONEMODEL_NAME"
        self.txt_grp_col_var.set(default_grp)

        self._update_txt_dest_label()

    def export_txt_start(self):
        """Valida la configuración e inicia la exportación TXT en un hilo de fondo."""
        if not self.filepath.get():
            messagebox.showwarning("Sin archivo", "Cargá un archivo CSV primero.")
            return
        col = self.txt_col_var.get()
        if not col or col == "(sin columnas)":
            messagebox.showwarning("Exportar TXT", "Seleccioná una columna.")
            return
        # Si está agrupado, verificar que la columna de agrupación sea válida
        group_col = None
        if self.txt_group_var.get() == "column":
            group_col = self.txt_grp_col_var.get()
            if not group_col or group_col == "(sin columnas)":
                messagebox.showwarning("Exportar TXT", "Seleccioná una columna de agrupación.")
                return
            if group_col == col:
                messagebox.showwarning(
                    "Exportar TXT",
                    "La columna de valores y la de agrupación no pueden ser la misma.",
                )
                return

        self.txt_status_label.configure(text="⏳ Leyendo archivo...", text_color="orange")
        self.txt_progress_bar.set(0)
        self.txt_pct_label.configure(text="0%")
        self._txt_cancel = False
        self.txt_cancel_btn.configure(state="normal")

        threading.Thread(
            target=self._export_txt_thread,
            args=(col, self.txt_format_var.get(), group_col),
            daemon=True,
        ).start()

    def _export_txt_thread(self, col: str, fmt: str, group_col: str | None):
        """Lee el CSV en segundo plano, extrae valores únicos y escribe los TXT con progreso."""

        def write_values(filepath, values):
            """Escribe la lista de valores en el formato elegido."""
            with open(filepath, "w", encoding="utf-8") as f:
                for v in values:
                    f.write(f"'{v}',\n" if fmt == "quoted" else f"{v}\n")

        try:
            input_path = Path(self.filepath.get())
            output_dir = input_path.parent / "Export_TXT"
            output_dir.mkdir(parents=True, exist_ok=True)

            if group_col:
                # ── Modo agrupado: un archivo por valor de group_col ──────────
                groups = get_unique_values_by_group(
                    filepath     = str(input_path),
                    encoding     = self.detected_encoding,
                    delimiter    = self.detected_delimiter,
                    value_column = col,
                    group_column = group_col,
                )
                total   = max(len(groups), 1)
                n_total = 0
                n_files = 0
                for idx, (group_val, values) in enumerate(groups.items()):
                    if self._txt_cancel:
                        break
                    out_file = output_dir / (sanitize_filename(group_val) + ".txt")
                    write_values(out_file, values)
                    n_total += len(values)
                    n_files += 1
                    pct     = (idx + 1) / total
                    pct_int = int(pct * 100)
                    self.after(0, lambda p=pct: self.txt_progress_bar.set(p))
                    self.after(0, lambda t=pct_int: self.txt_pct_label.configure(text=f"{t}%"))
                if self._txt_cancel:
                    msg   = f"Cancelado  |  {n_files} archivo(s) escrito(s)  |  {n_total:,} valores"
                    color = "orange"
                else:
                    msg   = (
                        f"✓  {len(groups)} archivo(s) creado(s)  |  "
                        f"{n_total:,} valores en total  →  {output_dir}"
                    )
                    color = ("green", "#4ec94e")
            else:
                # ── Modo sin agrupar: un solo archivo ─────────────────────────
                values = get_unique_column_values(
                    filepath  = str(input_path),
                    encoding  = self.detected_encoding,
                    delimiter = self.detected_delimiter,
                    column    = col,
                )
                out_file = output_dir / (input_path.stem + ".txt")
                write_values(out_file, values)
                msg   = f"✓  {len(values):,} valores únicos exportados  →  {out_file}"
                color = ("green", "#4ec94e")

        except Exception as exc:
            msg   = f"Error: {exc}"
            color = "red"

        self.after(0, lambda: self.txt_cancel_btn.configure(state="disabled"))
        self.after(0, lambda: self.txt_progress_bar.set(1.0))
        self.after(0, lambda: self.txt_pct_label.configure(text="100%"))
        self.after(0, lambda m=msg, c=color: self.txt_status_label.configure(text=m, text_color=c))

    # ─────────────────────────────────────────────────────────────────────────
    # Exportar JSON — tab y lógica
    # ─────────────────────────────────────────────────────────────────────────

    def _build_export_json_tab(self, parent):
        """Tab para exportar el CSV segmentado en carpetas/archivos JSON (dos niveles)."""
        parent.grid_columnconfigure(0, weight=1)
        parent.grid_rowconfigure(1, weight=1)   # fila espaciadora → empuja controles al fondo

        # ── Panel de configuración ────────────────────────────────────────────
        cfg = ctk.CTkFrame(parent)
        cfg.grid(row=0, column=0, sticky="ew", padx=6, pady=(6, 4))
        cfg.grid_columnconfigure(1, weight=1)

        # Nivel 1: columna que define la CARPETA
        ctk.CTkLabel(cfg, text="Carpetas por:", width=140, anchor="e").grid(
            row=0, column=0, padx=(10, 4), pady=8
        )
        self.json_folder_col_var  = tk.StringVar(value="(sin columnas)")
        self.json_folder_col_menu = ctk.CTkOptionMenu(
            cfg,
            variable=self.json_folder_col_var,
            values=["(sin columnas)"],
            command=lambda _: self._update_json_dest_label(),
        )
        self.json_folder_col_menu.grid(row=0, column=1, sticky="ew", padx=(0, 10), pady=8)

        # Nivel 2: columna que define el NOMBRE DEL ARCHIVO
        ctk.CTkLabel(cfg, text="Archivos (JSON) por:", width=140, anchor="e").grid(
            row=1, column=0, padx=(10, 4), pady=(0, 8)
        )
        self.json_file_col_var  = tk.StringVar(value="(sin columnas)")
        self.json_file_col_menu = ctk.CTkOptionMenu(
            cfg,
            variable=self.json_file_col_var,
            values=["(sin columnas)"],
            command=lambda _: self._update_json_dest_label(),
        )
        self.json_file_col_menu.grid(row=1, column=1, sticky="ew", padx=(0, 10), pady=(0, 8))

        # Separador
        ctk.CTkFrame(cfg, height=2, fg_color="gray40").grid(
            row=2, column=0, columnspan=2, sticky="ew", padx=10, pady=(0, 8)
        )

        # Qué columnas incluir en el JSON
        ctk.CTkLabel(cfg, text="Columnas JSON:", width=140, anchor="e").grid(
            row=3, column=0, padx=(10, 4), pady=(0, 8)
        )
        self.json_cols_var = tk.StringVar(value="selected")
        cols_frame = ctk.CTkFrame(cfg, fg_color="transparent")
        cols_frame.grid(row=3, column=1, sticky="w", padx=(0, 10), pady=(0, 8))
        ctk.CTkRadioButton(
            cols_frame,
            text="Columnas seleccionadas del panel izquierdo",
            variable=self.json_cols_var, value="selected",
        ).pack(anchor="w", pady=(0, 4))
        ctk.CTkRadioButton(
            cols_frame,
            text="Todas las columnas del CSV",
            variable=self.json_cols_var, value="all",
            command=self.select_all_columns,
        ).pack(anchor="w")

        # Nota de formato
        ctk.CTkLabel(
            cfg,
            text='Estructura: JSON / <carpeta> / <archivo>.json  →  [ {…}, … ]',
            font=("Consolas", 11),
            text_color="gray",
            anchor="w",
        ).grid(row=4, column=0, columnspan=2, sticky="w", padx=14, pady=(0, 10))

        # ── Ruta de destino — recuadro prominente ─────────────────────────────
        dest_frame = ctk.CTkFrame(parent, fg_color=("#dbe8f5", "#1a3a5c"), corner_radius=8)
        dest_frame.grid(row=2, column=0, sticky="ew", padx=8, pady=(6, 4))
        dest_frame.grid_columnconfigure(0, weight=1)
        dest_frame.grid_columnconfigure(1, weight=0)

        ctk.CTkLabel(
            dest_frame, text="📁  Destino:",
            font=("Segoe UI", 13, "bold"), anchor="w",
        ).grid(row=0, column=0, sticky="w", padx=12, pady=(8, 0))

        self.json_dest_label = ctk.CTkLabel(
            dest_frame,
            text="<carpeta del CSV> / JSON / <valor_segmento>.json",
            font=("Segoe UI", 13), anchor="w",
            wraplength=520, justify="left",
        )
        self.json_dest_label.grid(row=1, column=0, sticky="w", padx=12, pady=(2, 10))

        ctk.CTkButton(
            dest_frame,
            text="📂  Abrir carpeta",
            command=self._open_json_dest_folder,
            width=150, height=32,
            font=("Segoe UI", 11, "bold"),
            fg_color=("gray75", "gray30"),
            hover_color=("gray65", "gray40"),
            text_color=("gray10", "gray90"),
        ).grid(row=1, column=1, padx=(4, 12), pady=(2, 10), sticky="e")

        # ── Botones exportar/cancelar + barra de progreso + estado ─────────────
        json_btns_frame = ctk.CTkFrame(parent, fg_color="transparent")
        json_btns_frame.grid(row=3, column=0, padx=12, pady=(6, 4), sticky="w")
        ctk.CTkButton(
            json_btns_frame,
            text="🗂  Exportar JSON",
            command=self.export_json_start,
            height=38,
            font=("Segoe UI", 12, "bold"),
            fg_color="#2d7d46", hover_color="#1f5c32",
        ).pack(side="left", padx=(0, 8))
        self.json_cancel_btn = ctk.CTkButton(
            json_btns_frame,
            text="✕  Cancelar",
            command=self._cancel_json,
            height=38,
            font=("Segoe UI", 12, "bold"),
            fg_color="#7d2d2d", hover_color="#5c1f1f",
            state="disabled",
        )
        self.json_cancel_btn.pack(side="left")

        json_prog_frame = ctk.CTkFrame(parent, fg_color="transparent")
        json_prog_frame.grid(row=4, column=0, sticky="ew", padx=12, pady=(0, 2))
        json_prog_frame.grid_columnconfigure(0, weight=1)
        self.json_progress_bar = ctk.CTkProgressBar(json_prog_frame, height=20)
        self.json_progress_bar.grid(row=0, column=0, sticky="ew", padx=(0, 8))
        self.json_progress_bar.set(0)
        self.json_pct_label = ctk.CTkLabel(
            json_prog_frame, text="0%",
            font=("Segoe UI", 16, "bold"), width=56, anchor="e",
        )
        self.json_pct_label.grid(row=0, column=1)

        self.json_status_label = ctk.CTkLabel(
            parent, text="", text_color="gray",
            font=("Segoe UI", 13, "bold"), anchor="w", wraplength=580,
        )
        self.json_status_label.grid(row=5, column=0, sticky="w", padx=14, pady=(4, 8))

    # ── Tab: Buscar ──────────────────────────────────────────────────────────

    def _build_search_tab(self, parent):
        """Tab para buscar un valor exacto en múltiples archivos CSV y exportar resultados."""
        parent.grid_columnconfigure(0, weight=1)
        parent.grid_rowconfigure(5, weight=1)   # treeview de resultados ocupa el espacio sobrante

        # ── Título ────────────────────────────────────────────────────────────
        ctk.CTkLabel(
            parent, text="Buscar en archivos CSV",
            font=("Segoe UI", 14, "bold"), anchor="w",
        ).grid(row=0, column=0, sticky="w", padx=12, pady=(8, 4))

        # ── Fila 1: campo de búsqueda ─────────────────────────────────────────
        input_row = ctk.CTkFrame(parent, fg_color="transparent")
        input_row.grid(row=1, column=0, sticky="ew", padx=12, pady=(0, 4))
        input_row.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(
            input_row, text="Código(s) a buscar:",
            font=("Segoe UI", 12, "bold"), anchor="ne", width=130,
        ).grid(row=0, column=0, padx=(0, 8), pady=(4, 0), sticky="ne")

        self.search_input = ctk.CTkTextbox(
            input_row,
            height=72, font=("Segoe UI", 13),
            wrap="none",
        )
        self.search_input.grid(row=0, column=1, sticky="ew")
        # Hint de placeholder manual (se borra al escribir)
        self.search_input.insert("1.0", "ej: ZY32MJ3LZH\nO varios separados por coma o Enter")
        self.search_input.configure(text_color="gray60")
        def _on_focus_in(e):
            content = self.search_input.get("1.0", "end-1c")
            if content.startswith("ej:"):
                self.search_input.delete("1.0", "end")
                self.search_input.configure(text_color=("gray10", "white"))
        def _on_focus_out(e):
            content = self.search_input.get("1.0", "end-1c").strip()
            if not content:
                self.search_input.insert("1.0", "ej: ZY32MJ3LZH\nO varios separados por coma o Enter")
                self.search_input.configure(text_color="gray60")
        self.search_input.bind("<FocusIn>",  _on_focus_in)
        self.search_input.bind("<FocusOut>", _on_focus_out)
        # Ctrl+Enter lanza la búsqueda
        self.search_input.bind("<Control-Return>", lambda _: self.search_start())

        # ── Fila 2: columna + botones ─────────────────────────────────────────
        ctrl_row = ctk.CTkFrame(parent, fg_color="transparent")
        ctrl_row.grid(row=2, column=0, sticky="ew", padx=12, pady=(0, 4))

        ctk.CTkLabel(ctrl_row, text="Buscar en columna:", anchor="e", width=130).grid(
            row=0, column=0, padx=(0, 8),
        )
        self.search_col_var  = tk.StringVar(value="(sin columnas)")
        self.search_col_menu = ctk.CTkOptionMenu(
            ctrl_row, variable=self.search_col_var,
            values=["(sin columnas)"], width=200,
        )
        self.search_col_menu.grid(row=0, column=1, padx=(0, 16))

        self.search_btn = ctk.CTkButton(
            ctrl_row, text="🔍  Buscar",
            command=self.search_start,
            height=36, width=130,
            font=("Segoe UI", 12, "bold"),
            fg_color="#2d7d46", hover_color="#1f5c32",
        )
        self.search_btn.grid(row=0, column=2, padx=(0, 8))

        self.search_cancel_btn = ctk.CTkButton(
            ctrl_row, text="✕  Cancelar",
            command=self._cancel_search,
            height=36, width=130,
            font=("Segoe UI", 12, "bold"),
            fg_color="#7d2d2d", hover_color="#5c1f1f",
            state="disabled",
        )
        self.search_cancel_btn.grid(row=0, column=3, padx=(0, 8))

        ctk.CTkButton(
            ctrl_row, text="🗑  Limpiar",
            command=self._clear_search_results,
            height=36, width=110,
            font=("Segoe UI", 12, "bold"),
            fg_color="#4a4a4a", hover_color="#2e2e2e",
        ).grid(row=0, column=4)

        # ── Barra de progreso ─────────────────────────────────────────────────
        search_prog_frame = ctk.CTkFrame(parent, fg_color="transparent")
        search_prog_frame.grid(row=3, column=0, sticky="ew", padx=12, pady=(0, 2))
        search_prog_frame.grid_columnconfigure(0, weight=1)

        self.search_progress_bar = ctk.CTkProgressBar(search_prog_frame, height=20)
        self.search_progress_bar.grid(row=0, column=0, sticky="ew", padx=(0, 8))
        self.search_progress_bar.set(0)

        self.search_pct_label = ctk.CTkLabel(
            search_prog_frame, text="0%",
            font=("Segoe UI", 16, "bold"), width=56, anchor="e",
        )
        self.search_pct_label.grid(row=0, column=1)

        # ── Etiqueta de estado ────────────────────────────────────────────────
        self.search_status_label = ctk.CTkLabel(
            parent,
            text="Agregá archivos CSV en el panel izquierdo e ingresá un valor a buscar.",
            text_color="gray", font=("Segoe UI", 11),
            anchor="w", wraplength=700,
        )
        self.search_status_label.grid(row=4, column=0, sticky="w", padx=14, pady=(2, 4))

        # ── Treeview de resultados ─────────────────────────────────────────────
        results_frame = tk.Frame(parent, bg=self._tree_colors["bg"])
        results_frame.grid(row=5, column=0, sticky="nsew", padx=8, pady=(0, 4))
        results_frame.grid_columnconfigure(0, weight=1)
        results_frame.grid_rowconfigure(0, weight=1)

        self.search_tree = ttk.Treeview(
            results_frame, show="headings", selectmode="browse",
        )
        vsb_s = ttk.Scrollbar(results_frame, orient="vertical",   command=self.search_tree.yview)
        hsb_s = ttk.Scrollbar(results_frame, orient="horizontal", command=self.search_tree.xview)
        self.search_tree.configure(yscrollcommand=vsb_s.set, xscrollcommand=hsb_s.set)
        self.search_tree.grid(row=0, column=0, sticky="nsew")
        vsb_s.grid(row=0, column=1, sticky="ns")
        hsb_s.grid(row=1, column=0, sticky="ew")

        # ── Fila de exportación JSON ───────────────────────────────────────────
        export_row = ctk.CTkFrame(parent, fg_color=("#dbe8f5", "#1a3a5c"), corner_radius=8)
        export_row.grid(row=6, column=0, sticky="ew", padx=8, pady=(4, 8))

        ctk.CTkLabel(
            export_row,
            text="📁  Exportar resultados como JSON:",
            font=("Segoe UI", 12, "bold"), anchor="w",
        ).grid(row=0, column=0, columnspan=5, sticky="w", padx=12, pady=(8, 4))

        ctk.CTkLabel(export_row, text="Carpetas por:", anchor="e", width=100).grid(
            row=1, column=0, padx=(12, 4), pady=(0, 10),
        )
        self.search_folder_col_var  = tk.StringVar(value="(sin columnas)")
        self.search_folder_col_menu = ctk.CTkOptionMenu(
            export_row, variable=self.search_folder_col_var,
            values=["(sin columnas)"], width=180,
        )
        self.search_folder_col_menu.grid(row=1, column=1, padx=(0, 12), pady=(0, 10))

        ctk.CTkLabel(export_row, text="Archivos por:", anchor="e", width=100).grid(
            row=1, column=2, padx=(0, 4), pady=(0, 10),
        )
        self.search_file_col_var  = tk.StringVar(value="(sin columnas)")
        self.search_file_col_menu = ctk.CTkOptionMenu(
            export_row, variable=self.search_file_col_var,
            values=["(sin columnas)"], width=180,
        )
        self.search_file_col_menu.grid(row=1, column=3, padx=(0, 12), pady=(0, 10))

        ctk.CTkButton(
            export_row,
            text="💾  Exportar JSON",
            command=self.export_search_json_start,
            height=36,
            font=("Segoe UI", 12, "bold"),
            fg_color="#2d7d46", hover_color="#1f5c32",
        ).grid(row=1, column=4, padx=(0, 8), pady=(0, 6))

        ctk.CTkButton(
            export_row,
            text="📄  Exportar CSV",
            command=self.export_search_csv_start,
            height=36,
            font=("Segoe UI", 12, "bold"),
            fg_color="#1a5c8a", hover_color="#0f3d5c",
        ).grid(row=1, column=5, padx=(0, 8), pady=(0, 6))

        self.search_json_open_btn = ctk.CTkButton(
            export_row,
            text="📂  Abrir carpeta",
            command=self._open_search_json_folder,
            height=36,
            font=("Segoe UI", 12, "bold"),
            fg_color="#5a3a8a", hover_color="#3d2560",
            state="disabled",
        )
        self.search_json_open_btn.grid(row=1, column=6, padx=(0, 12), pady=(0, 6))

        # ── Barra de progreso de exportación (row 2) ──────────────────────────
        export_row.grid_columnconfigure(0, weight=1)
        exp_prog_frame = ctk.CTkFrame(export_row, fg_color="transparent")
        exp_prog_frame.grid(row=2, column=0, columnspan=7, sticky="ew", padx=12, pady=(0, 8))
        exp_prog_frame.grid_columnconfigure(0, weight=1)

        self.search_export_bar = ctk.CTkProgressBar(exp_prog_frame, height=16)
        self.search_export_bar.grid(row=0, column=0, sticky="ew", padx=(0, 8))
        self.search_export_bar.set(0)

        self.search_export_pct = ctk.CTkLabel(
            exp_prog_frame, text="",
            font=("Segoe UI", 13, "bold"), width=56, anchor="e",
        )
        self.search_export_pct.grid(row=0, column=1)

    def _refresh_search_col_menu(self):
        """Actualiza el dropdown de columna de búsqueda según el primer archivo cargado."""
        if not self.search_files:
            self.search_col_menu.configure(values=["(sin columnas)"])
            self.search_col_var.set("(sin columnas)")
            return
        try:
            enc   = detect_encoding(self.search_files[0])
            delim = detect_delimiter(self.search_files[0], enc)
            cols  = get_columns(self.search_files[0], enc, delim)
        except Exception:
            cols = []
        values = cols if cols else ["(sin columnas)"]
        self.search_col_menu.configure(values=values)
        # Default: "SN" directo; si no existe, buscar col real mapeada a "SN"
        if "SN" in values:
            self.search_col_var.set("SN")
        else:
            sn_real = next(
                (real for real, preset in self.search_rename_map.items()
                 if preset == "SN" and real in values),
                None,
            )
            self.search_col_var.set(sn_real if sn_real else values[0])

    def _check_search_preset_columns(self):
        """
        Verifica si el primer CSV de búsqueda tiene todas las columnas preset.
        Si faltan, abre el ColumnMapDialog para que el usuario mapee equivalentes.
        El resultado se guarda en self.search_rename_map = {real_col: preset_col}.
        """
        if not self.search_files:
            return
        try:
            enc   = detect_encoding(self.search_files[0])
            delim = detect_delimiter(self.search_files[0], enc)
            cols  = get_columns(self.search_files[0], enc, delim)
        except Exception:
            return
        missing = [c for c in PRESET_COLUMNS if c not in cols]
        if not missing:
            return  # Todas las columnas preset presentes, no hay nada que mapear

        dialog = ColumnMapDialog(self, missing, cols)
        self.wait_window(dialog)
        if dialog.cancelled:
            return

        # dialog.result = {preset_col: real_col | None}
        # Invertir a {real_col: preset_col} para renombrar en _search_thread
        self.search_rename_map = {}
        for preset_col, real_col in dialog.result.items():
            if real_col is not None:
                self.search_rename_map[real_col] = preset_col

        # Refrescar dropdown para que el default SN use el mapeo nuevo
        self._refresh_search_col_menu()

    def _refresh_json_col_menu(self):
        """Actualiza los dropdowns de carpeta y archivo; auto-selecciona por defecto."""
        values = self.columns if self.columns else ["(sin columnas)"]
        self.json_folder_col_menu.configure(values=values)
        self.json_file_col_menu.configure(values=values)

        # Auto-seleccionar PHONEMODEL_NAME para carpetas
        folder_default = values[0]
        for real, preset in self.column_rename_map.items():
            if preset == "PHONEMODEL_NAME" and real in values:
                folder_default = real
                break
        else:
            if "PHONEMODEL_NAME" in values:
                folder_default = "PHONEMODEL_NAME"
        self.json_folder_col_var.set(folder_default)

        # Auto-seleccionar SN para archivos
        file_default = values[0]
        for real, preset in self.column_rename_map.items():
            if preset == "SN" and real in values:
                file_default = real
                break
        else:
            if "SN" in values:
                file_default = "SN"
        self.json_file_col_var.set(file_default)

        self._update_json_dest_label()

    def _update_json_dest_label(self):
        """Actualiza la etiqueta de ruta de destino con la estructura de dos niveles."""
        if not self.filepath.get():
            self.json_dest_label.configure(
                text="<carpeta del CSV> / JSON / <carpeta> / <archivo>.json"
            )
            return
        p          = Path(self.filepath.get())
        folder_col = self.json_folder_col_var.get()
        file_col   = self.json_file_col_var.get()
        self.json_dest_label.configure(
            text=f"{p.parent / 'Export_JSON'}\\<{folder_col}>\\<{file_col}>.json"
        )

    def _open_json_dest_folder(self):
        """Abre en el Explorador la carpeta JSON de destino (la crea si no existe)."""
        if not self.filepath.get():
            messagebox.showwarning("Sin archivo", "Cargá un archivo CSV primero.")
            return
        folder = Path(self.filepath.get()).parent / "Export_JSON"
        folder.mkdir(parents=True, exist_ok=True)
        os.startfile(str(folder))

    def _cancel_json(self):
        """Solicita la cancelación de la exportación JSON en curso."""
        self._json_cancel = True
        self.json_cancel_btn.configure(state="disabled")

    def _cancel_txt(self):
        """Solicita la cancelación de la exportación TXT en curso."""
        self._txt_cancel = True
        self.txt_cancel_btn.configure(state="disabled")

    def _cancel_processing(self):
        """Solicita la cancelación del procesamiento CSV en curso."""
        self._csv_cancel = True
        self.csv_cancel_btn.configure(state="disabled")

    def _open_output_folder(self):
        """Abre en el explorador la carpeta de salida configurada."""
        d = self.output_dir.get()
        if d and Path(d).exists():
            os.startfile(d)

    # ─────────────────────────────────────────────────────────────────────────
    # Búsqueda en CSVs — lógica
    # ─────────────────────────────────────────────────────────────────────────

    def search_start(self):
        """Valida la configuración e inicia la búsqueda en los archivos cargados.
        Acepta uno o varios valores separados por coma y/o salto de línea."""
        raw = self.search_input.get("1.0", "end-1c").strip()
        if not raw or raw.startswith("ej:"):
            messagebox.showwarning("Buscar", "Ingresá un valor a buscar.")
            return
        # Parsear múltiples valores: separados por coma o newline
        import re as _re
        values = [v.strip() for v in _re.split(r"[,\n]+", raw) if v.strip()]
        if not values:
            messagebox.showwarning("Buscar", "Ingresá al menos un valor a buscar.")
            return
        if not self.search_files:
            messagebox.showwarning("Buscar", "Agregá al menos un archivo CSV en el panel izquierdo.")
            return
        col = self.search_col_var.get()
        if not col or col == "(sin columnas)":
            messagebox.showwarning("Buscar", "Seleccioná la columna en la que buscar.")
            return

        self.search_results.clear()
        self.search_progress_bar.set(0)
        self.search_pct_label.configure(text="0%")
        n_vals = len(values)
        lbl = f"⏳ Buscando {n_vals} valor(es)..." if n_vals > 1 else "⏳ Buscando..."
        self.search_status_label.configure(text=lbl, text_color="orange")
        self._search_cancel = False
        self.search_cancel_btn.configure(state="normal")
        self.search_btn.configure(state="disabled")

        threading.Thread(
            target=self._search_thread,
            args=(values, col),
            daemon=True,
        ).start()

    # Alias conocidos para la columna SN: contienen los mismos datos,
    # se prueban en orden de prioridad antes de caer al mapeo del usuario.
    _SN_ALIASES: list[str] = ["SN", "STR_PSN"]

    def _resolve_search_col(self, search_col: str, file_cols: set[str]) -> str | None:
        """
        Dado el nombre de columna seleccionado y el conjunto de columnas del archivo,
        devuelve la columna real a usar o None si no se puede resolver.

        Prioridad:
          1. Si la búsqueda apunta a SN/STR_PSN, probar los alias en orden (SN > STR_PSN).
          2. Intentar search_col directamente.
          3. Intentar el preset lógico mapeado (ej. search_col="COD_SERIE" → preset="SN").
          4. None → el archivo no tiene la columna buscada.
        """
        target_preset = self.search_rename_map.get(search_col, search_col)

        # Si el objetivo es SN-relacionado, usar el alias que exista con mayor prioridad
        if search_col in self._SN_ALIASES or target_preset in self._SN_ALIASES:
            for alias in self._SN_ALIASES:
                if alias in file_cols:
                    return alias

        # Fallback: columna seleccionada o su preset
        if search_col in file_cols:
            return search_col
        if target_preset in file_cols:
            return target_preset

        return None

    def _search_thread(self, values: list[str], search_col: str):
        """Busca cada valor de `values` en `search_col` en todos los archivos cargados."""
        total       = len(self.search_files) * len(values)
        step        = 0
        all_results: list[dict] = []

        for filepath in self.search_files:
            if self._search_cancel:
                break
            try:
                enc   = detect_encoding(filepath)
                delim = detect_delimiter(filepath, enc)

                file_cols  = set(get_columns(filepath, enc, delim))
                actual_col = self._resolve_search_col(search_col, file_cols)
                if actual_col is None:
                    step += len(values)
                    continue  # este archivo no tiene la columna buscada

                for value in values:
                    if self._search_cancel:
                        break
                    rows = search_value_in_csv(
                        filepath      = filepath,
                        encoding      = enc,
                        delimiter     = delim,
                        search_column = actual_col,
                        search_value  = value,
                        cancel_fn     = lambda: self._search_cancel,
                    )
                    all_results.extend(rows)
                    step   += 1
                    pct     = step / total
                    pct_int = int(pct * 100)
                    self.after(0, lambda p=pct: self.search_progress_bar.set(p))
                    self.after(0, lambda t=pct_int: self.search_pct_label.configure(text=f"{t}%"))
            except Exception:
                step += len(values)   # saltar silenciosamente

        # ── Aplicar mapeo de columnas: renombrar claves reales al nombre preset ──
        if self.search_rename_map:
            renamed: list[dict] = []
            for rec in all_results:
                new_rec = {}
                for k, v in rec.items():
                    new_rec[self.search_rename_map.get(k, k)] = v
                renamed.append(new_rec)
            all_results = renamed

        self.search_results = all_results

        # ── Detectar qué columnas preset están disponibles y cuáles faltan ──────
        all_keys = {k for r in all_results for k in r if k not in ("_file", "_row")}
        found_preset   = [c for c in PRESET_COLUMNS if c in all_keys]
        missing_preset = [c for c in PRESET_COLUMNS if c not in all_keys]

        # Para cada columna faltante, sugerir la columna disponible más parecida
        def _best_match(preset_col: str, available: set) -> str | None:
            pl = preset_col.lower()
            for col in sorted(available):
                if pl in col.lower() or col.lower() in pl:
                    return col
            return None

        vals_label = ", ".join(values) if len(values) <= 3 else f"{len(values)} valores"
        if self._search_cancel:
            msg   = f"Cancelado  |  {len(all_results):,} resultado(s) parciales"
            color = "orange"
        elif not all_results:
            msg   = f'Sin resultados para: {vals_label}'
            color = "gray"
        else:
            files_found = len({r["_file"] for r in all_results})
            msg = f"✓  {len(all_results):,} resultado(s) en {files_found} archivo(s)"
            if missing_preset:
                hints = []
                for mc in missing_preset:
                    suggestion = _best_match(mc, all_keys - set(found_preset))
                    hints.append(f"{mc} → ¿'{suggestion}'?" if suggestion else mc)
                msg += f"\n⚠  Columna(s) no encontrada(s): {',  '.join(hints)}"
            color = ("green", "#4ec94e")

        self.after(0, lambda fp=found_preset: self._populate_search_treeview(all_results, fp))
        self.after(0, lambda m=msg, c=color: self.search_status_label.configure(text=m, text_color=c))
        self.after(0, lambda: self.search_cancel_btn.configure(state="disabled"))
        self.after(0, lambda: self.search_btn.configure(state="normal"))
        self.after(0, lambda: self.search_progress_bar.set(1.0))
        self.after(0, lambda: self.search_pct_label.configure(text="100%"))

    def _populate_search_treeview(self, results: list[dict], preset_cols: list[str]):
        """Actualiza el Treeview mostrando solo las columnas del preset encontradas."""
        display_cols = ["Archivo", "Fila"] + preset_cols
        self.search_tree["columns"] = display_cols

        self.search_tree.heading("Archivo", text="Archivo", anchor="w")
        self.search_tree.column("Archivo", width=200, minwidth=120, anchor="w")
        self.search_tree.heading("Fila", text="Fila", anchor="center")
        self.search_tree.column("Fila", width=60, minwidth=50, anchor="center")
        for col in preset_cols:
            self.search_tree.heading(col, text=col, anchor="w")
            self.search_tree.column(col, width=150, minwidth=70, anchor="w")

        # Limpiar y poblar filas
        for item in self.search_tree.get_children():
            self.search_tree.delete(item)

        # Asignar un color distinto a cada archivo de origen
        unique_files = list(dict.fromkeys(r.get("_file", "") for r in results))
        mode_idx = 0 if ctk.get_appearance_mode().lower() == "dark" else 1
        file_tag_map: dict[str, str] = {}
        for i, fname in enumerate(unique_files):
            tag_name = f"src_file_{i}"
            bg_color = SEARCH_FILE_PALETTE[i % len(SEARCH_FILE_PALETTE)][mode_idx]
            self.search_tree.tag_configure(tag_name, background=bg_color)
            file_tag_map[fname] = tag_name

        for rec in results:
            vals = [rec.get("_file", ""), rec.get("_row", "")]
            vals += [rec.get(col, "") for col in preset_cols]
            tag  = file_tag_map.get(rec.get("_file", ""), "")
            self.search_tree.insert("", "end", values=vals, tags=(tag,) if tag else ())

        # Actualizar los dropdowns de exportación con las columnas encontradas
        self._refresh_search_json_col_menus(preset_cols)

    def _cancel_search(self):
        """Solicita la cancelación de la búsqueda en curso."""
        self._search_cancel = True
        self.search_cancel_btn.configure(state="disabled")

    def _clear_search_results(self):
        """Limpia el campo de búsqueda, el treeview y los resultados en memoria."""
        self.search_input.delete("1.0", "end")
        self.search_input.insert("1.0", "ej: ZY32MJ3LZH\nO varios separados por coma o Enter")
        self.search_input.configure(text_color="gray60")
        self.search_results.clear()
        self.search_progress_bar.set(0)
        self.search_pct_label.configure(text="0%")
        self.search_status_label.configure(
            text="Agregá archivos CSV en el panel izquierdo e ingresá un valor a buscar.",
            text_color="gray",
        )
        # Vaciar treeview
        for item in self.search_tree.get_children():
            self.search_tree.delete(item)
        # Resetear dropdowns de exportación
        self._refresh_search_json_col_menus([])
        # Deshabilitar botón abrir carpeta
        self.search_json_open_btn.configure(state="disabled")

    def _refresh_search_json_col_menus(self, cols: list[str]):
        """Actualiza los dropdowns de carpeta/archivo para la exportación JSON de búsqueda."""
        values = cols if cols else ["(sin columnas)"]
        self.search_folder_col_menu.configure(values=values)
        self.search_file_col_menu.configure(values=values)
        self.search_folder_col_var.set(
            "PHONEMODEL_NAME" if "PHONEMODEL_NAME" in values else values[0]
        )
        self.search_file_col_var.set(
            "SN" if "SN" in values else values[0]
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Exportar JSON de resultados de búsqueda
    # ─────────────────────────────────────────────────────────────────────────

    def export_search_json_start(self):
        """Valida y lanza la exportación JSON de los resultados de búsqueda."""
        if not self.search_results:
            messagebox.showwarning("Exportar JSON", "No hay resultados de búsqueda. Realizá una búsqueda primero.")
            return
        folder_col = self.search_folder_col_var.get()
        file_col   = self.search_file_col_var.get()
        if not folder_col or folder_col == "(sin columnas)":
            messagebox.showwarning("Exportar JSON", "Seleccioná la columna para las carpetas.")
            return
        if not file_col or file_col == "(sin columnas)":
            messagebox.showwarning("Exportar JSON", "Seleccioná la columna para los archivos.")
            return

        # Exportar junto al primer archivo cargado, en subcarpeta "Search_Export_JSON"
        base = Path(self.search_files[0]).parent if self.search_files else Path(self.output_dir.get())
        output_dir = base / "Search_Export_JSON"
        output_dir.mkdir(parents=True, exist_ok=True)

        self.search_status_label.configure(text="⏳ Exportando JSON...", text_color="orange")
        self.search_export_bar.set(0)
        self.search_export_pct.configure(text="0%")

        threading.Thread(
            target=self._export_search_json_thread,
            args=(folder_col, file_col, output_dir),
            daemon=True,
        ).start()

    def _export_search_json_thread(self, folder_col: str, file_col: str, output_dir: Path):
        """Agrupa los resultados de búsqueda en memoria y escribe los archivos JSON."""
        _export_success = False
        try:
            # Agrupar en memoria (no re-lee los CSVs)
            grouped: dict[str, dict[str, list[dict]]] = {}
            for rec in self.search_results:
                fv  = str(rec.get(folder_col, "")).strip() or "_sin_carpeta"
                fiv = str(rec.get(file_col,   "")).strip() or "_sin_archivo"
                grouped.setdefault(fv, {}).setdefault(fiv, []).append(rec)

            total_files = max(sum(len(v) for v in grouped.values()), 1)
            n_files = 0
            n_rows  = 0

            for folder_val, files_dict in grouped.items():
                safe_folder = sanitize_filename(folder_val)
                folder_path = output_dir / safe_folder
                folder_path.mkdir(parents=True, exist_ok=True)

                for file_val, records in files_dict.items():
                    # Transformar: solo columnas PRESET → camelCase (classCode se deja tal cual)
                    transformed = []
                    for rec in records:
                        new_rec = {}
                        for k, v in rec.items():
                            if k in ("_file", "_row"):
                                continue
                            if k not in PRESET_COLUMNS:        # ignorar columnas no-preset
                                continue
                            json_key = PRESET_TO_JSON_KEY.get(k, k)
                            new_rec[json_key] = v
                        transformed.append(new_rec)

                    safe_file = sanitize_filename(file_val)
                    out_file  = folder_path / f"{safe_file}.json"
                    with open(str(out_file), "w", encoding="utf-8") as fh:
                        json.dump(transformed, fh, ensure_ascii=False, indent=4)

                    n_rows  += len(transformed)
                    n_files += 1
                    pct     = n_files / total_files
                    pct_int = int(pct * 100)
                    self.after(0, lambda p=pct: self.search_export_bar.set(p))
                    self.after(0, lambda t=pct_int: self.search_export_pct.configure(text=f"{t}%"))

            n_folders = len(grouped)
            msg   = (
                f"✓  {n_folders} carpeta(s)  |  {n_files} archivo(s)  |  "
                f"{n_rows:,} filas  →  {output_dir}"
            )
            color = ("green", "#4ec94e")
            self._search_json_last_dir = output_dir
            _export_success = True

        except Exception as exc:
            msg   = f"Error: {exc}"
            color = "red"

        self.after(0, lambda: self.search_export_bar.set(1.0))
        self.after(0, lambda: self.search_export_pct.configure(text="100%"))
        self.after(0, lambda m=msg, c=color: self.search_status_label.configure(text=m, text_color=c))
        if _export_success:
            self.after(0, lambda: self.search_json_open_btn.configure(state="normal"))

    def _open_search_json_folder(self):
        """Abre en el explorador la última carpeta JSON/CSV exportada desde el tab Buscar."""
        if self._search_json_last_dir and self._search_json_last_dir.exists():
            os.startfile(str(self._search_json_last_dir))

    # ─────────────────────────────────────────────────────────────────────────
    # Exportar CSV de resultados de búsqueda
    # ─────────────────────────────────────────────────────────────────────────

    def export_search_csv_start(self):
        """Valida y lanza la exportación CSV de los resultados de búsqueda."""
        if not self.search_results:
            messagebox.showwarning("Exportar CSV", "No hay resultados de búsqueda. Realizá una búsqueda primero.")
            return
        folder_col = self.search_folder_col_var.get()
        file_col   = self.search_file_col_var.get()
        if not folder_col or folder_col == "(sin columnas)":
            messagebox.showwarning("Exportar CSV", "Seleccioná la columna para las carpetas.")
            return
        if not file_col or file_col == "(sin columnas)":
            messagebox.showwarning("Exportar CSV", "Seleccioná la columna para los archivos.")
            return

        base = Path(self.search_files[0]).parent if self.search_files else Path(self.output_dir.get())
        output_dir = base / "Search_Export_CSV"
        output_dir.mkdir(parents=True, exist_ok=True)

        self.search_status_label.configure(text="⏳ Exportando CSV...", text_color="orange")
        self.search_export_bar.set(0)
        self.search_export_pct.configure(text="0%")

        threading.Thread(
            target=self._export_search_csv_thread,
            args=(folder_col, file_col, output_dir),
            daemon=True,
        ).start()

    def _export_search_csv_thread(self, folder_col: str, file_col: str, output_dir: Path):
        """Agrupa los resultados y escribe un CSV por archivo con las columnas PRESET."""
        _export_success = False
        try:
            # Determinar qué columna usar para carpetas: priorizar PHONEMODEL_NAME
            actual_folder_col = folder_col
            if self.search_results:
                # Verificar si PHONEMODEL_NAME existe en los datos
                has_phonemodel = any("PHONEMODEL_NAME" in rec for rec in self.search_results)
                if has_phonemodel:
                    actual_folder_col = "PHONEMODEL_NAME"

            # Agrupar en memoria
            grouped: dict[str, dict[str, list[dict]]] = {}
            for rec in self.search_results:
                fv  = str(rec.get(actual_folder_col, "")).strip() or "_sin_carpeta"
                fiv = str(rec.get(file_col,   "")).strip() or "_sin_archivo"
                grouped.setdefault(fv, {}).setdefault(fiv, []).append(rec)

            total_files = max(sum(len(v) for v in grouped.values()), 1)
            n_files = 0
            n_rows  = 0

            for folder_val, files_dict in grouped.items():
                safe_folder = sanitize_filename(folder_val)
                folder_path = output_dir / safe_folder
                folder_path.mkdir(parents=True, exist_ok=True)

                for file_val, records in files_dict.items():
                    safe_file = sanitize_filename(file_val)
                    out_file  = folder_path / f"{safe_file}.csv"

                    with open(str(out_file), "w", newline="", encoding="utf-8-sig") as fh:
                        writer = csv.DictWriter(
                            fh,
                            fieldnames=PRESET_COLUMNS,
                            extrasaction="ignore",
                        )
                        writer.writeheader()
                        for rec in records:
                            row = {k: rec.get(k, "") for k in PRESET_COLUMNS}
                            writer.writerow(row)

                    n_rows  += len(records)
                    n_files += 1
                    pct     = n_files / total_files
                    pct_int = int(pct * 100)
                    self.after(0, lambda p=pct: self.search_export_bar.set(p))
                    self.after(0, lambda t=pct_int: self.search_export_pct.configure(text=f"{t}%"))

            n_folders = len(grouped)
            msg   = (
                f"✓  {n_folders} carpeta(s)  |  {n_files} archivo(s) CSV  |  "
                f"{n_rows:,} filas  →  {output_dir}"
            )
            color = ("green", "#4ec94e")
            self._search_json_last_dir = output_dir
            _export_success = True

        except Exception as exc:
            msg   = f"Error al exportar CSV: {exc}"
            color = "red"

        self.after(0, lambda: self.search_export_bar.set(1.0))
        self.after(0, lambda: self.search_export_pct.configure(text="100%"))
        self.after(0, lambda m=msg, c=color: self.search_status_label.configure(text=m, text_color=c))
        if _export_success:
            self.after(0, lambda: self.search_json_open_btn.configure(state="normal"))

    def export_json_start(self):
        """Valida la configuración e inicia la exportación JSON en un hilo de fondo."""
        if not self.filepath.get():
            messagebox.showwarning("Sin archivo", "Cargá un archivo CSV primero.")
            return

        folder_col = self.json_folder_col_var.get()
        file_col   = self.json_file_col_var.get()
        if not folder_col or folder_col == "(sin columnas)":
            messagebox.showwarning("Exportar JSON", "Seleccioná una columna para las carpetas.")
            return
        if not file_col or file_col == "(sin columnas)":
            messagebox.showwarning("Exportar JSON", "Seleccioná una columna para los archivos.")
            return

        # Determinar qué columnas incluir en el JSON
        if self.json_cols_var.get() == "selected":
            cols = [col for col, var in self.column_vars.items() if var.get()]
            if not cols:
                messagebox.showwarning(
                    "Exportar JSON",
                    "No hay columnas seleccionadas en el panel izquierdo.\n"
                    "Marcá al menos una o elegí «Todas las columnas».",
                )
                return
        else:
            cols = list(self.columns)

        # Asegurar que ambas columnas de segmentación estén incluidas
        if folder_col not in cols:
            cols = [folder_col] + cols
        if file_col not in cols:
            cols = [file_col] + cols

        self.json_status_label.configure(text="⏳ Leyendo archivo...", text_color="orange")
        self.json_progress_bar.set(0)
        self.json_pct_label.configure(text="0%")
        self._json_cancel = False
        self.json_cancel_btn.configure(state="normal")

        threading.Thread(
            target=self._export_json_thread,
            args=(folder_col, file_col, cols),
            daemon=True,
        ).start()

    def _export_json_thread(self, folder_col: str, file_col: str, columns: list[str]):
        """Lee el CSV en segundo plano y escribe un JSON por SN dentro de carpetas por phoneModelName."""
        try:
            input_path = Path(self.filepath.get())
            output_dir = input_path.parent / "Export_JSON"
            output_dir.mkdir(parents=True, exist_ok=True)

            # ── Fase 1: lectura del CSV (overlay activo) ──────────────────────
            result = collect_rows_by_two_groups(
                filepath        = str(input_path),
                encoding        = self.detected_encoding,
                delimiter       = self.detected_delimiter,
                folder_column   = folder_col,
                file_column     = file_col,
                columns_to_read = columns,
                rename_map      = dict(self.column_rename_map),
            )

            # ── Fase 2: escritura de archivos JSON (mostrar progreso) ──────────
            total_files = max(sum(len(files) for files in result.values()), 1)
            n_files = 0
            n_rows  = 0

            for folder_val, files_dict in result.items():
                if self._json_cancel:
                    break
                safe_folder = sanitize_filename(folder_val)
                folder_path = output_dir / safe_folder
                folder_path.mkdir(parents=True, exist_ok=True)

                for file_val, records in files_dict.items():
                    if self._json_cancel:
                        break
                    # Transformar cada registro:
                    #   · renombrar claves al camelCase del preset (CLASSCODE → classCode, etc.)
                    #   · traducir el valor de classCode a su descripción completa
                    transformed = []
                    for rec in records:
                        new_rec = {}
                        for k, v in rec.items():
                            json_key = PRESET_TO_JSON_KEY.get(k, k)   # camelCase si es preset
                            if json_key == "classCode":
                                v = CLASS_CODE_MAP.get(v, v)           # descripción completa, fallback al código
                            new_rec[json_key] = v
                        transformed.append(new_rec)

                    safe_file = sanitize_filename(file_val)
                    out_file  = folder_path / f"{safe_file}.json"
                    with open(str(out_file), "w", encoding="utf-8") as fh:
                        json.dump(transformed, fh, ensure_ascii=False, indent=4)
                    n_rows  += len(transformed)
                    n_files += 1
                    pct     = n_files / total_files
                    pct_int = int(pct * 100)
                    self.after(0, lambda p=pct: self.json_progress_bar.set(p))
                    self.after(0, lambda t=pct_int: self.json_pct_label.configure(text=f"{t}%"))

            if self._json_cancel:
                msg   = f"Cancelado  |  {n_files} archivo(s) escrito(s)  |  {n_rows:,} filas"
                color = "orange"
            else:
                n_folders = len(result)
                msg   = (
                    f"✓  {n_folders} carpeta(s)  |  {n_files} archivo(s)  |  "
                    f"{n_rows:,} filas  →  {output_dir}"
                )
                color = ("green", "#4ec94e")

        except Exception as exc:
            msg   = f"Error: {exc}"
            color = "red"

        self.after(0, lambda: self.json_cancel_btn.configure(state="disabled"))
        self.after(0, lambda: self.json_progress_bar.set(1.0))
        self.after(0, lambda: self.json_pct_label.configure(text="100%"))
        self.after(0, lambda m=msg, c=color: self.json_status_label.configure(text=m, text_color=c))

    # ─────────────────────────────────────────────────────────────────────────
    # Agregar Columna — tab y lógica
    # ─────────────────────────────────────────────────────────────────────────

    def _build_add_column_tab(self, parent):
        """Tab para agregar una nueva columna con valor constante a un CSV."""
        parent.grid_columnconfigure(0, weight=1)
        parent.grid_rowconfigure(1, weight=1)   # fila espaciadora

        # ── Panel de configuración ────────────────────────────────────────────
        cfg = ctk.CTkFrame(parent)
        cfg.grid(row=0, column=0, sticky="ew", padx=6, pady=(6, 4))
        cfg.grid_columnconfigure(1, weight=1)

        # Nombre de la nueva columna
        ctk.CTkLabel(cfg, text="Nombre de columna:", width=140, anchor="e").grid(
            row=0, column=0, padx=(10, 4), pady=8
        )
        self.add_col_name_var = tk.StringVar(value="")
        self.add_col_name_entry = ctk.CTkEntry(
            cfg, textvariable=self.add_col_name_var, placeholder_text="Ej: HONEMODEL_NAME"
        )
        self.add_col_name_entry.grid(row=0, column=1, sticky="ew", padx=(0, 10), pady=8)

        # Valor constante
        ctk.CTkLabel(cfg, text="Valor constante:", width=140, anchor="e").grid(
            row=1, column=0, padx=(10, 4), pady=(0, 8)
        )
        self.add_col_value_var = tk.StringVar(value="")
        self.add_col_value_entry = ctk.CTkEntry(
            cfg, textvariable=self.add_col_value_var, placeholder_text="Ej: MODELO_DEFAULT"
        )
        self.add_col_value_entry.grid(row=1, column=1, sticky="ew", padx=(0, 10), pady=(0, 8))

        # Separador
        ctk.CTkFrame(cfg, height=2, fg_color="gray40").grid(
            row=2, column=0, columnspan=2, sticky="ew", padx=10, pady=(0, 8)
        )

        # Posición de la columna
        ctk.CTkLabel(cfg, text="Posición:", width=140, anchor="e").grid(
            row=3, column=0, padx=(10, 4), pady=(0, 4)
        )
        self.add_col_position_var = tk.StringVar(value="end")
        pos_frame = ctk.CTkFrame(cfg, fg_color="transparent")
        pos_frame.grid(row=3, column=1, sticky="w", padx=(0, 10), pady=(0, 4))

        ctk.CTkRadioButton(
            pos_frame, text="Al inicio",
            variable=self.add_col_position_var, value="start",
            command=self._on_add_col_position_change,
        ).pack(anchor="w", pady=(0, 4))
        ctk.CTkRadioButton(
            pos_frame, text="Al final",
            variable=self.add_col_position_var, value="end",
            command=self._on_add_col_position_change,
        ).pack(anchor="w", pady=(0, 4))

        after_frame = ctk.CTkFrame(pos_frame, fg_color="transparent")
        after_frame.pack(anchor="w")
        ctk.CTkRadioButton(
            after_frame, text="Después de:",
            variable=self.add_col_position_var, value="after",
            command=self._on_add_col_position_change,
        ).pack(side="left", padx=(0, 8))
        self.add_col_after_var = tk.StringVar(value="(sin columnas)")
        self.add_col_after_menu = ctk.CTkOptionMenu(
            after_frame,
            variable=self.add_col_after_var,
            values=["(sin columnas)"],
            width=200,
            state="disabled",
        )
        self.add_col_after_menu.pack(side="left")

        # ── Ruta de destino ───────────────────────────────────────────────────
        dest_frame = ctk.CTkFrame(parent, fg_color=("#dbe8f5", "#1a3a5c"), corner_radius=8)
        dest_frame.grid(row=2, column=0, sticky="ew", padx=8, pady=(6, 4))
        dest_frame.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(
            dest_frame, text="📁  Destino:",
            font=("Segoe UI", 13, "bold"),
            anchor="w",
        ).grid(row=0, column=0, sticky="w", padx=12, pady=(8, 0))

        self.add_col_dest_label = ctk.CTkLabel(
            dest_frame,
            text="<carpeta del CSV> / CSV_con_columna_agregada / <nombre_archivo>.csv",
            font=("Segoe UI", 13),
            anchor="w",
            wraplength=520,
            justify="left",
        )
        self.add_col_dest_label.grid(row=1, column=0, sticky="w", padx=12, pady=(2, 10))

        # Botón "Abrir carpeta"
        self.add_col_open_btn = ctk.CTkButton(
            dest_frame,
            text="📂  Abrir carpeta",
            command=self._open_add_col_folder,
            width=150, height=32,
            font=("Segoe UI", 11, "bold"),
            fg_color=("gray75", "gray30"),
            hover_color=("gray65", "gray40"),
            text_color=("gray10", "gray90"),
            state="disabled",
        )
        self.add_col_open_btn.grid(row=1, column=1, padx=(4, 12), pady=(2, 10), sticky="e")

        # ── Botones procesar/cancelar + barra de progreso ─────────────────────
        btns_frame = ctk.CTkFrame(parent, fg_color="transparent")
        btns_frame.grid(row=3, column=0, padx=12, pady=(6, 4), sticky="w")
        ctk.CTkButton(
            btns_frame,
            text="➕  Agregar Columna",
            command=self.add_column_start,
            height=38,
            font=("Segoe UI", 12, "bold"),
            fg_color="#2d7d46", hover_color="#1f5c32",
        ).pack(side="left")

        # Barra de progreso
        prog_frame = ctk.CTkFrame(parent, fg_color="transparent")
        prog_frame.grid(row=4, column=0, sticky="ew", padx=12, pady=(0, 2))
        prog_frame.grid_columnconfigure(0, weight=1)
        self.add_col_progress_bar = ctk.CTkProgressBar(prog_frame, height=20)
        self.add_col_progress_bar.grid(row=0, column=0, sticky="ew", padx=(0, 8))
        self.add_col_progress_bar.set(0)
        self.add_col_pct_label = ctk.CTkLabel(
            prog_frame, text="0%",
            font=("Segoe UI", 11, "bold"),
            width=50,
        )
        self.add_col_pct_label.grid(row=0, column=1)

        # Etiqueta de estado
        self.add_col_status_label = ctk.CTkLabel(
            parent, text="Listo para procesar.",
            font=("Segoe UI", 11),
            anchor="w",
        )
        self.add_col_status_label.grid(row=5, column=0, sticky="w", padx=12, pady=(2, 10))

        # Variable para la carpeta de salida
        self._add_col_last_dir = None

    def _on_add_col_position_change(self):
        """Habilita/deshabilita el selector de columna según la posición elegida."""
        if self.add_col_position_var.get() == "after":
            self.add_col_after_menu.configure(state="normal")
        else:
            self.add_col_after_menu.configure(state="disabled")

    def _update_add_col_columns(self):
        """Actualiza el dropdown de columnas disponibles para 'después de'."""
        if not self.columns:
            values = ["(sin columnas)"]
        else:
            values = list(self.columns)
        self.add_col_after_menu.configure(values=values)
        if values and values[0] != "(sin columnas)":
            self.add_col_after_var.set(values[0])
        else:
            self.add_col_after_var.set("(sin columnas)")

    def _open_add_col_folder(self):
        """Abre la carpeta donde se guardó el CSV con la columna agregada."""
        if self._add_col_last_dir and self._add_col_last_dir.exists():
            os.startfile(str(self._add_col_last_dir))

    def add_column_start(self):
        """Valida y lanza el procesamiento para agregar columna."""
        if not self.filepath.get():
            messagebox.showwarning("Sin archivo", "Cargá un archivo CSV primero.")
            return

        col_name = self.add_col_name_var.get().strip().upper()
        if not col_name:
            messagebox.showwarning("Agregar Columna", "Ingresá el nombre de la columna a agregar.")
            return

        col_value = self.add_col_value_var.get().strip().upper()
        if not col_value:
            messagebox.showwarning("Agregar Columna", "Ingresá el valor constante para la columna.")
            return

        position = self.add_col_position_var.get()
        after_col = None
        if position == "after":
            after_col = self.add_col_after_var.get()
            if not after_col or after_col == "(sin columnas)":
                messagebox.showwarning("Agregar Columna", "Seleccioná la columna después de la cual insertar.")
                return

        self.add_col_status_label.configure(text="⏳ Procesando...", text_color="orange")
        self.add_col_progress_bar.set(0)
        self.add_col_pct_label.configure(text="0%")

        threading.Thread(
            target=self._add_column_thread,
            args=(col_name, col_value, position, after_col),
            daemon=True,
        ).start()

    def _add_column_thread(self, col_name: str, col_value: str, position: str, after_col: Optional[str]):
        """Ejecuta add_column_to_csv en segundo plano."""
        try:
            def cb(pct, msg):
                self.after(0, lambda p=pct: self.add_col_progress_bar.set(p))
                pct_int = int(pct * 100)
                self.after(0, lambda t=pct_int: self.add_col_pct_label.configure(text=f"{t}%"))

            result = add_column_to_csv(
                filepath=self.filepath.get(),
                encoding=self.detected_encoding,
                delimiter=self.detected_delimiter,
                column_name=col_name,
                column_value=col_value,
                position=position,
                after_column=after_col,
                output_dir=None,
                progress_callback=cb,
            )

            output_file = result["output_file"]
            total_rows = result["total_rows"]
            self._add_col_last_dir = Path(output_file).parent

            msg = f"✓  Columna '{col_name}' agregada  |  {total_rows:,} filas  →  {output_file}"
            color = ("green", "#4ec94e")

            self.after(0, lambda: self.add_col_open_btn.configure(state="normal"))

        except Exception as exc:
            msg = f"Error: {exc}"
            color = "red"

        self.after(0, lambda: self.add_col_progress_bar.set(1.0))
        self.after(0, lambda: self.add_col_pct_label.configure(text="100%"))
        self.after(0, lambda m=msg, c=color: self.add_col_status_label.configure(text=m, text_color=c))

    # ─────────────────────────────────────────────────────────────────────────
    # Carpeta de salida
    # ─────────────────────────────────────────────────────────────────────────

    def choose_output_dir(self):
        """Abre el diálogo para elegir la carpeta donde se guardan los CSVs generados."""
        d = filedialog.askdirectory(title="Seleccionar carpeta de salida")
        if d:
            self.output_dir.set(d)

    # ─────────────────────────────────────────────────────────────────────────
    # Procesamiento
    # ─────────────────────────────────────────────────────────────────────────

    def start_processing(self):
        """Valida la configuración e inicia el procesamiento CSV en un hilo de fondo."""
        if not self.filepath.get():
            messagebox.showwarning("Sin archivo", "Seleccioná un archivo CSV primero.")
            return

        selected = [col for col, var in self.column_vars.items() if var.get()]
        if not selected:
            messagebox.showwarning("Sin columnas", "Seleccioná al menos una columna para exportar.")
            return

        if self.processing:
            return

        delim_map = {"comma": ",", "semicolon": ";", "tab": "\t"}
        out_delim = delim_map[self.out_delimiter.get()]

        self.processing = True
        self._csv_cancel = False
        self.process_btn.configure(state="disabled", text="⏳ Procesando...")
        self.csv_cancel_btn.configure(state="normal")
        self.progress_bar.set(0)
        self.progress_pct_label.configure(text="0%")
        self.status_label.configure(text="Iniciando...", text_color="gray")

        # El hilo es daemon: se mata automáticamente si el proceso principal cierra
        thread = threading.Thread(
            target=self._run_in_thread,
            args=(selected, out_delim, dict(self.column_rename_map), dict(self.date_transforms)),
            daemon=True,
        )
        thread.start()
        self._poll_queue()

    def _run_in_thread(
        self,
        selected_columns: list[str],
        out_delimiter: str,
        rename_map: dict,
        date_transforms: dict,
    ):
        """Ejecuta process_csv en segundo plano y publica progreso/resultado en la queue."""
        def cb(pct, msg):
            if self._csv_cancel:
                raise InterruptedError("Cancelado por el usuario.")
            self.progress_queue.put(("progress", pct, msg))

        try:
            result = process_csv(
                filepath         = self.filepath.get(),
                encoding         = self.detected_encoding,
                delimiter        = self.detected_delimiter,
                selected_columns = selected_columns,
                filters          = dict(self.filters),
                output_dir       = self.output_dir.get(),
                out_delimiter    = out_delimiter,
                rename_map       = rename_map,
                date_transforms  = date_transforms,
                progress_callback= cb,
            )
            self.progress_queue.put(("done", result))
        except InterruptedError:
            self.progress_queue.put(("cancelled",))
        except Exception as exc:
            self.progress_queue.put(("error", str(exc)))

    def _poll_queue(self):
        """Consume mensajes de la queue de progreso y actualiza la UI en el hilo principal."""
        # Salir inmediatamente si la ventana ya fue destruida
        if self._closing or not self.winfo_exists():
            return

        try:
            while True:
                item = self.progress_queue.get_nowait()
                kind = item[0]

                if kind == "progress":
                    _, pct, msg = item
                    self.progress_bar.set(pct)
                    self.progress_pct_label.configure(text=f"{int(pct * 100)}%")
                    self.status_label.configure(text=msg, text_color="gray")

                elif kind == "done":
                    result   = item[1]
                    n_files  = len(result.get("files_created", []))
                    n_rows   = result.get("total_rows", 0)
                    split_col = result.get("split_column", "")
                    self.progress_bar.set(1.0)
                    self.progress_pct_label.configure(text="100%")
                    self.processing = False
                    self.process_btn.configure(state="normal", text="▶  PROCESAR")
                    self.csv_cancel_btn.configure(state="disabled")
                    self.open_output_btn.configure(state="normal")
                    self.status_label.configure(
                        text=f"✓  Completado: {n_files} archivo(s) generado(s)  |  {n_rows:,} filas exportadas",
                        text_color=("green", "#4ec94e"),
                    )
                    messagebox.showinfo(
                        "Proceso completado",
                        f"¡Éxito!\n\n"
                        f"Archivos generados: {n_files}\n"
                        f"Filas exportadas:   {n_rows:,}\n"
                        f"Dividido por:       «{split_col}»\n\n"
                        f"Guardados en:\n{self.output_dir.get()}",
                    )
                    return

                elif kind == "cancelled":
                    self.processing = False
                    self.process_btn.configure(state="normal", text="▶  PROCESAR")
                    self.csv_cancel_btn.configure(state="disabled")
                    self.progress_bar.set(0)
                    self.progress_pct_label.configure(text="0%")
                    self.status_label.configure(text="Cancelado.", text_color="orange")
                    return

                elif kind == "error":
                    error_msg = item[1]
                    self.processing = False
                    self.process_btn.configure(state="normal", text="▶  PROCESAR")
                    self.csv_cancel_btn.configure(state="disabled")
                    self.status_label.configure(text=f"Error: {error_msg}", text_color="red")
                    messagebox.showerror("Error en el procesamiento", f"{error_msg}")
                    return

        except queue.Empty:
            pass

        # Si aún está procesando, volver a verificar en 120 ms
        if self.processing:
            self.after(120, self._poll_queue)


# ─────────────────────────────────────────────────────────────────────────────
# Diálogo de mapeo de columnas faltantes
# ─────────────────────────────────────────────────────────────────────────────

class ColumnMapDialog(ctk.CTkToplevel):
    """
    Modal para mapear columnas del preset que no existen en el CSV a columnas equivalentes.
    Devuelve un dict {preset_col: col_real o None} en self.result.
    """

    SKIP = "(omitir)"

    def __init__(self, parent, missing: list[str], available: list[str]):
        super().__init__(parent)
        self.title("Columnas no encontradas — Mapeo")
        self.resizable(False, False)
        self.grab_set()       # bloquea la ventana principal mientras está abierto
        self.lift()
        self.focus_force()

        self.missing   = missing
        self.available = available
        self.cancelled = True
        self.result:   dict[str, str | None] = {}
        self._menus:   dict[str, tk.StringVar] = {}

        self._build()
        self._center()

    def _center(self):
        """Centra el diálogo sobre la ventana principal."""
        self.update_idletasks()
        pw = self.master.winfo_x() + self.master.winfo_width()  // 2
        ph = self.master.winfo_y() + self.master.winfo_height() // 2
        w, h = self.winfo_width(), self.winfo_height()
        self.geometry(f"+{pw - w // 2}+{ph - h // 2}")

    def _build(self):
        """Construye las filas de mapeo y los botones Aplicar/Cancelar."""
        self.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(
            self,
            text=(
                "Las siguientes columnas del preset no se encontraron en el CSV.\n"
                "Elegí una columna equivalente o elegí «(omitir)» para saltearla."
            ),
            font=("Segoe UI", 11), wraplength=480, justify="left",
        ).grid(row=0, column=0, padx=20, pady=(16, 10), sticky="w")

        ctk.CTkFrame(self, height=2, fg_color="gray40").grid(
            row=1, column=0, sticky="ew", padx=16, pady=(0, 10)
        )

        options = [self.SKIP] + self.available

        for i, col in enumerate(self.missing):
            row_frame = ctk.CTkFrame(self, fg_color="transparent")
            row_frame.grid(row=i + 2, column=0, sticky="ew", padx=16, pady=4)
            row_frame.grid_columnconfigure(1, weight=1)

            # Nombre del preset (etiqueta izquierda en dorado)
            ctk.CTkLabel(
                row_frame, text=col,
                font=("Segoe UI", 11, "bold"), width=180,
                anchor="e", text_color="#e0a040",
            ).grid(row=0, column=0, padx=(0, 10))

            ctk.CTkLabel(
                row_frame, text="→", font=("Segoe UI", 12), width=20,
            ).grid(row=0, column=1, padx=(0, 6))

            # Dropdown con sugerencia automática
            var  = tk.StringVar(value=self._best_guess(col))
            menu = ctk.CTkOptionMenu(row_frame, variable=var, values=options, width=220)
            menu.grid(row=0, column=2, sticky="ew")
            self._menus[col] = var

        n = len(self.missing)
        ctk.CTkFrame(self, height=2, fg_color="gray40").grid(
            row=n + 2, column=0, sticky="ew", padx=16, pady=(12, 0)
        )

        btn_row = ctk.CTkFrame(self, fg_color="transparent")
        btn_row.grid(row=n + 3, column=0, pady=14)

        ctk.CTkButton(
            btn_row, text="Aplicar selección", width=160,
            command=self._confirm,
            fg_color="#2d7d46", hover_color="#1f5c32",
        ).pack(side="left", padx=(0, 10))

        ctk.CTkButton(
            btn_row, text="Cancelar", width=100,
            fg_color="transparent", border_width=1,
            command=self._cancel,
        ).pack(side="left")

    def _best_guess(self, preset_col: str) -> str:
        """Sugiere la columna disponible más parecida al nombre del preset (búsqueda por substring)."""
        preset_lower = preset_col.lower()
        for col in self.available:
            if preset_lower in col.lower() or col.lower() in preset_lower:
                return col
        return self.SKIP

    def _confirm(self):
        """Guarda el mapeo seleccionado y cierra el diálogo."""
        self.cancelled = False
        for preset_col, var in self._menus.items():
            val = var.get()
            self.result[preset_col] = None if val == self.SKIP else val
        self.destroy()

    def _cancel(self):
        """Cancela sin guardar y cierra el diálogo."""
        self.cancelled = True
        self.destroy()
