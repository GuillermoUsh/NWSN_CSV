"""
app.py — Interfaz gráfica con CustomTkinter para el CSV Processor.
"""

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
    sanitize_filename,
    process_csv,
)

PREVIEW_ROWS = 200
APP_VERSION  = "1.0"
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

# Paleta de colores para el Treeview según el modo claro/oscuro
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
        self._build_column_panel(main)
        self._build_right_tabs(main)

    def _build_column_panel(self, parent):
        """Panel izquierdo con checkboxes para seleccionar las columnas de salida."""
        panel = ctk.CTkFrame(parent, width=275)
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
        """Crea el tabview con las pestañas Vista Previa y Exportar TXT."""
        self.tabview = ctk.CTkTabview(parent)
        self.tabview.grid(row=0, column=1, sticky="nsew")

        self.tabview.add("   Vista Previa   ")
        self.tabview.add("   Exportar TXT   ")
        # self.tabview.add("Filtros")       # TODO: descomentar para reactivar
        # self.tabview.add("Transformar")   # TODO: descomentar para reactivar

        # Hacer los botones de pestaña más grandes y visibles
        try:
            self.tabview._segmented_button.configure(
                font=("Segoe UI", 13, "bold"),
                height=38,
            )
        except Exception:
            pass

        self._build_preview_tab(self.tabview.tab("   Vista Previa   "))
        self._build_export_txt_tab(self.tabview.tab("   Exportar TXT   "))
        # self._build_filter_tab(self.tabview.tab("Filtros"))
        # self._build_transform_tab(self.tabview.tab("Transformar"))

    # ── Tab: Vista Previa ────────────────────────────────────────────────────

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

        self.process_btn = ctk.CTkButton(
            bottom, text="▶  PROCESAR",
            command=self.start_processing,
            font=("Segoe UI", 13, "bold"),
            width=160, height=40,
            fg_color="#2d7d46", hover_color="#1f5c32",
        )
        self.process_btn.grid(row=0, column=3, rowspan=2, padx=12, pady=8)

        self.progress_bar = ctk.CTkProgressBar(bottom, height=14)
        self.progress_bar.grid(row=2, column=0, columnspan=3, sticky="ew", padx=12, pady=(0, 4))
        self.progress_bar.set(0)

        self.status_label = ctk.CTkLabel(
            bottom, text="Listo.", text_color="gray", anchor="w",
            font=("Segoe UI", 9),
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
        self.output_dir.set(str(Path(path).parent / "ForModels"))

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
        dest_frame.grid(row=1, column=0, sticky="ew", padx=8, pady=(6, 4))
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

        # ── Botón exportar + barra de progreso + estado ───────────────────────
        ctk.CTkButton(
            parent,
            text="📄  Exportar TXT",
            command=self.export_txt_start,
            height=38,
            font=("Segoe UI", 12, "bold"),
            fg_color="#2d7d46", hover_color="#1f5c32",
        ).grid(row=2, column=0, padx=12, pady=(6, 4), sticky="w")

        # Barra de progreso para la exportación TXT
        self.txt_progress_bar = ctk.CTkProgressBar(parent, height=10)
        self.txt_progress_bar.grid(row=3, column=0, sticky="ew", padx=12, pady=(0, 2))
        self.txt_progress_bar.set(0)

        self.txt_status_label = ctk.CTkLabel(
            parent, text="", text_color="gray",
            font=("Segoe UI", 13, "bold"), anchor="w", wraplength=580,
        )
        self.txt_status_label.grid(row=4, column=0, sticky="w", padx=14, pady=(4, 8))

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
        folder = Path(self.filepath.get()).parent / "TXT"
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
        txt_folder = p.parent / "TXT"
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

        # Overlay bloqueante mientras dura la lectura del CSV
        self._show_overlay("📄  Exportando TXT...")

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
            output_dir = input_path.parent / "TXT"
            output_dir.mkdir(parents=True, exist_ok=True)

            if group_col:
                # ── Modo agrupado: un archivo por valor de group_col ──────────
                # Fase 1 — lectura del CSV (lenta); overlay activo
                groups = get_unique_values_by_group(
                    filepath     = str(input_path),
                    encoding     = self.detected_encoding,
                    delimiter    = self.detected_delimiter,
                    value_column = col,
                    group_column = group_col,
                )
                # Fase 2 — escritura de archivos; ocultar overlay y mostrar progreso
                self.after(0, self._hide_overlay)
                total   = max(len(groups), 1)
                n_total = 0
                for idx, (group_val, values) in enumerate(groups.items()):
                    out_file = output_dir / (sanitize_filename(group_val) + ".txt")
                    write_values(out_file, values)
                    n_total += len(values)
                    pct = (idx + 1) / total
                    self.after(0, lambda p=pct: self.txt_progress_bar.set(p))
                msg = (
                    f"✓  {len(groups)} archivo(s) creado(s)  |  "
                    f"{n_total:,} valores en total  →  {output_dir}"
                )
            else:
                # ── Modo sin agrupar: un solo archivo ─────────────────────────
                # Fase 1 — lectura; overlay activo
                values = get_unique_column_values(
                    filepath  = str(input_path),
                    encoding  = self.detected_encoding,
                    delimiter = self.detected_delimiter,
                    column    = col,
                )
                # Fase 2 — escritura; ocultar overlay
                self.after(0, self._hide_overlay)
                out_file = output_dir / (input_path.stem + ".txt")
                write_values(out_file, values)
                self.after(0, lambda: self.txt_progress_bar.set(1.0))
                msg = f"✓  {len(values):,} valores únicos exportados  →  {out_file}"

            color = ("green", "#4ec94e")
        except Exception as exc:
            self.after(0, self._hide_overlay)
            msg   = f"Error: {exc}"
            color = "red"

        self.after(0, lambda m=msg, c=color: self.txt_status_label.configure(text=m, text_color=c))

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
        self.process_btn.configure(state="disabled", text="⏳ Procesando...")
        self.progress_bar.set(0)
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
                    self.status_label.configure(text=msg, text_color="gray")

                elif kind == "done":
                    result   = item[1]
                    n_files  = len(result.get("files_created", []))
                    n_rows   = result.get("total_rows", 0)
                    split_col = result.get("split_column", "")
                    self.progress_bar.set(1.0)
                    self.processing = False
                    self.process_btn.configure(state="normal", text="▶  PROCESAR")
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

                elif kind == "error":
                    error_msg = item[1]
                    self.processing = False
                    self.process_btn.configure(state="normal", text="▶  PROCESAR")
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
