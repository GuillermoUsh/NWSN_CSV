"""
processor.py — Lógica de procesamiento de archivos CSV grandes.
"""

import re
import csv
import chardet
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional


# ─── Detección de encoding ────────────────────────────────────────────────────

def detect_encoding(filepath: str, sample_size: int = 200_000) -> str:
    """Lee los primeros N bytes del archivo y devuelve la codificación detectada."""
    with open(filepath, "rb") as f:
        raw = f.read(sample_size)
    result   = chardet.detect(raw)
    encoding = result.get("encoding") or "utf-8"
    # Normalizar alias problemáticos a nombres que Python acepta sin error
    mapping = {
        "ascii":        "utf-8",
        "iso-8859-1":  "latin-1",
        "windows-1252": "cp1252",
    }
    return mapping.get(encoding.lower(), encoding)


# ─── Detección de delimitador ─────────────────────────────────────────────────

def detect_delimiter(filepath: str, encoding: str) -> str:
    """Detecta el delimitador usando csv.Sniffer; fallback por conteo en la primera línea."""
    try:
        with open(filepath, "r", encoding=encoding, errors="replace") as f:
            sample = f.read(16_384)
        dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t")
        return dialect.delimiter
    except csv.Error:
        # Sniffer falló: elegir el candidato con mayor frecuencia en la primera línea
        with open(filepath, "r", encoding=encoding, errors="replace") as f:
            first_line = f.readline()
        candidates = {
            ",":  first_line.count(","),
            ";":  first_line.count(";"),
            "\t": first_line.count("\t"),
        }
        return max(candidates, key=candidates.get)


# ─── Lectura de cabecera / preview ────────────────────────────────────────────

def get_columns(filepath: str, encoding: str, delimiter: str) -> list[str]:
    """Devuelve la lista de nombres de columnas del CSV sin leer los datos."""
    df = pd.read_csv(
        filepath, sep=delimiter, encoding=encoding,
        nrows=0, on_bad_lines="skip", engine="python",
    )
    return list(df.columns)


def get_preview(filepath: str, encoding: str, delimiter: str, nrows: int = 200) -> pd.DataFrame:
    """Devuelve las primeras N filas como DataFrame de strings (sin NaN)."""
    df = pd.read_csv(
        filepath, sep=delimiter, encoding=encoding,
        nrows=nrows, on_bad_lines="skip", dtype=str, engine="python",
    )
    return df.fillna("")


# ─── Conteo de filas (para la barra de progreso) ──────────────────────────────

def count_rows_fast(filepath: str) -> int:
    """Cuenta líneas del archivo en binario (eficiente para 500k+ filas)."""
    count = 0
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(1_048_576), b""):
            count += chunk.count(b"\n")
    return max(count - 1, 0)   # descontar la línea de cabecera


# ─── Utilidades de nombre de archivo ─────────────────────────────────────────

def sanitize_filename(value: str, max_len: int = 100) -> str:
    """Convierte un valor de columna en un nombre de archivo seguro (sin caracteres especiales)."""
    safe = str(value).strip()
    for ch in r'\/:*?"<>|':
        safe = safe.replace(ch, "_")
    return safe[:max_len] if safe else "SIN_VALOR"


# Alias privado para compatibilidad interna
_sanitize_filename = sanitize_filename


# ─── Normalización de fechas ──────────────────────────────────────────────────

# Regex para detectar valores que contienen SOLO hora: HH:MM  /  HH:MM:SS  /  HH:MM.S
_TIME_ONLY_RE = re.compile(r"^\d{1,2}:\d{2}([:.]\d+)?$")

# Formatos completos de datetime a probar en orden
_DT_FORMATS = [
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%Y/%m/%d %H:%M:%S",
]


def normalize_datetime_value(value: str, base_date: str) -> str:
    """
    Normaliza un valor de fecha/hora al formato YYYY-MM-DD HH:MM:SS.mmm.

    Casos manejados:
    - Solo hora (ej: '11:46.0', '14:30:26') → se combina con base_date.
    - Datetime completo reconocible            → se reformatea.
    - Valor no parseable                       → se devuelve sin modificar.

    base_date debe ser 'YYYY-MM-DD'.
    """
    val = str(value).strip()
    if not val:
        return value

    # ── Caso 1: valor contiene solo hora ──────────────────────────────────
    if _TIME_ONLY_RE.match(val):
        try:
            # Unificar separadores: "11:46.0" → "11:46:0" → partes [11, 46, 0]
            normalized = val.replace(".", ":")
            parts = normalized.split(":")
            h     = int(parts[0])
            m     = int(parts[1]) if len(parts) > 1 else 0
            s_raw = float(parts[2]) if len(parts) > 2 else 0.0
            s     = int(s_raw)
            ms    = int(round((s_raw - s) * 1000))
            dt    = datetime.strptime(base_date, "%Y-%m-%d").replace(
                hour=h, minute=m, second=s, microsecond=ms * 1000
            )
            return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{ms:03d}"
        except Exception:
            return value   # si algo falla, devolver sin cambios

    # ── Caso 2: datetime completo → reformatear ───────────────────────────
    for fmt in _DT_FORMATS:
        try:
            dt = datetime.strptime(val, fmt)
            ms = dt.microsecond // 1000
            return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{ms:03d}"
        except ValueError:
            continue   # probar el siguiente formato

    # ── Caso 3: intentar con pandas como último recurso ───────────────────
    try:
        dt = pd.to_datetime(val, dayfirst=False)
        ms = dt.microsecond // 1000
        return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{ms:03d}"
    except Exception:
        return value   # no se pudo parsear → sin cambios


def apply_date_transforms(chunk: pd.DataFrame, date_transforms: dict[str, str]) -> pd.DataFrame:
    """
    Aplica normalize_datetime_value a cada columna indicada en date_transforms.
    date_transforms = {nombre_columna: base_date_str}  (base_date_str = 'YYYY-MM-DD')
    """
    for col, base_date in date_transforms.items():
        if col in chunk.columns:
            chunk[col] = chunk[col].apply(
                lambda v, bd=base_date: normalize_datetime_value(v, bd)
            )
    return chunk


# ─── Extracción de valores únicos ────────────────────────────────────────────

def get_unique_column_values(
    filepath:  str,
    encoding:  str,
    delimiter: str,
    column:    str,
) -> list[str]:
    """
    Lee el CSV en chunks y devuelve los valores únicos no vacíos de la columna indicada,
    ordenados alfabéticamente.
    """
    seen: set[str] = set()
    chunk_iter = None
    try:
        chunk_iter = pd.read_csv(
            filepath,
            sep=delimiter,
            encoding=encoding,
            chunksize=25_000,
            usecols=[column],      # leer solo la columna necesaria (más eficiente)
            dtype=str,
            on_bad_lines="skip",
            engine="python",
        )
        for chunk in chunk_iter:
            for val in chunk[column].fillna(""):
                v = str(val).strip()
                if v:
                    seen.add(v)
    finally:
        if chunk_iter is not None:
            chunk_iter.close()
    return sorted(seen)


def get_unique_values_by_group(
    filepath:     str,
    encoding:     str,
    delimiter:    str,
    value_column: str,
    group_column: str,
) -> dict[str, list[str]]:
    """
    Lee el CSV en chunks y devuelve los valores únicos de value_column agrupados
    por cada valor único de group_column.
    Devuelve {group_value: [unique_values ordenados]}.
    """
    groups: dict[str, set[str]] = {}
    chunk_iter = None
    try:
        chunk_iter = pd.read_csv(
            filepath,
            sep=delimiter,
            encoding=encoding,
            chunksize=25_000,
            usecols=[group_column, value_column],   # solo las dos columnas necesarias
            dtype=str,
            on_bad_lines="skip",
            engine="python",
        )
        for chunk in chunk_iter:
            chunk = chunk.fillna("")
            for g, v in zip(chunk[group_column], chunk[value_column]):
                g = str(g).strip()
                v = str(v).strip()
                if g and v:
                    groups.setdefault(g, set()).add(v)
    finally:
        if chunk_iter is not None:
            chunk_iter.close()
    # Devolver con valores ordenados dentro de cada grupo
    return {g: sorted(vals) for g, vals in sorted(groups.items())}


# ─── Procesamiento principal ──────────────────────────────────────────────────

def process_csv(
    filepath:          str,
    encoding:          str,
    delimiter:         str,
    selected_columns:  list[str],
    filters:           dict[str, str],
    output_dir:        str,
    out_delimiter:     str,
    rename_map:        Optional[dict[str, str]] = None,
    date_transforms:   Optional[dict[str, str]] = None,
    progress_callback: Optional[Callable[[float, str], None]] = None,
) -> dict:
    """
    Lee el CSV en chunks, aplica filtros/transformaciones y genera un CSV de salida
    por cada valor único de la PRIMERA columna del archivo original.

    Parámetros
    ----------
    filepath          : ruta al CSV de entrada
    encoding          : codificación detectada del archivo
    delimiter         : delimitador de entrada
    selected_columns  : columnas a incluir en la salida
    filters           : {columna: valor}  (contiene, case-insensitive, sin regex)
    output_dir        : carpeta donde se guardan los CSVs generados
    out_delimiter     : delimitador de salida (',', ';', '\\t')
    rename_map        : {col_real: nombre_preset} para renombrar encabezados en la salida
    date_transforms   : {col: base_date} para normalizar columnas de fecha/hora
    progress_callback : función (0.0–1.0, mensaje) para actualizar la barra de progreso
    """

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Leer el nombre de la primera columna para usarla como criterio de split
    with open(filepath, "r", encoding=encoding, errors="replace") as f:
        reader = csv.reader(f, delimiter=delimiter)
        original_header = next(reader, [])
    if not original_header:
        raise ValueError("El archivo CSV no tiene cabecera o está vacío.")
    split_col = original_header[0].strip().strip('"')

    # Estimar total de filas para la barra de progreso
    if progress_callback:
        progress_callback(0.01, "Contando filas del archivo...")
    total_rows_est = count_rows_fast(filepath)

    # Siempre leer split_col aunque el usuario no la haya seleccionado
    cols_for_split = list(dict.fromkeys([split_col] + selected_columns))
    write_cols     = selected_columns   # columnas que van al CSV de salida

    file_handles: dict[str, object]     = {}
    csv_writers:  dict[str, tuple]      = {}
    files_created: list[str]            = []
    total_written = 0
    rows_read     = 0
    chunksize     = 25_000
    chunk_iter    = None   # referencia explícita para cerrar en el finally

    try:
        chunk_iter = pd.read_csv(
            filepath,
            sep=delimiter,
            encoding=encoding,
            chunksize=chunksize,
            on_bad_lines="skip",
            dtype=str,
            engine="python",
        )

        for chunk in chunk_iter:
            chunk = chunk.fillna("")

            # Normalizar columnas de fecha antes de aplicar filtros
            if date_transforms:
                chunk = apply_date_transforms(chunk, date_transforms)

            # Verificar que las columnas necesarias estén presentes en este chunk
            available_write = [c for c in write_cols     if c in chunk.columns]
            if split_col not in chunk.columns or not available_write:
                rows_read += chunksize
                continue

            # Aplicar filtros: cada condición es un "contiene" case-insensitive
            for col, val in filters.items():
                if col in chunk.columns and val:
                    mask  = chunk[col].str.contains(val, case=False, na=False, regex=False)
                    chunk = chunk[mask]

            if chunk.empty:
                rows_read += chunksize
                if progress_callback and total_rows_est > 0:
                    pct = min(0.99, rows_read / total_rows_est)
                    progress_callback(pct, f"Procesando... {rows_read:,} / ~{total_rows_est:,} filas")
                continue

            # Agrupar por la primera columna y escribir un CSV por grupo
            for group_val, group_df in chunk.groupby(split_col, sort=False):
                safe_name = _sanitize_filename(group_val)
                out_file  = str(output_path / f"{safe_name}.csv")

                if out_file not in file_handles:
                    # Primera vez que aparece este grupo: abrir archivo y escribir encabezado
                    fh     = open(out_file, "w", newline="", encoding="utf-8-sig")
                    writer = csv.writer(fh, delimiter=out_delimiter)

                    header_to_write = [c for c in available_write if c in group_df.columns]
                    # Aplicar renombrado de encabezados si hay mapa definido
                    header_output = [
                        rename_map[c] if rename_map and c in rename_map else c
                        for c in header_to_write
                    ]
                    writer.writerow(header_output)

                    file_handles[out_file] = fh
                    csv_writers[out_file]  = (writer, header_to_write)
                    files_created.append(out_file)

                writer, header_to_write = csv_writers[out_file]
                sub = group_df[header_to_write]
                for row in sub.itertuples(index=False, name=None):
                    writer.writerow(row)

                total_written += len(group_df)

            rows_read += chunksize
            if progress_callback and total_rows_est > 0:
                pct = min(0.99, rows_read / total_rows_est)
                progress_callback(pct, f"Procesando... {rows_read:,} / ~{total_rows_est:,} filas")

    finally:
        # Cerrar el lector del CSV de entrada (evita que el archivo quede abierto ante excepciones)
        if chunk_iter is not None:
            chunk_iter.close()
        # Cerrar todos los archivos de salida
        for fh in file_handles.values():
            fh.close()

    if progress_callback:
        progress_callback(1.0, "Completado")

    return {
        "files_created": files_created,
        "total_rows":    total_written,
        "split_column":  split_col,
    }
