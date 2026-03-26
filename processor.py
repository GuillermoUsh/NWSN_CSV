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


def collect_rows_by_group(
    filepath:        str,
    encoding:        str,
    delimiter:       str,
    group_column:    str,
    columns_to_read: list[str],
    rename_map:      Optional[dict[str, str]] = None,
) -> dict[str, list[dict]]:
    """
    Lee el CSV en chunks y devuelve {group_value: [rows_as_dicts]}.

    Cada fila es un dict {key: value} donde key es el nombre final
    (con rename_map aplicado si corresponde).
    columns_to_read puede o no incluir group_column; la función lo garantiza.
    """
    def _key(col: str) -> str:
        if rename_map and col in rename_map:
            return rename_map[col]
        return col

    groups: dict[str, list[dict]] = {}
    chunk_iter = None
    try:
        chunk_iter = pd.read_csv(
            filepath,
            sep=delimiter,
            encoding=encoding,
            chunksize=25_000,
            dtype=str,
            on_bad_lines="skip",
            engine="python",
        )
        for chunk in chunk_iter:
            chunk = chunk.fillna("")
            if group_column not in chunk.columns:
                continue
            # Solo las columnas solicitadas que existen en este chunk
            available = [c for c in columns_to_read if c in chunk.columns]
            key_map   = {c: _key(c) for c in available}
            for seg_val, group_df in chunk.groupby(group_column, sort=False):
                seg_val = str(seg_val).strip()
                if not seg_val:
                    continue
                sub     = group_df[available].rename(columns=key_map)
                records = sub.to_dict(orient="records")
                groups.setdefault(seg_val, []).extend(records)
    finally:
        if chunk_iter is not None:
            chunk_iter.close()
    return groups


def collect_rows_by_two_groups(
    filepath:         str,
    encoding:         str,
    delimiter:        str,
    folder_column:    str,
    file_column:      str,
    columns_to_read:  list[str],
    rename_map:       Optional[dict[str, str]] = None,
) -> dict[str, dict[str, list[dict]]]:
    """
    Lee el CSV en chunks y devuelve {folder_val: {file_val: [rows_as_dicts]}}.

    Genera la jerarquía: JSON/<folder_val>/<file_val>.json
    Ambas columnas de agrupación deben existir en el CSV.
    """
    def _key(col: str) -> str:
        if rename_map and col in rename_map:
            return rename_map[col]
        return col

    result: dict[str, dict[str, list[dict]]] = {}
    chunk_iter = None
    try:
        chunk_iter = pd.read_csv(
            filepath,
            sep=delimiter,
            encoding=encoding,
            chunksize=25_000,
            dtype=str,
            on_bad_lines="skip",
            engine="python",
        )
        for chunk in chunk_iter:
            chunk = chunk.fillna("")
            if folder_column not in chunk.columns or file_column not in chunk.columns:
                continue
            available = [c for c in columns_to_read if c in chunk.columns]
            key_map   = {c: _key(c) for c in available}
            for folder_val, folder_df in chunk.groupby(folder_column, sort=False):
                folder_val = str(folder_val).strip()
                if not folder_val:
                    continue
                for file_val, file_df in folder_df.groupby(file_column, sort=False):
                    file_val = str(file_val).strip()
                    if not file_val:
                        continue
                    sub     = file_df[available].rename(columns=key_map)
                    records = sub.to_dict(orient="records")
                    result.setdefault(folder_val, {}).setdefault(file_val, []).extend(records)
    finally:
        if chunk_iter is not None:
            chunk_iter.close()
    return result


def search_value_in_csv(
    filepath:        str,
    encoding:        str,
    delimiter:       str,
    search_column:   str,
    search_value:    str,
    columns_to_read: Optional[list[str]] = None,
    cancel_fn:       Optional[Callable[[], bool]] = None,
) -> list[dict]:
    """
    Busca `search_value` (exacta, case-insensitive) en `search_column`.

    Devuelve lista de dicts con:
      '_file'  → nombre del archivo (basename)
      '_row'   → número de fila en el CSV (2-based; la fila 1 es el encabezado)
      + todas las columnas disponibles (o solo las de `columns_to_read` si se especifica)

    Si `cancel_fn` es provisto y devuelve True, la búsqueda se interrumpe al final
    del chunk actual y retorna los resultados parciales hasta ese momento.
    """
    results: list[dict] = []
    chunk_iter = None
    sv = search_value.strip().lower()
    try:
        chunk_iter = pd.read_csv(
            filepath,
            sep=delimiter,
            encoding=encoding,
            chunksize=25_000,
            dtype=str,
            on_bad_lines="skip",
            engine="python",
        )
        basename = Path(filepath).name
        for chunk in chunk_iter:
            if cancel_fn and cancel_fn():
                break
            chunk = chunk.fillna("")
            if search_column not in chunk.columns:
                continue
            mask    = chunk[search_column].str.strip().str.lower() == sv
            matched = chunk[mask]
            if columns_to_read:
                avail   = [c for c in columns_to_read if c in matched.columns]
                matched = matched[avail]
            for orig_idx, row in matched.iterrows():
                rec = {"_file": basename, "_row": orig_idx + 2}   # +2: encabezado en fila 1
                rec.update(row.to_dict())
                results.append(rec)
    finally:
        if chunk_iter is not None:
            chunk_iter.close()
    return results


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
    column_order:      Optional[list[str]] = None,
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
    column_order      : orden deseado de columnas (usando nombres finales después de rename_map)
    progress_callback : función (0.0–1.0, mensaje) para actualizar la barra de progreso
    """

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Leer el encabezado y determinar la columna de split
    with open(filepath, "r", encoding=encoding, errors="replace") as f:
        reader = csv.reader(f, delimiter=delimiter)
        original_header = next(reader, [])
    if not original_header:
        raise ValueError("El archivo CSV no tiene cabecera o está vacío.")

    # Normalizar nombres de columnas
    header_normalized = [col.strip().strip('"') for col in original_header]

    # Determinar columna de split con el siguiente orden de prioridad:
    # 1. Columna mapeada a PHONEMODEL_NAME en rename_map
    # 2. PHONEMODEL_NAME directamente
    # 3. PHONENAME (nombre real de la columna en algunos archivos)
    # 4. HONEMODEL_NAME (fallback por typo)
    # 5. Primera columna
    split_col = None

    # Verificar si hay una columna mapeada a PHONEMODEL_NAME
    if rename_map:
        for real_col, preset_col in rename_map.items():
            if preset_col == "PHONEMODEL_NAME" and real_col in header_normalized:
                split_col = real_col
                break

    # Si no se encontró en el mapeo, buscar directamente
    if split_col is None:
        for col in header_normalized:
            if col in ("PHONEMODEL_NAME", "PHONENAME", "HONEMODEL_NAME"):
                split_col = col
                break

    # Fallback a la primera columna
    if split_col is None:
        split_col = header_normalized[0]

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

                    # Reordenar columnas según column_order si está especificado
                    if column_order:
                        # Crear mapeo de nombre final -> nombre original
                        final_to_original = {
                            (rename_map[c] if rename_map and c in rename_map else c): c
                            for c in header_to_write
                        }
                        # Reordenar: primero las que están en column_order, luego el resto
                        ordered_finals = []
                        for col in column_order:
                            if col in final_to_original:
                                ordered_finals.append(col)
                        # Agregar columnas que no están en column_order al final
                        for col in header_output:
                            if col not in ordered_finals:
                                ordered_finals.append(col)

                        # Reordenar header_to_write según el nuevo orden
                        header_to_write = [final_to_original[col] for col in ordered_finals]
                        header_output = ordered_finals

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


# ─── Agregar columna con valor constante ──────────────────────────────────────

def add_column_to_csv(
    filepath:          str,
    encoding:          str,
    delimiter:         str,
    column_name:       str,
    column_value:      str,
    position:          str = "end",
    after_column:      Optional[str] = None,
    output_dir:        Optional[str] = None,
    column_order:      Optional[list[str]] = None,
    progress_callback: Optional[Callable[[float, str], None]] = None,
) -> dict:
    """
    Lee un CSV y genera una nueva versión con una columna adicional que tiene
    el mismo valor en todos los registros.

    Parámetros
    ----------
    filepath          : ruta al CSV de entrada
    encoding          : codificación del archivo
    delimiter         : delimitador del CSV
    column_name       : nombre de la nueva columna a agregar
    column_value      : valor constante para todos los registros
    position          : "start" (inicio), "end" (final), o "after" (después de after_column)
    after_column      : nombre de la columna después de la cual insertar (solo si position="after")
    output_dir        : carpeta de salida (si None, usa la carpeta del archivo original)
    column_order      : orden deseado de columnas en la salida
    progress_callback : función (0.0–1.0, mensaje) para actualizar progreso
    """

    input_path = Path(filepath)
    if output_dir:
        output_path = Path(output_dir)
    else:
        output_path = input_path.parent / "CSV_con_columna_agregada"
    output_path.mkdir(parents=True, exist_ok=True)

    output_file = output_path / input_path.name

    # Leer encabezado para determinar posición de inserción
    with open(filepath, "r", encoding=encoding, errors="replace") as f:
        reader = csv.reader(f, delimiter=delimiter)
        original_header = next(reader, [])

    if not original_header:
        raise ValueError("El archivo CSV no tiene cabecera o está vacío.")

    # Verificar si la columna ya existe
    if column_name in original_header:
        raise ValueError(f"La columna '{column_name}' ya existe en el CSV.")

    # Determinar índice de inserción
    if position == "start":
        insert_idx = 0
    elif position == "end":
        insert_idx = len(original_header)
    elif position == "after":
        if not after_column or after_column not in original_header:
            raise ValueError(f"Columna '{after_column}' no encontrada en el CSV.")
        insert_idx = original_header.index(after_column) + 1
    else:
        raise ValueError(f"Posición inválida: '{position}'. Usa 'start', 'end', o 'after'.")

    # Crear nuevo encabezado con la columna insertada
    new_header = original_header[:insert_idx] + [column_name] + original_header[insert_idx:]

    # Reordenar columnas según column_order si está especificado
    if column_order:
        ordered_header = []
        # Primero agregar columnas que están en column_order (en ese orden)
        for col in column_order:
            if col in new_header:
                ordered_header.append(col)
        # Luego agregar columnas que no están en column_order
        for col in new_header:
            if col not in ordered_header:
                ordered_header.append(col)
        new_header = ordered_header

    # Contar filas para progreso
    if progress_callback:
        progress_callback(0.01, "Contando filas...")
    total_rows = count_rows_fast(filepath)

    # Procesar CSV en chunks
    rows_processed = 0
    chunk_iter = None

    try:
        chunk_iter = pd.read_csv(
            filepath,
            sep=delimiter,
            encoding=encoding,
            chunksize=25_000,
            dtype=str,
            on_bad_lines="skip",
            engine="python",
        )

        # Abrir archivo de salida
        with open(str(output_file), "w", newline="", encoding="utf-8-sig") as out_f:
            writer = csv.writer(out_f, delimiter=delimiter)
            writer.writerow(new_header)

            for chunk in chunk_iter:
                chunk = chunk.fillna("")

                # Insertar la nueva columna con el valor constante
                chunk.insert(insert_idx, column_name, column_value)

                # Escribir filas
                for row in chunk[new_header].itertuples(index=False, name=None):
                    writer.writerow(row)

                rows_processed += len(chunk)

                if progress_callback and total_rows > 0:
                    pct = min(0.99, rows_processed / total_rows)
                    progress_callback(pct, f"Procesando... {rows_processed:,} / ~{total_rows:,} filas")

    finally:
        if chunk_iter is not None:
            chunk_iter.close()

    if progress_callback:
        progress_callback(1.0, "Completado")

    return {
        "output_file": str(output_file),
        "total_rows": rows_processed,
        "column_added": column_name,
    }
