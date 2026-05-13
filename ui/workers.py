"""
ui/workers.py — Workers QThread para operaciones asíncronas de procesamiento CSV.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from PyQt6.QtCore import QThread, pyqtSignal

from processor import (
    detect_encoding, detect_delimiter, get_columns, get_preview,
    process_csv, search_values_in_csv,
)
from constants import PREVIEW_ROWS, SEARCH_FILE_PALETTE


class FileLoaderWorker(QThread):
    done  = pyqtSignal(str, str, str, list, object)  # path, enc, delim, cols, df
    error = pyqtSignal(str)

    def __init__(self, path: str):
        super().__init__()
        self._path = path

    def run(self):
        try:
            enc   = detect_encoding(self._path)
            delim = detect_delimiter(self._path, enc)
            cols  = get_columns(self._path, enc, delim)
            df    = get_preview(self._path, enc, delim, PREVIEW_ROWS)
            self.done.emit(self._path, enc, delim, cols, df)
        except Exception as e:
            self.error.emit(str(e))


class CSVExportWorker(QThread):
    progress = pyqtSignal(int, str)
    done     = pyqtSignal(str)
    error    = pyqtSignal(str)

    def __init__(self, filepath, enc, delim, columns, filters, out_dir, out_delim, rename_map):
        super().__init__()
        self._filepath   = filepath
        self._enc        = enc
        self._delim      = delim
        self._columns    = columns
        self._filters    = filters
        self._out_dir    = out_dir
        self._out_delim  = out_delim
        self._rename_map = rename_map

    def run(self):
        def cb(pct, msg): self.progress.emit(int(pct * 100), msg)
        try:
            process_csv(
                filepath          = self._filepath,
                encoding          = self._enc,
                delimiter         = self._delim,
                selected_columns  = self._columns,
                filters           = self._filters,
                output_dir        = self._out_dir,
                out_delimiter     = self._out_delim,
                rename_map        = self._rename_map,
                progress_callback = cb,
            )
            self.done.emit(self._out_dir)
        except Exception as e:
            self.error.emit(str(e))


class SearchWorker(QThread):
    progress  = pyqtSignal(int, str)
    done      = pyqtSignal(list)
    error     = pyqtSignal(str)
    warning   = pyqtSignal(str)   # emitido por cada archivo que falla silenciosamente
    cancelled = pyqtSignal()

    def __init__(self, files: list, values: list, col_candidates: list,
                 preset_canonical: list, extra_maps: dict = None):
        super().__init__()
        self._files            = files
        self._values           = values
        self._col_candidates   = col_candidates
        self._preset_canonical = preset_canonical
        self._extra_maps       = extra_maps or {}
        self._cancel           = False

    def cancel(self): self._cancel = True

    def run(self):
        results = []
        total   = len(self._files)
        try:
            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = {
                    pool.submit(self._search_one, fp, i): fp
                    for i, fp in enumerate(self._files)
                }
                for idx, fut in enumerate(as_completed(futures)):
                    if self._cancel:
                        pool.shutdown(wait=False, cancel_futures=True)
                        self.cancelled.emit()
                        return
                    rows, fp, err = fut.result()
                    if err:
                        self.warning.emit(err)
                    results.extend(rows)
                    pct = int((idx + 1) / total * 100)
                    self.progress.emit(pct, f"Buscando en {Path(fp).name}…")
            self.done.emit(results)
        except Exception as e:
            self.error.emit(str(e))

    def _search_one(self, filepath: str, file_idx: int) -> tuple[list, str, str]:
        try:
            enc      = detect_encoding(filepath)
            delim    = detect_delimiter(filepath, enc)
            cols     = get_columns(filepath, enc, delim)
            cols_set = set(cols)

            extra = self._extra_maps.get(filepath, {})

            col = next((c for c in self._col_candidates if c in cols_set), None)
            if col is None:
                canonical_target = self._col_candidates[0]
                col = next(
                    (actual for actual, canon in extra.items() if canon == canonical_target),
                    None
                )
            if col is None:
                return [], filepath, ""

            rows = search_values_in_csv(filepath, enc, delim, col, self._values)
            if not rows:
                return [], filepath, ""

            rename = {}
            for canonical, candidates in self._preset_canonical:
                for c in candidates:
                    if c in cols_set and c != canonical:
                        rename[c] = canonical
                        break
            rename.update(extra)

            palette_idx = file_idx % len(SEARCH_FILE_PALETTE)
            basename    = Path(filepath).name
            for row in rows:
                if rename:
                    for actual, canon in list(rename.items()):
                        if actual in row:
                            row[canon] = row.pop(actual)
                row["__file__"]     = basename
                row["__filepath__"] = filepath
                row["__palette__"]  = palette_idx
            return rows, filepath, ""
        except Exception as e:
            return [], filepath, f"⚠ Error en {Path(filepath).name}: {e}"


class GenericWorker(QThread):
    """Worker genérico para TXT, JSON, AddColumn y exportaciones de búsqueda."""
    progress  = pyqtSignal(int, str)
    done      = pyqtSignal(str)
    error     = pyqtSignal(str)
    cancelled = pyqtSignal()

    def __init__(self, fn, *args):
        super().__init__()
        self._fn     = fn
        self._args   = args
        self._cancel = False

    def cancel(self): self._cancel = True

    def run(self):
        def cb(pct, msg): self.progress.emit(pct, msg)
        try:
            result = self._fn(*self._args, lambda: self._cancel, cb)
            if self._cancel:
                self.cancelled.emit()
            else:
                self.done.emit(result or "")
        except Exception as e:
            self.error.emit(str(e))
