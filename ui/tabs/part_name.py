"""
ui/tabs/part_name.py — Tab de análisis de part names por CLASSCODE.
"""

import re

import pandas as pd
from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QComboBox, QScrollArea, QFrame, QSizePolicy,
)

from ui.widgets import DataTable, PandasTableModel, hline


class PartNameTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._df: pd.DataFrame | None = None
        self._palette: dict = {}
        self._current_regex: str = ""
        self._last_render = None

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        tab_scroll = QScrollArea()
        tab_scroll.setWidgetResizable(True)
        tab_scroll.setFrameShape(QFrame.Shape.NoFrame)
        outer.addWidget(tab_scroll)

        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(10, 8, 10, 8)
        layout.setSpacing(6)
        tab_scroll.setWidget(container)

        row1 = QHBoxLayout()
        row1.addWidget(QLabel("CLASSCODE:"))
        self.combo_classcode = QComboBox()
        self.combo_classcode.setMinimumWidth(200)
        self.btn_analyze = QPushButton("🔍  Analizar")
        self.btn_analyze.setObjectName("accent")
        self.btn_analyze.setFixedHeight(32)
        self.btn_analyze.clicked.connect(self._analyze)
        row1.addWidget(self.combo_classcode)
        row1.addWidget(self.btn_analyze)
        row1.addStretch()
        layout.addLayout(row1)

        self.lbl_phonemodel_warning = QLabel("")
        self.lbl_phonemodel_warning.setFont(QFont("Segoe UI", 10))
        self.lbl_phonemodel_warning.setWordWrap(True)
        self.lbl_phonemodel_warning.setMinimumHeight(30)
        self.lbl_phonemodel_warning.setVisible(False)
        layout.addWidget(self.lbl_phonemodel_warning)

        layout.addWidget(hline())

        hdr_row = QHBoxLayout()
        self.lbl_regex = QLabel("RegEx para CLASSCODE: (Seleccioná un CLASSCODE y presioná 'Analizar')")
        self.lbl_regex.setFont(QFont("Consolas", 10))
        self.lbl_regex.setWordWrap(True)
        self.lbl_regex.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self.btn_copy_regex = QPushButton("📋  Copiar")
        self.btn_copy_regex.setFixedHeight(32)
        self.btn_copy_regex.setFixedWidth(90)
        self.btn_copy_regex.clicked.connect(self._copy_regex)
        self.btn_copy_regex.setEnabled(False)
        hdr_row.addWidget(self.lbl_regex, stretch=1)
        hdr_row.addWidget(self.btn_copy_regex)
        layout.addLayout(hdr_row)

        layout.addWidget(hline())

        layout.addWidget(QLabel("📋  KEYUNITBARCODE del CLASSCODE seleccionado:"))
        self.table_keyunit = DataTable()
        self.table_keyunit.setMinimumHeight(200)
        layout.addWidget(self.table_keyunit)

        layout.addWidget(hline())

        layout.addWidget(QLabel("📊  KEYMATERIAL (agrupados):"))
        self._mat_widget = QWidget()
        self._mat_layout = QVBoxLayout(self._mat_widget)
        self._mat_layout.setContentsMargins(4, 4, 4, 4)
        self._mat_layout.setSpacing(4)
        self._mat_layout.addStretch()
        layout.addWidget(self._mat_widget)

        layout.addStretch()

    def setup(self, df: pd.DataFrame, columns: list, palette: dict,
              filepath: str = "", enc: str = "", delim: str = ""):
        self._df       = df
        self._palette  = palette
        self._filepath = filepath
        self._enc      = enc
        self._delim    = delim

        self.lbl_regex.setStyleSheet(
            f"padding: 6px; background: {palette['surface2']}; "
            f"border-radius: 4px; color: {palette['text_muted']};"
        )

        if "CLASSCODE" in columns:
            codes = (
                sorted(df["CLASSCODE"].dropna().unique().tolist())
                if self._df is not None and "CLASSCODE" in df.columns
                else []
            )
            self.combo_classcode.clear()
            self.combo_classcode.addItems(codes)

    def _analyze(self):
        if self._df is None:
            return
        code = self.combo_classcode.currentText()
        if not code:
            return

        col_map         = {c.strip(): c for c in self._df.columns}
        classcode_col   = col_map.get("CLASSCODE")
        phonemodel_col  = col_map.get("PHONEMODEL_NAME")
        keyunit_col     = col_map.get("KEYUNITBARCODE")
        keymaterial_col = col_map.get("KEYMATERIAL")

        subset = self._df[self._df[classcode_col] == code] if classcode_col else self._df

        show_cols = [c for c in [phonemodel_col, keyunit_col, keymaterial_col] if c]
        if show_cols:
            model = PandasTableModel(subset[show_cols].copy(), self._palette)
            self.table_keyunit.set_model(model)
            self.table_keyunit.apply_palette(self._palette)

        self.lbl_phonemodel_warning.setVisible(False)
        self.btn_copy_regex.setEnabled(False)
        self._current_regex = ""
        p = self._palette

        if phonemodel_col:
            if self._filepath:
                import csv as _csv
                phonemodels_set: set = set()
                try:
                    with open(self._filepath, encoding=self._enc, newline="") as f:
                        reader = _csv.DictReader(f, delimiter=self._delim)
                        for row in reader:
                            cc = row.get(classcode_col or "CLASSCODE", "").strip()
                            pm = row.get(phonemodel_col, "").strip()
                            if cc == code and pm:
                                phonemodels_set.add(pm)
                                if len(phonemodels_set) > 1:
                                    break
                except Exception:
                    pass
                phonemodels = list(phonemodels_set)
            else:
                phonemodels = subset[phonemodel_col].dropna().unique().tolist()

            if len(phonemodels) > 1:
                models_str = ", ".join(str(m) for m in phonemodels)
                self.lbl_phonemodel_warning.setText(
                    f"⚠  CLASSCODE {code} tiene {len(phonemodels)} PHONEMODEL_NAME distintos: {models_str}\n"
                    f"No se puede generar RegEx con modelos mezclados."
                )
                self.lbl_phonemodel_warning.setStyleSheet(
                    f"color: {p['error']}; background: {p['surface2']}; "
                    f"padding: 8px; border-radius: 4px; font-weight: 600;"
                )
                self.lbl_phonemodel_warning.setVisible(True)
                self.lbl_regex.setText(f"RegEx para {code}: cancelado — modelos mezclados")
                self.lbl_regex.setStyleSheet(
                    f"padding: 6px; background: {p['surface2']}; "
                    f"border-radius: 4px; color: {p['error']};"
                )
                self._last_render = (subset, keymaterial_col, code)
                self._render_keymaterial(subset, keymaterial_col, code)
                return

            elif len(phonemodels) == 1:
                self.lbl_phonemodel_warning.setText(f"✓  PHONEMODEL_NAME: {phonemodels[0]}")
                self.lbl_phonemodel_warning.setStyleSheet(
                    f"color: {p['success']}; background: {p['surface2']}; "
                    f"padding: 6px; border-radius: 4px;"
                )
                self.lbl_phonemodel_warning.setVisible(True)

        if keyunit_col:
            regex_values = subset[keyunit_col].dropna().unique().tolist()
            if regex_values:
                pattern = self._generate_regex(regex_values)
                self.lbl_regex.setText(f"RegEx para {code}: {pattern}")
                self.lbl_regex.setStyleSheet(
                    f"padding: 6px; background: {p['surface2']}; "
                    f"border-radius: 4px; color: {p['text']};"
                )
                self.btn_copy_regex.setEnabled(True)
                self._current_regex = pattern
            else:
                self.lbl_regex.setText(f"RegEx para {code}: sin KEYUNITBARCODE")
                self.lbl_regex.setStyleSheet(
                    f"padding: 6px; background: {p['surface2']}; "
                    f"border-radius: 4px; color: {p['warning']};"
                )
        else:
            self.lbl_regex.setText(f"RegEx para {code}: columna KEYUNITBARCODE no encontrada")
            self.lbl_regex.setStyleSheet(
                f"padding: 6px; background: {p['surface2']}; "
                f"border-radius: 4px; color: {p['error']};"
            )

        self._last_render = (subset, keymaterial_col, code)
        self._render_keymaterial(subset, keymaterial_col, code)

    def _render_keymaterial(self, subset: pd.DataFrame, keymaterial_col: str | None, code: str):
        while self._mat_layout.count() > 1:
            item = self._mat_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        if not keymaterial_col:
            return
        p          = self._palette
        counts     = subset[keymaterial_col].value_counts().head(50)
        font_row   = QFont("Segoe UI", 10)
        font_total = QFont("Segoe UI", 11)
        font_total.setBold(True)
        for mat, cnt in counts.items():
            row_w = QWidget()
            row_h = QHBoxLayout(row_w)
            row_h.setContentsMargins(4, 1, 4, 1)
            row_h.setSpacing(8)
            lbl_pn  = QLabel("PN:")
            lbl_pn.setFont(font_row)
            lbl_pn.setStyleSheet(f"color: {p['text_muted']}; font-weight: 600;")
            lbl_mat = QLabel(str(mat))
            lbl_mat.setFont(font_row)
            lbl_mat.setStyleSheet(f"color: {p['text']}; font-weight: 500;")
            lbl_mat.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
            lbl_cc  = QLabel(f"{code}:")
            lbl_cc.setFont(font_row)
            lbl_cc.setStyleSheet(f"color: {p['accent']}; font-weight: 700;")
            lbl_cnt = QLabel(f"[{cnt}]")
            lbl_cnt.setFont(font_row)
            lbl_cnt.setStyleSheet(f"color: {p['accent']}; font-weight: 600;")
            lbl_cnt.setFixedWidth(55)
            for w in (lbl_pn, lbl_mat, lbl_cc, lbl_cnt):
                row_h.addWidget(w)
            self._mat_layout.insertWidget(self._mat_layout.count() - 1, row_w)

        total_w = QWidget()
        total_h = QHBoxLayout(total_w)
        total_h.setContentsMargins(4, 6, 4, 2)
        total_h.setSpacing(6)
        lbl_total = QLabel(f"{code}  —  {len(subset)} registros")
        lbl_total.setFont(font_total)
        lbl_total.setStyleSheet(f"color: {p['text']}; font-weight: 700;")
        total_h.addWidget(lbl_total)
        total_h.addStretch()
        self._mat_layout.insertWidget(self._mat_layout.count() - 1, total_w)

    def apply_palette(self, p: dict):
        self._palette = p
        self.table_keyunit.apply_palette(p)
        if self._last_render is not None:
            subset, keymaterial_col, code = self._last_render
            self._render_keymaterial(subset, keymaterial_col, code)

    def _generate_regex(self, values: list) -> str:
        if not values:
            return ""

        if len(values) == 1:
            return f"^{re.escape(values[0])}$"

        def _split_at_percents(val):
            p1 = val.find('%')
            if p1 == -1: return None
            p2 = val.find('%', p1 + 1)
            if p2 == -1: return None
            return val[:p1], val[p1 + 1:p2], val[p2 + 1:]

        pct_parts = [_split_at_percents(v) for v in values]
        if all(p is not None for p in pct_parts):
            before_set = {p[0] for p in pct_parts}
            if len(before_set) == 1:
                before   = pct_parts[0][0]
                middles  = [p[1] for p in pct_parts]
                suffixes = [p[2] for p in pct_parts]

                mid_prefix = middles[0]
                for m in middles[1:]:
                    while not m.startswith(mid_prefix) and mid_prefix:
                        mid_prefix = mid_prefix[:-1]

                mid_variants = sorted({m[len(mid_prefix):] for m in middles})

                if len(mid_variants) == 1 and mid_variants[0] == "":
                    mid_regex = re.escape(mid_prefix)
                elif len(mid_variants) <= 8 and all(len(v) <= 6 for v in mid_variants):
                    mid_regex = (re.escape(mid_prefix) +
                                 "(" + "|".join(re.escape(v) for v in mid_variants) + ")")
                else:
                    mid_len   = len(middles[0]) - len(mid_prefix)
                    mid_regex = re.escape(mid_prefix) + f"[-A-Z0-9]{{{mid_len}}}"

                suf_lens    = [len(s) for s in suffixes]
                mn, mx      = min(suf_lens), max(suf_lens)
                suffix_part = f"{{{mn}}}" if mn == mx else f"{{{mn},{mx}}}"
                return f"^{re.escape(before)}%{mid_regex}%[-A-Z0-9]{suffix_part}$"

        prefix = values[0]
        for val in values[1:]:
            while not val.startswith(prefix) and prefix:
                prefix = prefix[:-1]

        if not prefix or len(prefix) < 3:
            if len(values) <= 5:
                return f"^({'|'.join(re.escape(v) for v in values)})$"
            return f"^({'|'.join(re.escape(v) for v in values[:5])}|...)$"

        def truncate_at_last_delimiter(text):
            percent_positions = [i for i, char in enumerate(text) if char == '%']
            if len(percent_positions) >= 2:
                return text[:percent_positions[1] + 1]
            elif len(percent_positions) == 1:
                return text[:percent_positions[0] + 1]
            for i in range(len(text) - 1, -1, -1):
                if not text[i].isalnum():
                    return text[:i + 1]
            return text

        suffixes       = [v[len(prefix):] for v in values]
        suffix_lengths = [len(s) for s in suffixes]

        if len(set(suffix_lengths)) == 1:
            common_length   = suffix_lengths[0]
            variation_start = None
            for i in range(min(len(s) for s in suffixes)):
                chars_at_i = {s[i] if i < len(s) else '' for s in suffixes}
                if len(chars_at_i) > 1:
                    variation_start = i
                    break

            if variation_start is not None:
                abs_variation_pos = len(prefix) + variation_start
                total_length      = len(values[0])
                if abs_variation_pos > total_length * 0.4:
                    truncated_prefix  = truncate_at_last_delimiter(prefix)
                    new_suffix_length = common_length + (len(prefix) - len(truncated_prefix))
                    return f"^{re.escape(truncated_prefix)}[-A-Z0-9]{{{new_suffix_length}}}$"

                before_var    = suffixes[0][:variation_start] if variation_start > 0 else ""
                variation_end = variation_start + 1
                for i in range(variation_start + 1, common_length):
                    chars_at_i = {s[i] if i < len(s) else '' for s in suffixes}
                    if len(chars_at_i) > 1:
                        variation_end = i + 1
                    else:
                        break

                variations = sorted({s[variation_start:variation_end] for s in suffixes})

                if 2 <= len(variations) <= 5 and all(v for v in variations):
                    common_after = ""
                    if variation_end < common_length:
                        chars_at_i = {s[variation_end] if variation_end < len(s) else '' for s in suffixes}
                        if len(chars_at_i) == 1:
                            char = suffixes[0][variation_end]
                            if not char.isalnum():
                                common_after = char

                    if not common_after:
                        truncated_prefix  = truncate_at_last_delimiter(prefix)
                        new_suffix_length = common_length + (len(prefix) - len(truncated_prefix))
                        return f"^{re.escape(truncated_prefix)}[-A-Z0-9]{{{new_suffix_length}}}$"

                    common_after_len = len(common_after)
                    remaining_chars  = common_length - variation_end - common_after_len

                    pattern_parts = [f"^{re.escape(prefix)}"]
                    if before_var:
                        pattern_parts.append(re.escape(before_var))
                    pattern_parts.append(f"({'|'.join(re.escape(v) for v in variations)})")
                    if common_after:
                        pattern_parts.append(re.escape(common_after))
                    if remaining_chars > 0:
                        pattern_parts.append(f"[-A-Z0-9]{{{remaining_chars}}}")
                    pattern_parts.append("$")
                    return "".join(pattern_parts)

            truncated_prefix  = truncate_at_last_delimiter(prefix)
            new_suffix_length = common_length + (len(prefix) - len(truncated_prefix))
            return f"^{re.escape(truncated_prefix)}[-A-Z0-9]{{{new_suffix_length}}}$"

        truncated_prefix = truncate_at_last_delimiter(prefix)
        min_len = min(suffix_lengths) + (len(prefix) - len(truncated_prefix))
        max_len = max(suffix_lengths) + (len(prefix) - len(truncated_prefix))
        if min_len == max_len:
            return f"^{re.escape(truncated_prefix)}[-A-Z0-9]{{{min_len}}}$"
        return f"^{re.escape(truncated_prefix)}[-A-Z0-9]{{{min_len},{max_len}}}$"

    def _copy_regex(self):
        if not self._current_regex:
            return
        from PyQt6.QtWidgets import QApplication
        QApplication.clipboard().setText(self._current_regex)
        original_text = self.btn_copy_regex.text()
        self.btn_copy_regex.setText("✓ Copiado")
        QTimer.singleShot(1500, lambda: self.btn_copy_regex.setText(original_text))
