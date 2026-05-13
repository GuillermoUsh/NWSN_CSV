"""
Tests unitarios para processor.py — funciones puras y procesamiento de archivos.
"""

import csv
import textwrap
from pathlib import Path

import pytest

from processor import (
    sanitize_filename,
    normalize_datetime_value,
    detect_encoding,
    detect_delimiter,
    get_columns,
    get_preview,
    count_rows_fast,
    search_values_in_csv,
)


# ── sanitize_filename ─────────────────────────────────────────────────────────

class TestSanitizeFilename:
    def test_caracteres_prohibidos_windows(self):
        assert "\\" not in sanitize_filename("a\\b")
        assert "/" not in sanitize_filename("a/b")
        assert ":" not in sanitize_filename("a:b")
        assert "*" not in sanitize_filename("a*b")
        assert "?" not in sanitize_filename("a?b")
        assert '"' not in sanitize_filename('a"b')
        assert "<" not in sanitize_filename("a<b")
        assert ">" not in sanitize_filename("a>b")
        assert "|" not in sanitize_filename("a|b")

    def test_path_traversal_bloqueado(self):
        resultado = sanitize_filename("../../etc/passwd")
        assert ".." not in resultado

    def test_valor_vacio(self):
        assert sanitize_filename("") == "SIN_VALOR"
        assert sanitize_filename("   ") == "SIN_VALOR"

    def test_limite_longitud(self):
        largo = "A" * 200
        assert len(sanitize_filename(largo)) <= 100

    def test_valor_normal_sin_cambios(self):
        assert sanitize_filename("MODELO_XYZ_123") == "MODELO_XYZ_123"

    def test_conserva_guiones_y_puntos_simples(self):
        resultado = sanitize_filename("archivo.v2-final")
        assert "-" in resultado
        assert "." in resultado


# ── normalize_datetime_value ──────────────────────────────────────────────────

class TestNormalizeDatetimeValue:
    BASE = "2024-01-15"

    def test_solo_hora_hhmm(self):
        result = normalize_datetime_value("14:30", self.BASE)
        assert result == "2024-01-15 14:30:00.000"

    def test_solo_hora_hhmmss(self):
        result = normalize_datetime_value("09:05:42", self.BASE)
        assert result == "2024-01-15 09:05:42.000"

    def test_formato_iso(self):
        result = normalize_datetime_value("2024-03-20 11:22:33", self.BASE)
        assert result == "2024-03-20 11:22:33.000"

    def test_formato_con_microsegundos(self):
        result = normalize_datetime_value("2024-03-20 11:22:33.123456", self.BASE)
        assert result.startswith("2024-03-20 11:22:33.")

    def test_formato_dmy(self):
        result = normalize_datetime_value("25/12/2023 08:00:00", self.BASE)
        assert "2023-12-25" in result

    def test_valor_no_parseable_sin_cambios(self):
        original = "NO_ES_FECHA"
        result = normalize_datetime_value(original, self.BASE)
        assert result == original

    def test_valor_vacio_sin_cambios(self):
        result = normalize_datetime_value("", self.BASE)
        assert result == ""


# ── Funciones que requieren archivos CSV temporales ───────────────────────────

@pytest.fixture
def simple_csv(tmp_path: Path) -> Path:
    content = textwrap.dedent("""\
        PHONEMODEL_NAME,SN,CLASSCODE
        ModeloA,SN001,AT
        ModeloA,SN002,AT
        ModeloB,SN003,KTL
        ModeloB,SN004,KTL
    """)
    p = tmp_path / "test.csv"
    p.write_text(content, encoding="utf-8")
    return p


@pytest.fixture
def semicolon_csv(tmp_path: Path) -> Path:
    content = "COL1;COL2;COL3\nA;B;C\nD;E;F\n"
    p = tmp_path / "semicolon.csv"
    p.write_text(content, encoding="utf-8")
    return p


class TestDetectEncoding:
    def test_utf8(self, simple_csv: Path):
        enc = detect_encoding(str(simple_csv))
        assert enc.lower().replace("-", "") in ("utf8", "utf8sig", "ascii")


class TestDetectDelimiter:
    def test_coma(self, simple_csv: Path):
        enc   = detect_encoding(str(simple_csv))
        delim = detect_delimiter(str(simple_csv), enc)
        assert delim == ","

    def test_punto_y_coma(self, semicolon_csv: Path):
        enc   = detect_encoding(str(semicolon_csv))
        delim = detect_delimiter(str(semicolon_csv), enc)
        assert delim == ";"


class TestGetColumns:
    def test_columnas_correctas(self, simple_csv: Path):
        enc   = detect_encoding(str(simple_csv))
        delim = detect_delimiter(str(simple_csv), enc)
        cols  = get_columns(str(simple_csv), enc, delim)
        assert cols == ["PHONEMODEL_NAME", "SN", "CLASSCODE"]


class TestCountRowsFast:
    def test_cuenta_filas_sin_cabecera(self, simple_csv: Path):
        count = count_rows_fast(str(simple_csv))
        assert count == 4   # 4 filas de datos, 1 cabecera descontada

    def test_archivo_vacio(self, tmp_path: Path):
        p = tmp_path / "vacio.csv"
        p.write_text("COL1,COL2\n", encoding="utf-8")
        assert count_rows_fast(str(p)) == 0


class TestSearchValuesInCsv:
    def test_busqueda_exacta(self, simple_csv: Path):
        enc   = detect_encoding(str(simple_csv))
        delim = detect_delimiter(str(simple_csv), enc)
        rows  = search_values_in_csv(str(simple_csv), enc, delim, "SN", ["SN001"])
        assert len(rows) == 1
        assert rows[0]["SN"] == "SN001"

    def test_busqueda_case_insensitive(self, simple_csv: Path):
        enc   = detect_encoding(str(simple_csv))
        delim = detect_delimiter(str(simple_csv), enc)
        rows  = search_values_in_csv(str(simple_csv), enc, delim, "SN", ["sn001"])
        assert len(rows) == 1

    def test_multiples_valores(self, simple_csv: Path):
        enc   = detect_encoding(str(simple_csv))
        delim = detect_delimiter(str(simple_csv), enc)
        rows  = search_values_in_csv(str(simple_csv), enc, delim, "SN", ["SN001", "SN003"])
        assert len(rows) == 2

    def test_valor_inexistente(self, simple_csv: Path):
        enc   = detect_encoding(str(simple_csv))
        delim = detect_delimiter(str(simple_csv), enc)
        rows  = search_values_in_csv(str(simple_csv), enc, delim, "SN", ["SN_INEXISTENTE"])
        assert rows == []

    def test_columna_inexistente(self, simple_csv: Path):
        enc   = detect_encoding(str(simple_csv))
        delim = detect_delimiter(str(simple_csv), enc)
        rows  = search_values_in_csv(str(simple_csv), enc, delim, "COLUMNA_QUE_NO_EXISTE", ["SN001"])
        assert rows == []

    def test_metadatos_en_resultado(self, simple_csv: Path):
        enc   = detect_encoding(str(simple_csv))
        delim = detect_delimiter(str(simple_csv), enc)
        rows  = search_values_in_csv(str(simple_csv), enc, delim, "SN", ["SN002"])
        assert "_file" in rows[0]
        assert "_row"  in rows[0]
        assert rows[0]["_file"] == "test.csv"
