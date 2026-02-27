# NWSN_CSV

Herramienta de escritorio para procesar archivos CSV de gran volumen (500k+ registros).

## Características

- **Detección automática** de encoding y delimitador del CSV
- **Vista previa** de las primeras 200 filas con encabezados personalizados
- **Selección de columnas** con opción de mapeo de nombres (renombrar al exportar)
- **Filtros** por valor de columna (búsqueda case-insensitive)
- **Exportación dividida**: genera un CSV separado por valor de la primera columna (ej. por modelo de equipo)
- **Exportar TXT**: extrae valores únicos de una columna hacia archivos `.txt`, con agrupación opcional
- Salida siempre en **UTF-8 con BOM** (compatible con Excel)
- Procesamiento en **chunks de 25.000 filas** para no saturar la memoria RAM

## Stack

| Componente | Tecnología |
|---|---|
| UI | CustomTkinter |
| CSV | pandas + chardet |
| Distribución | PyInstaller (.exe) |

## Archivos

```
main.py          → Punto de entrada
app.py           → Interfaz gráfica (CSVProcessorApp)
processor.py     → Lógica de procesamiento CSV
requirements.txt → Dependencias
build.bat        → Script de build con PyInstaller
```

## Instalación (desarrollo)

```bash
pip install -r requirements.txt
python main.py
```

## Build del ejecutable

```bash
build.bat
```

El `.exe` se genera en la carpeta `setup/`.
