# PRD — CSV Processor

## ¿Qué es?

CSV Processor es una aplicación de escritorio para Windows que permite procesar, filtrar, exportar y buscar datos en archivos CSV de gran tamaño (400 MB o más), sin necesidad de abrir Excel ni escribir código.

---

## ¿Para quién es?

Operadores y analistas que trabajan con archivos CSV de datos de dispositivos móviles (números de serie, modelos, códigos de parte) y necesitan transformar esos archivos diariamente sin depender de herramientas técnicas.

---

## Funcionalidades

### 1. Exportar CSV
Carga un archivo CSV y genera un CSV separado por cada modelo de teléfono (PHONEMODEL_NAME) que contenga el archivo.

**El usuario puede:**
- Ver una preview de los datos antes de exportar
- Elegir qué columnas incluir en el resultado (panel izquierdo)
- Renombrar columnas en la salida si el archivo usa nombres distintos a los estándar
- Elegir el delimitador de salida: coma, punto y coma o tabulación
- Elegir la carpeta de destino
- Cancelar el proceso en cualquier momento

**Resultado:** una carpeta `csv_output/` con un archivo por modelo. Si un modelo supera 500.000 filas, se parte en `_part2`, `_part3`, etc. El nombre de cada archivo incluye el nombre del archivo fuente.

---

### 2. Exportar SN a TXT
Toma una columna del CSV (típicamente el número de serie) y genera un archivo `.txt` con todos los valores únicos.

**El usuario puede:**
- Elegir la columna a exportar
- Elegir el formato: valor solo (`ZY32MJ3LZH`) o formato SQL (`'ZY32MJ3LZH',`)
- Agrupar por modelo: genera un `.txt` por cada valor distinto de la columna de agrupación
- Ver una preview de cómo quedará el archivo antes de exportar
- Cancelar el proceso

**Resultado:** carpeta `txt_output/` con uno o varios archivos `.txt`.

---

### 3. Exportar JSON
Organiza los datos del CSV en una estructura de carpetas y archivos JSON.

**El usuario puede:**
- Elegir por qué columna se crean las **carpetas**
- Elegir por qué columna se crean los **archivos JSON** dentro de cada carpeta
- Ver una preview de la estructura antes de exportar
- Cancelar el proceso

**Resultado:** carpeta `json_output/` con subcarpetas y archivos `.json`, cada uno con un array de registros.

---

### 4. Buscar
Busca uno o varios valores (números de serie, códigos) en múltiples archivos CSV al mismo tiempo.

**El usuario puede:**
- Agregar varios archivos CSV al panel izquierdo
- Pegar los códigos a buscar (separados por coma o enter)
- Elegir en qué columna buscar (con soporte de mapeo si los archivos usan nombres distintos)
- Ver qué códigos **no se encontraron** en ningún archivo
- Filtrar las columnas que aparecen en los resultados
- Exportar los resultados a JSON o CSV
- Cancelar la búsqueda

**Resultado:** tabla con todas las filas encontradas, coloreadas por archivo de origen.

---

### 5. Agregar Columna
Agrega una columna con un valor fijo a un CSV existente.

**El usuario puede:**
- Cargar su propio archivo CSV (independiente del resto de la app)
- Definir el nombre y valor de la nueva columna
- Elegir la posición: al inicio, al final o después de una columna específica
- Ver una preview con la columna agregada antes de procesar

**Validación automática:** antes de procesar, verifica que todos los registros del archivo pertenezcan al mismo modelo. Si detecta modelos diferentes, alerta al usuario y no deja continuar.

**Resultado:** carpeta `CSV_con_columna_agregada/` con el archivo procesado.

---

### 6. Part Name
Analiza los part names presentes en el archivo por código de clase (CLASSCODE).

**El usuario puede:**
- Seleccionar un CLASSCODE y ver todos los part names asociados
- Ver cuántos registros tiene cada part name
- Identificar patrones y variaciones en los nombres

---

## Características generales

- **Archivos grandes:** optimizado para CSVs de más de 40 millones de filas sin cargar todo en memoria
- **Detección automática:** detecta codificación y delimitador del archivo al abrirlo
- **Mapeo de columnas:** si el archivo usa nombres distintos a los estándar (ej: `STR_PSN_1` en lugar de `SN`), el usuario puede mapearlos una vez al cargar
- **Tema claro/oscuro:** botón en la barra superior para cambiar el tema
- **Cancelación:** todos los procesos largos se pueden cancelar en cualquier momento
- **Barra de progreso:** muestra el avance en tiempo real con mensaje de estado

---

## Lo que NO hace (fuera de alcance)

- No edita celdas individuales
- No genera gráficos ni reportes visuales
- No sube archivos a ningún servidor
- No trabaja con formatos distintos a CSV en la entrada
