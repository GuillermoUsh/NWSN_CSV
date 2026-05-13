"""
constants.py — Constantes de dominio compartidas entre UI y lógica de procesamiento.
"""

PRESET_COLUMNS = [
    "PHONEMODEL_NAME", "SN", "KEYUNITBARCODE",
    "CLASSCODE", "CREATETIME", "KEYMATERIAL",
]

PRESET_TO_JSON_KEY = {
    "PHONEMODEL_NAME": "phoneModelName",
    "SN":              "sn",
    "KEYUNITBARCODE":  "keyUnitBarcode",
    "CLASSCODE":       "classCode",
    "CREATETIME":      "createTime",
    "KEYMATERIAL":     "keyMaterial",
}

CLASS_CODE_MAP = {
    "AT":   "Battery_AT",        "KTL":  "Front_Housing_KTL",
    "HS":   "Rear_Camera_HS",    "KMTL": "Middle_Frame_KMTL",
    "CP":   "Charger_Port_CP",   "BTN":  "Side_Button_BTN",
    "SPK":  "Speaker_SPK",       "MIC":  "Microphone_MIC",
    "VIB":  "Vibrator_VIB",      "CAM":  "Front_Camera_CAM",
    "SCR":  "Screen_Assembly_SCR","BT":  "Bluetooth_Module_BT",
    "WIFI": "WiFi_Module_WIFI",  "NFC":  "NFC_Module_NFC",
    "FP":   "Fingerprint_FP",    "LCD":  "LCD_Panel_LCD",
    "TP":   "Touch_Panel_TP",    "PCB":  "Main_Board_PCB",
}

# Paleta de colores por archivo en el tab Buscar (light_hex, dark_hex)
SEARCH_FILE_PALETTE = [
    ("#dbeafe", "#1e3a5f"), ("#dcfce7", "#14532d"), ("#fef9c3", "#713f12"),
    ("#fce7f3", "#831843"), ("#ede9fe", "#4c1d95"), ("#ffedd5", "#7c2d12"),
    ("#cffafe", "#164e63"), ("#f1f5f9", "#334155"),
]

PREVIEW_ROWS = 2000   # filas cargadas en memoria para el preview
PAGE_SIZE    = 200    # filas visibles por página en la tabla

# Mapeo canónico: (nombre_final, [candidatos en orden de prioridad])
SEARCH_PRESET_CANONICAL: list[tuple[str, list[str]]] = [
    ("PHONEMODEL_NAME", ["PHONEMODEL_NAME", "PHONE_MODEL", "MODEL_NAME", "MODEL", "PHONEMODEL"]),
    ("SN",              ["SN", "STR_PSN_1", "SN_1", "PSN", "SERIAL"]),
    ("KEYUNITBARCODE",  ["KEYUNITBARCODE"]),
    ("CLASSCODE",       ["CLASSCODE"]),
    ("CREATETIME",      ["CREATETIME"]),
    ("KEYMATERIAL",     ["KEYMATERIAL"]),
]
