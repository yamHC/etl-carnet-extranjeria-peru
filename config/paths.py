import os

# BASE_DIR guarda la ruta raíz: - CARNET DE EXTRANJERIA (ENERO - NOVIEMBRE)
BASE_DIR = os.path.dirname(os.path.dirname(__file__))

# Carpeta DATA - DATA_DIR → Ruta absoluta de tu carpeta DATA
DATA_DIR = os.path.join(BASE_DIR, "DATA")

# Patrón para buscar CSV
DATA_PATTERN = os.path.join(DATA_DIR, "*.csv")