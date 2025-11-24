import pandas as pd
import os 
from config.paths import BASE_DIR

def transform_data():
    
    print("🧹 Iniciando TRANSFORM...")
    
    # Ruta del RAW
    raw_path = os.path.join(BASE_DIR, "output", "raw.csv")
    if not os.path.exists(raw_path):
        raise Exception("❌ No se encontró raw.csv. ¿Ejecutaste EXTRACT primero?")
    
    # Leer RAW
    df = pd.read_csv(raw_path, sep="|")
    print("✔ RAW cargado. Filas:", len(df))

    
    # Normalizar nombres de columnas
    df.columns = (
        df.columns
        .str.upper()
        .str.strip()
        .str.replace(" ", "_")
    )

    # Convertir a str las columnas y limpiar espacios en blanco
    columnas_str = ["SEDE_ATENCION", "DISTRITO_ATENCION", "PROVINCIA_ATENCION", "DEPARTAMENTO_ATENCION", "SEXO", "EDAD", "PROVINCIA_BENEFICIARIO", "DEPARTAMENTO_BENEFICIARIO", "NACIONALIDAD", "MES_TRAMITE", "TIPO_TRAMITE"]
    
    for col in columnas_str:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()
    
    print("✔ Columnas convertidas a str y Eliminamos los Espacios")
    
    # convertir datetime 
    df["FECHA_PROCESO"] = pd.to_datetime(df["FECHA_PROCESO"], errors="coerce")
    
    # convertir int
    df["ANIO_TRAMITE"] = pd.to_numeric(df["ANIO_TRAMITE"], errors="coerce")
    df["CANTIDAD"] = pd.to_numeric(df["CANTIDAD"], errors="coerce")
    
    # Eliminar Duplicados
    df = df.drop_duplicates()
    print("✔ Duplicados eliminados.")
    
    # guardar limpio la data
    clean_path = os.path.join(BASE_DIR, "output", "clean.csv")
    df.to_csv(clean_path, index=False)

    print(f"✔ TRANSFORM completado. Archivo generado: {clean_path}")

    return clean_path

if __name__ == "__main__":
    transform_data()