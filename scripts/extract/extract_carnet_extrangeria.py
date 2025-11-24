import pandas as pd
import glob
from config.paths import DATA_PATTERN

def extract_data():
    
    print("📥 Iniciando EXTRACT...")
    
    # Buscamos todos los CSV dentro de DATA/ llamando al DATA_PATTERN que esta dentro de paths.py
    rutas_csv = glob.glob(DATA_PATTERN)
    if not rutas_csv:
        raise Exception("❌ No se encontraron archivos CSV en la carpeta DATA.")
    
    # Creamos una lista vacia donde iran todos los CSV
    dataframes = []
    
    # Leer cada CSV y guardarlo en la lista vacia
    for ruta in rutas_csv:
        print(f"  ➤ Leyendo archivo: {ruta}")
        df = pd.read_csv(ruta)
        dataframes.append(df)
    
    # Unir todos los CSV en un solo Dataframe
    df_total = pd.concat(dataframes, ignore_index=True)
    
    # Guardar dataset RAW
    output_path = "output/raw.csv"
    df_total.to_csv(output_path, index=False)
    
    print(f"✔ EXTRACT completado. Archivo generado: {output_path}")

    # Esto lo usará el pipeline
    return output_path

if __name__ == "__main__":
    extract_data()

