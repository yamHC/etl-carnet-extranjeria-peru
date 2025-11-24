from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load_data

def pipeline():
    print("🚀 Iniciando pipeline ETL...")
    
    # 1. EXTRACT
    raw_path = extract_data()

    # 2. TRANSFORM
    clean_path = transform_data(raw_path)

    # 3. LOAD
    load_data(clean_path)

    print("✔ Pipeline completado con éxito.")

if __name__ == "__main__":
    pipeline()