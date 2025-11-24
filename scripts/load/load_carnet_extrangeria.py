import pandas as pd
import os
from config.paths import BASE_DIR
from config.db_config import DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME
from sqlalchemy import create_engine    # sql - mysql - posgresql

def load_data():
    
    print("🚚 Iniciando LOAD...")

    # Conectacmos a Mysql
    engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    # leemos la Data clean.cv
    clean_path = os.path.join(BASE_DIR, "output", "clean.csv")
    if not os.path.exists(clean_path):
        raise Exception("❌ No se encontró clean.csv. Ejecuta primero TRANSFORM.")

    df = pd.read_csv(clean_path)
    print("✔ clean.csv cargado:", len(df), "filas")
    
    
    # -----------------------------------------
    # 3. CARGAR TABLAS DIM UNA POR UNA
    # -----------------------------------------
    
    # ---------- DIM_SEDE ----------
    df_sede = df[["SEDE_ATENCION", "DISTRITO_ATENCION", "PROVINCIA_ATENCION", "DEPARTAMENTO_ATENCION"]].drop_duplicates()
    df_sede.to_sql("dim_sede", engine, if_exists="append", index=False)
    print("✔ dim_beneficiario cargado")

    
    # ---------- DIM_BENEFICIARIO  ----------
    dim_beneficiario = df[['SEXO','EDAD','PROVINCIA_BENEFICIARIO','DEPARTAMENTO_BENEFICIARIO','NACIONALIDAD']].drop_duplicates()
    dim_beneficiario.to_sql("dim_beneficiario", engine, if_exists="append", index=False)
    print("✔ dim_beneficiario cargado")


    # ---------- DIM_TRAMITE  ----------
    dim_tramite = df[['TIPO_TRAMITE','ANIO_TRAMITE','MES_TRAMITE']].drop_duplicates()
    dim_tramite.to_sql('dim_tramite', engine, if_exists='append', index=False)
    print("✔ dim_tramite cargado")

    
    # ---------- DIM_TIEMPO  ----------
    dim_tiempo = df[['FECHA_PROCESO']].drop_duplicates()
    dim_tiempo.to_sql('dim_tiempo', engine, if_exists='append', index=False)
    print("✔ dim_tiempo cargado")
    
    
    # ============================
    # 4) eer las tablas DIM desde MySQL tal como están.
    # ============================
    dim_sede  = pd.read_sql("SELECT * FROM dim_sede", engine)
    dim_benef  = pd.read_sql("SELECT * FROM dim_beneficiario", engine)
    dim_tram  = pd.read_sql("SELECT * FROM dim_tramite", engine)
    dim_time  = pd.read_sql("SELECT * FROM dim_tiempo", engine)
    
    # ============================
    # 5) Hacer MERGE para obtener IDs
    # ============================

    df_fact = df.copy()

    # --- MERGE SEDE
    df_fact = df_fact.merge(
        dim_sede,
        left_on=['SEDE_ATENCION','DISTRITO_ATENCION','PROVINCIA_ATENCION','DEPARTAMENTO_ATENCION'],
        right_on=['sede_atencion','distrito_atencion','provincia_atencion','departamento_atencion'],
        how='left'
    )

    # --- MERGE BENEFICIARIO
    df_fact = df_fact.merge(
        dim_benef,
        left_on=['SEXO','EDAD','PROVINCIA_BENEFICIARIO','DEPARTAMENTO_BENEFICIARIO','NACIONALIDAD'],
        right_on=['sexo','edad','provincia_beneficiario','departamento_beneficiario','nacionalidad'],
        how='left'
    )

    # --- MERGE TRAMITE
    df_fact = df_fact.merge(
        dim_tram,
        left_on=['TIPO_TRAMITE','ANIO_TRAMITE','MES_TRAMITE'],
        right_on=['tipo_tramite','anio_tramite','mes_tramite'],
        how='left'
    )

    # --- MERGE TIEMPO
    df_fact = df_fact.merge(
        dim_time,
        left_on=['FECHA_PROCESO'],
        right_on=['fecha_proceso'],
        how='left'
    )
    
    # ============================
    # 6) Crear FACT limpia
    # ============================

    fact_final = df_fact[[
        "sede_id",
        "beneficiario_id",
        "tramite_id",
        "tiempo_id",
        "CANTIDAD"
    ]]
    
    # ============================
    # 7) Insertar FACT
    # ============================
    fact_final.to_sql("fact_tramite", engine, if_exists="append", index=False)

    print("🎉 LOAD COMPLETADO correctamente.")
    return True


if __name__ == "__main__":
    load_data()
