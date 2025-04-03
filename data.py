import pandas as pd
from sqlalchemy import create_engine, text
from pymongo import MongoClient

# Configurar credenciales
PG_USER = "postgres"
PG_PASSWORD = "123"
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DB = "paises"
DWH_DB = "data_warehouse"
MONGO_URI = "mongodb+srv://ejdeleon:EJGDLG5729651.@cluster0.i8iazza.mongodb.net/"
MONGO_DB = "turismo_data"
MONGO_COLLECTIONS = ["costos_africa", "costos_america", "costos_asia", "costos_europa"]

try:
    # Conexión a PostgreSQL
    pg_engine = create_engine(f'postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}')
    df_pg = pd.read_sql("SELECT * FROM envejecimiento; SELECT * FROM poblacion_costos;", pg_engine)
    print(" Datos de PostgreSQL cargados correctamente.")
except Exception as e:
    print(f" Error al conectar con PostgreSQL: {e}")
    df_pg = pd.DataFrame()

try:
    # Conexión a MongoDB
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    # Cargar datos de múltiples colecciones en MongoDB
    dfs_mongo = []
    for collection_name in MONGO_COLLECTIONS:
        collection = db[collection_name]
        df = pd.DataFrame(list(collection.find({}, {"_id": 0})))  # Ignorar el campo _id

        # Extraer valores de los costos (suponiendo que son diccionarios)
        if 'costos_diarios_estimados_en_dólares' in df.columns:
            costos_columns = pd.json_normalize(df['costos_diarios_estimados_en_dólares'])
            df = pd.concat([df, costos_columns], axis=1)
            df.drop(columns=['costos_diarios_estimados_en_dólares'], inplace=True)

        dfs_mongo.append(df)

    # Concatenar todos los DataFrames de MongoDB en uno solo
    df_mongo = pd.concat(dfs_mongo, ignore_index=True)
    print(" Datos de MongoDB cargados correctamente.")
except Exception as e:
    print(f" Error al conectar con MongoDB: {e}")
    df_mongo = pd.DataFrame()

# Normalización de datos antes de fusionar
if not df_pg.empty and not df_mongo.empty:
    # Normalizar nombres de columnas a minúsculas
    df_pg.columns = df_pg.columns.str.lower()
    df_mongo.columns = df_mongo.columns.str.lower()

    # Normaliza los nombres de las columnas
    if 'país' in df_mongo.columns:
        df_mongo['nombre_pais'] = df_mongo['país']
    
    if 'región' in df_mongo.columns:
        df_mongo['region'] = df_mongo['región']
        
    if 'continente' in df_mongo.columns:
        df_mongo['continente'] = df_mongo['continente']
    if 'población' in df_mongo.columns:
        df_mongo['poblacion'] = df_mongo['población']

    # Asegurar que las mismas columnas existen en ambos DataFrames
    common_columns = df_pg.columns.intersection(df_mongo.columns)

    # Si hay columnas en común, filtrar solo esas
    if not common_columns.empty:
        df_pg = df_pg[common_columns]
        df_mongo = df_mongo[common_columns]
    else:
        print("️ No hay columnas comunes entre PostgreSQL y MongoDB.")
    
    # Unir los datos sin duplicar columnas
    df_final = pd.concat([df_pg, df_mongo], ignore_index=True)
    print(f" Datos fusionados: {df_final.shape[0]} filas, {df_final.shape[1]} columnas")
else:
    print("️ No hay datos suficientes para fusionar.")
    df_final = pd.DataFrame()

# Carga en Data Warehouse
if not df_final.empty:
    try:
        dwh_engine = create_engine(f'postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{DWH_DB}')
        
        # Crear la tabla usando text() para ejecutar SQL directamente
        with dwh_engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS costos_turisticos_dwh (
                    id SERIAL PRIMARY KEY,
                    nombre_pais TEXT,
                    continente TEXT,
                    region TEXT,
                    poblacion INT

                )
            """))
            conn.commit()  # Asegurar que los cambios se confirmen

        # Insertar los datos en la tabla del Data Warehouse
        df_final.to_sql("costos_turisticos_dwh", dwh_engine, if_exists="append", index=False)
        
        print(" ¡Datos integrados con éxito en el Data Warehouse!")
    except Exception as e:
        print(f" Error al insertar en Data Warehouse: {e}")
else:
    print(" No hay datos para insertar en el Data Warehouse.")
          