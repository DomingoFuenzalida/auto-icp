import os
import time
import shutil
import csv
import psycopg2
from psycopg2 import sql
from datetime import datetime
import logging

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(BASE_DIR, 'lims')
PROCESSED_DIR = os.path.join(BASE_DIR, 'lims_processed')
ERRORS_DIR = os.path.join(BASE_DIR, 'lims_errors')
ENV_FILE = os.path.join(BASE_DIR, '.env')
LOG_FILE = os.path.join(BASE_DIR, 'auto_lims.log')

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def load_env_config():
    """Carga configuración simple desde .env sin dependencias externas"""
    config = {
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_NAME": "ISP-DB", #<------------------   Database
        "DB_USER": "postgres", #<------------------   User
        "DB_PASS": None #<------------------   Password
    }
    if os.path.exists(ENV_FILE):
        logging.info(f"Cargando configuración desde {ENV_FILE}")
        try:
            with open(ENV_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'): continue
                    if '=' in line:
                        key, val = line.split('=', 1)
                        config[key.strip()] = val.strip()
        except Exception as e:
            logging.warning(f"Advertencia leyendo .env: {e}")
    else:
        logging.info("No se encontró archivo .env, usando valores por defecto/entorno.")
    
    return config

config = load_env_config()

# Configuración de Base de Datos
DB_HOST = os.getenv("DB_HOST", config["DB_HOST"])
DB_PORT = os.getenv("DB_PORT", config["DB_PORT"])
DB_NAME = os.getenv("DB_NAME", config["DB_NAME"])
DB_USER = os.getenv("DB_USER", config["DB_USER"])
DB_PASS = os.getenv("DB_PASS", config["DB_PASS"])

TABLE_NAME = "lims_backup_historico"

# --- FUNCIONES DE AYUDA ---

def clean_name(name):
    """Limpia nombres de columnas al estilo Pandas"""
    if not name: return "unknown_col"
    return name.strip().lower()\
        .replace(' ', '_')\
        .replace('-', '_')\
        .replace('.', '_')\
        .replace('/', '_')\
        .replace('+', 'plus_')\
        .replace('(', '')\
        .replace(')', '')

def parse_value(value):
    """Limpia y convierte valores básicos"""
    if value is None:
        return None
    v = value.strip()
    if v in ['', 'N/A', 'n/a', 'NaN', 'nan']:
        return None
    return v

def infer_sql_type(header_name, values):
    """Infiere el tipo SQL (TEXT, DOUBLE PRECISION, TIMESTAMP)"""
    if 'date' in header_name or 'time' in header_name:
         return 'TIMESTAMP'
    
    is_numeric = True
    has_data = False
    
    for v in values:
        if v is None:
            continue
        has_data = True
        try:
            float(v)
        except ValueError:
            is_numeric = False
            break
            
    if has_data and is_numeric:
        return 'DOUBLE PRECISION'
    return 'TEXT'

def move_file(file_path, dest_dir):
    try:
        filename = os.path.basename(file_path)
        dest_path = os.path.join(dest_dir, filename)
        
        # Si ya existe, añadir timestamp
        if os.path.exists(dest_path):
            base, ext = os.path.splitext(filename)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            dest_path = os.path.join(dest_dir, f"{base}_{timestamp}{ext}")
            
        shutil.move(file_path, dest_path)
        logging.info(f"Archivo movido a: {dest_path}")
    except Exception as e:
        logging.error(f"Error moviendo archivo {file_path}: {e}")

def process_file(file_path):
    logging.info(f"--- Procesando archivo: {file_path} ---")
    conn = None
    try:
        # 1. Leer CSV
        raw_data = []
        headers = []
        
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.reader(f)
            try:
                original_headers = next(reader)
            except StopIteration:
                raise Exception("El archivo CSV está vacío.")
            
            headers = [clean_name(h) for h in original_headers]
            
            # Añadir columnas de metadata
            headers.append('source_filename')
            headers.append('import_timestamp')
            
            filename = os.path.basename(file_path)
            now = datetime.now()
            
            for row in reader:
                if not row: continue
                # Rellenar o recortar
                if len(row) < len(original_headers):
                     row += [None] * (len(original_headers) - len(row))
                
                cleaned_row = [parse_value(val) for val in row[:len(original_headers)]]
                
                # Metadata
                cleaned_row.append(filename)
                cleaned_row.append(now)
                
                raw_data.append(cleaned_row)

        if not raw_data:
            raise Exception("No hay datos válidos en el archivo.")

        # 2. Conectar a BD
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS
            )
        except Exception as e:
            logging.error("Error de conexión a BD. Verifique credenciales.", exc_info=False)
            raise e

        # 3. Inferir Tipos
        num_data_cols = len(original_headers) # Excluyendo metadata
        columns_data = [[] for _ in range(num_data_cols)]
        for row in raw_data:
            for i in range(num_data_cols):
                columns_data[i].append(row[i])
        
        column_types = []
        for i, h in enumerate(headers):
            if h == 'source_filename':
                column_types.append('TEXT')
            elif h == 'import_timestamp':
                column_types.append('TIMESTAMP')
            else:
                if i < num_data_cols:
                    column_types.append(infer_sql_type(h, columns_data[i]))
                else:
                    column_types.append('TEXT')

        # 4. Operaciones SQL
        with conn.cursor() as cur:
            # Crear tabla si no existe
            col_defs = []
            for h, ctype in zip(headers, column_types):
                col_defs.append(f"{h} {ctype}")
            
            create_query = f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({', '.join(col_defs)});"
            cur.execute(create_query)
            
            # Insertar
            placeholders = ', '.join(['%s'] * len(headers))
            insert_query = sql.SQL("INSERT INTO {} VALUES ({})").format(
                sql.Identifier(TABLE_NAME),
                sql.SQL(placeholders)
            )
            
            cur.executemany(insert_query, raw_data)
            
        conn.commit()
        logging.info(f"Importación exitosa: {len(raw_data)} filas.")
        return True # Success

    except Exception as e:
        logging.error(f"ERROR PROCESANDO {file_path}: {e}", exc_info=True)
        if conn: conn.rollback()
        return False # Failure
        
    finally:
        if conn: conn.close()

def main_loop():
    logging.info(f"Iniciando monitoreo en: {INPUT_DIR}")
    logging.info(f"Salida procesados: {PROCESSED_DIR}")
    logging.info(f"Salida errores: {ERRORS_DIR}")
    
    # Asegurar directorios
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(ERRORS_DIR, exist_ok=True)
    
    while True:
        try:
            files = [f for f in os.listdir(INPUT_DIR) if os.path.isfile(os.path.join(INPUT_DIR, f))]
            
            for file_name in files:
                file_path = os.path.join(INPUT_DIR, file_name)
                
                if file_name.startswith('.'):
                    continue
                
                time.sleep(1) 
                
                success = process_file(file_path)
                
                if success:
                    move_file(file_path, PROCESSED_DIR)
                else:
                    move_file(file_path, ERRORS_DIR)
                    
        except KeyboardInterrupt:
            logging.info("Deteniendo servicio...")
            break
        except Exception as e:
            logging.error(f"Error en ciclo principal: {e}")
        
        time.sleep(5)

if __name__ == "__main__":
    main_loop()
