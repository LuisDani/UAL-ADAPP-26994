from fuzzFunctions import execute_dynamic_matching
import pandas as pd
import os
import mysql.connector

params_dict = {
    "sourceDatabase": "crm",          
    "destDatabase": "dbo",             
    "sourceTable": "clientes",         
    "destTable": "usuarios",         
    "src_dest_mappings": {
        "nombre": "first_name",  
        "apellido": "last_name",     
        "email": "email",
    }
}

resultados = execute_dynamic_matching(params_dict, score_cutoff=70)
df = pd.DataFrame(resultados)

def export_or_print_result(resultados, df):
    if df.empty or not resultados:
        print("No se encontraron coincidencias con el puntaje especificado.")
        return
    formato = input("¿Desea ver los resultados como DataFrame (df) o Diccionario (d)? (df/d): ").lower()
    export_choice = input("¿Desea exportar los resultados a un archivo CSV? (s/n): ").lower()
    if export_choice == 's':
        df.to_csv('resultados.csv', index=False)
        print("Resultados exportados a 'resultados.csv'")
    else:
        if formato == 'df':
            print(df.to_string(index=False))
        elif formato == 'd':
            print(resultados)
        else:
            print("Opción no válida. Mostrando como DataFrame por defecto.")
            print(df.to_string(index=False))


def export_to_excel(df):
    if df.empty:
        print("El DataFrame está vacío. No hay datos para exportar.")
        return
    
    print("Columnas disponibles:")
    print(", ".join(df.columns))
    selected_cols = input("¿Qué columnas desea exportar? (Separe los nombres con comas, o escriba 'all' para todas): ").strip()
    
    if selected_cols.lower() == 'all':
        df_selected = df
        cols_to_export = df.columns.tolist()
    else:
        cols = [col.strip() for col in selected_cols.split(",")]
        invalid_cols = [col for col in cols if col not in df.columns]
        if invalid_cols:
            print(f"Columnas inválidas: {', '.join(invalid_cols)}. Operación cancelada.")
            return
        
        if 'score' not in cols and 'score' in df.columns:
            cols.append('score')

        
        df_selected = df[cols]
        cols_to_export = cols
    
    print("\n¿Desea renombrar las columnas seleccionadas?")
    rename_choice = input("Escriba 's' para renombrar columnas o 'n' para mantener los nombres actuales: ").lower()
    
    if rename_choice == 's':
        new_names = {}
        print("\nRenombrado de columnas seleccionadas (deje en blanco para mantener el nombre actual):")
        for col in cols_to_export:
            if col == 'score':
                continue 
            
            new_name = input(f"Nombre actual '{col}'. Nuevo nombre: ").strip()
            if new_name: 
                new_names[col] = new_name
        
        if new_names:
            df_selected = df_selected.rename(columns=new_names)
            print("Columnas renombradas exitosamente.")
    
    export_choice = input("\n¿Desea exportar los resultados a un archivo Excel? (s/n): ").lower()
    if export_choice != 's':
        return 
    
    name = input("¿Cómo desea nombrar el archivo Excel (sin extensión)?: ")
    rows = input("¿Cuántas filas desea exportar? (Ingrese un número o 'all' para todas): ").lower()
    
    folder = "resultados_excel"
    if export_choice == 's':
        os.makedirs(folder, exist_ok=True)
        file_path = os.path.join(folder, f'{name}.xlsx')
        
        if rows == 'all':
            df_to_export = df_selected
        else:
            try:
                num_rows = int(rows)
                if num_rows <= 0:
                    print("Número de filas no válido. Operación cancelada.")
                    return
                df_to_export = df_selected.head(num_rows)
            except ValueError:
                print("Número de filas no válido. Operación cancelada.")
                return
        
        df_to_export.to_excel(file_path, index=False)
        print(f"Resultados exportados a '{file_path}'")


def export_to_csv(df):
    if df.empty:
        print("El DataFrame está vacío. No hay datos para exportar.")
        return
    
    df_processed = df.copy()
    
    if 'score' in df_processed.columns:
        df_processed['score'] = df_processed['score'].apply(lambda x: f"{x:.2f}%" if isinstance(x, (int, float)) else x)
    
    if 'nombre' in df_processed.columns and 'apellido' in df_processed.columns:
        df_processed['nombre_completo'] = df_processed['nombre'] + ' ' + df_processed['apellido']
        df_processed = df_processed.drop(['nombre', 'apellido'], axis=1, errors='ignore')
    
    print("Columnas disponibles:")
    print(", ".join(df_processed.columns))
    selected_cols = input("¿Qué columnas desea exportar? (Separe los nombres con comas, o escriba 'all' para todas): ").strip()
    
    if selected_cols.lower() == 'all':
        df_selected = df_processed
        cols_to_export = df_processed.columns.tolist()
    else:
        cols = [col.strip() for col in selected_cols.split(",")]
        invalid_cols = [col for col in cols if col not in df_processed.columns]
        if invalid_cols:
            print(f"Columnas inválidas: {', '.join(invalid_cols)}. Operación cancelada.")
            return
        
        if 'score' not in cols and 'score' in df_processed.columns:
            cols.append('score')
            print("La columna 'score' ha sido añadida automáticamente.")
        
        df_selected = df_processed[cols]
        cols_to_export = cols
    
    print("\n¿Desea renombrar las columnas seleccionadas?")
    rename_choice = input("Escriba 's' para renombrar columnas o 'n' para mantener los nombres actuales: ").lower()
    
    if rename_choice == 's':
        new_names = {}
        print("\nRenombrado de columnas seleccionadas (deje en blanco para mantener el nombre actual):")
        for col in cols_to_export:
            if col == 'score':
                continue
            
            new_name = input(f"Nombre actual '{col}'. Nuevo nombre: ").strip()
            if new_name:
                new_names[col] = new_name
        
        if new_names:
            df_selected = df_selected.rename(columns=new_names)
            print("Columnas renombradas exitosamente.")
    
    export_choice = input("\n¿Desea exportar los resultados a un archivo CSV? (s/n): ").lower()
    if export_choice != 's':
        return 
    
    name = input("¿Cómo desea nombrar el archivo CSV (sin extensión)?: ")
    rows = input("¿Cuántas filas desea exportar? (Ingrese un número o 'all' para todas): ").lower()
    
    folder = "resultados_csv"
    if export_choice == 's':
        os.makedirs(folder, exist_ok=True)
        file_path = os.path.join(folder, f'{name}.csv')
        
        if rows == 'all':
            df_to_export = df_selected
        else:
            try:
                num_rows = int(rows)
                if num_rows <= 0:
                    print("Número de filas no válido. Operación cancelada.")
                    return
                df_to_export = df_selected.head(num_rows)
            except ValueError:
                print("Número de filas no válido. Operación cancelada.")
                return
        
        df_to_export.to_csv(file_path, index=False)
        print(f"Resultados exportados a '{file_path}'")


def matched_record(df):
    if df.empty:
        print("No se encontraron coincidencias con el puntaje especificado.")
        return
    
    # Primero filtrar por score numérico antes de convertirlo a porcentaje
    # Asegurarnos de que el score sea numérico
    df['score'] = pd.to_numeric(df['score'], errors='coerce')
    
    # Filtrar scores válidos (no NaN)
    df_valid = df.dropna(subset=['score'])
    
    if df_valid.empty:
        print("No hay scores válidos para analizar.")
        return
    
    # Separar en high y low score usando el valor numérico
    df_high_score = df_valid[df_valid['score'] >= 97]  
    df_low_score = df_valid[df_valid['score'] < 97]
    
    # Ahora convertir a porcentaje para mostrar
    df_high_score = df_high_score.copy()
    df_low_score = df_low_score.copy()
    df_high_score['score'] = df_high_score['score'].apply(lambda x: f"{x:.2f}%")
    df_low_score['score'] = df_low_score['score'].apply(lambda x: f"{x:.2f}%")
    
    if df_high_score.empty:
        print("No se encontraron coincidencias con un puntaje igual o superior al 97%.")
    else:
        print("Registros con puntaje igual o superior al 97%:")
        print(df_high_score)
        print()  # Línea en blanco para separar
    if df_low_score.empty:
        print("No se encontraron coincidencias con un puntaje inferior al 97%.")
    else:
        print("Registros con puntaje inferior al 97%:")
        print(df_low_score[['nombre', 'apellido', 'score']].to_string(index=False))
    
    # Función para seleccionar columnas
    def select_columns_for_export(df_to_export, export_type):
        if df_to_export.empty:
            return df_to_export
            
        print(f"\nColumnas disponibles para exportación {export_type}:")
        print(", ".join(df_to_export.columns))
        
        selected_cols = input(f"¿Qué columnas desea exportar para {export_type}? (Separe con comas o 'all' para todas): ").strip()
        
        if selected_cols.lower() == 'all':
            return df_to_export
        else:
            cols = [col.strip() for col in selected_cols.split(",")]
            invalid_cols = [col for col in cols if col not in df_to_export.columns]
            if invalid_cols:
                print(f"Columnas inválidas: {', '.join(invalid_cols)}. Exportando todas las columnas.")
                return df_to_export
            return df_to_export[cols]
    
    # Exportar high score a Excel
    export_high = input("\n¿Desea exportar los registros con puntaje igual o superior al 97% a un archivo Excel? (s/n): ").lower()
    if export_high == 's':
        df_high_export = select_columns_for_export(df_high_score, "high score")
        folder = "resultados_excel"
        name = input("¿Cómo desea nombrar el archivo Excel de high score (sin extensión)?: ")
        os.makedirs(folder, exist_ok=True)
        file_path_high = os.path.join(folder, f'{name}.xlsx')
        df_high_export.to_excel(file_path_high, index=False)
        print(f"Registros con puntaje igual o superior al 97% exportados a '{file_path_high}'")
    
    # Exportar low score a Excel
    export_low = input("¿Desea exportar los registros con puntaje inferior al 97% a un archivo Excel? (s/n): ").lower()
    if export_low == 's':
        df_low_export = select_columns_for_export(df_low_score, "low score")
        folder = "resultados_excel"
        name = input("¿Cómo desea nombrar el archivo Excel de low score (sin extensión)?: ")
        os.makedirs(folder, exist_ok=True)
        file_path_low = os.path.join(folder, f'{name}.xlsx')
        df_low_export.to_excel(file_path_low, index=False)
        print(f"Registros con puntaje inferior al 97% exportados a '{file_path_low}'")

    # Exportar high score a CSV
    export_high_csv = input("\n¿Desea exportar los registros con puntaje igual o superior al 97% a un archivo CSV? (s/n): ").lower()
    if export_high_csv == 's':
        df_high_export_csv = select_columns_for_export(df_high_score, "high score CSV")
        name = input("¿Cómo desea nombrar el archivo CSV de high score (sin extensión)?: ")
        folder = "resultados_csv"
        os.makedirs(folder, exist_ok=True)
        file_path_high_csv = os.path.join(folder, f'{name}.csv')
        df_high_export_csv.to_csv(file_path_high_csv, index=False)
        print(f"Registros con puntaje igual o superior al 97% exportados a '{file_path_high_csv}'")
    
    # Exportar low score a CSV
    export_low_csv = input("¿Desea exportar los registros con puntaje inferior al 97% a un archivo CSV? (s/n): ").lower()
    if export_low_csv == 's':
        df_low_export_csv = select_columns_for_export(df_low_score, "low score CSV")
        name = input("¿Cómo desea nombrar el archivo CSV de low score (sin extensión)?: ")
        folder = "resultados_csv"
        os.makedirs(folder, exist_ok=True)
        file_path_low_csv = os.path.join(folder, f'{name}.csv')
        df_low_export_csv.to_csv(file_path_low_csv, index=False)
        print(f"Registros con puntaje inferior al 97% exportados a '{file_path_low_csv}'")

def import_data():
    file_path = input("Ingrese el nombre del archivo Excel o CSV (ej: datos.csv, archivo.xlsx): ").strip()
    
    # Verificar si el archivo existe en la ruta proporcionada
    if not os.path.isfile(file_path):
        # Si no existe, buscar en la misma carpeta del script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        file_name = os.path.basename(file_path)
        new_path = os.path.join(script_dir, file_name)
        
        if os.path.isfile(new_path):
            file_path = new_path
            print(f"Archivo encontrado en la misma carpeta: {file_path}")
        else:
            print("El archivo no existe en la ruta proporcionada ni en la carpeta del script.")
            print("Por favor, verifique que el archivo exista y esté en la misma carpeta que este script.")
            return None
    
    try:
        # Determinar el tipo de archivo
        if file_path.lower().endswith('.xlsx'):
            df = pd.read_excel(file_path)
        elif file_path.lower().endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            print("Formato de archivo no soportado. Por favor, use .xlsx o .csv.")
            return None
        
        print("Archivo importado exitosamente.")
        print(f"Dimensiones del DataFrame: {df.shape[0]} filas x {df.shape[1]} columnas")
        print("Columnas disponibles:")
        print(", ".join(df.columns))
        return df
        
    except Exception as e:
        print(f"Error al leer el archivo: {e}")
        return None

def insert_to_mysql(df):
    if df is None or df.empty:
        print("DataFrame vacío. No hay datos para insertar.")
        return
    
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',
            database='dbo'
        )
        
        cursor = connection.cursor()
        
        # Nombre fijo de la tabla
        table_name = "matched_record"
        
        # Columnas fijas que debe tener la tabla
        required_columns = [
            'nombre', 'apellido', 'email', 'match_query', 
            'match_result', 'score', 'match_result_values', 
            'destTable', 'sourceTable'
        ]
        
        # Crear tabla con las columnas requeridas (todas como TEXT)
        column_definitions = [f"`{col}` TEXT" for col in required_columns]
        create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(column_definitions)})"
        cursor.execute(create_table_query)
        connection.commit()
        
        # DEBUG: Mostrar información del DataFrame recibido
        print(f"DataFrame recibido con {len(df)} registros y columnas: {list(df.columns)}")
        print(f"Primeros valores de 'score': {df['score'].head().tolist() if 'score' in df.columns else 'Columna score no encontrada'}")
        
        # Filtrar solo high matches (score >= 97)
        if 'score' in df.columns:
            # Convertir score a numérico - manejar el símbolo %
            def convert_score(score_value):
                if isinstance(score_value, str):
                    # Remover el símbolo % y convertir a float
                    score_value = score_value.replace('%', '').strip()
                try:
                    return float(score_value)
                except (ValueError, TypeError):
                    return None
            
            df['score_numeric'] = df['score'].apply(convert_score)
            print(f"Scores convertidos: {df['score_numeric'].head().tolist()}")
            
            df_high_matches = df[df['score_numeric'] >= 97].copy()
            df_high_matches.drop('score_numeric', axis=1, inplace=True, errors='ignore')
            print(f"Registros con score >= 97: {len(df_high_matches)}")
        else:
            print("No se encontró columna 'score' en el DataFrame. Insertando todos los registros.")
            df_high_matches = df.copy()
        
        if df_high_matches.empty:
            print("No hay registros con score >= 97% para insertar.")
            # Mostrar más información para debug
            if 'score_numeric' in df.columns:
                print(f"Valores únicos de score: {df['score_numeric'].unique()}")
                print(f"Valores máximos y mínimos: max={df['score_numeric'].max()}, min={df['score_numeric'].min()}")
            return
        
        # DEBUG: Mostrar los registros que se van a insertar
        print("Registros a insertar:")
        print(df_high_matches.head())
        
        # Preparar datos para inserción - mapear columnas existentes y dejar vacías las faltantes
        data_to_insert = []
        for _, row in df_high_matches.iterrows():
            values = []
            for col in required_columns:
                if col in df_high_matches.columns:
                    value = row[col]
                    # Manejar valores NaN/None
                    if pd.isna(value):
                        values.append(None)
                    elif isinstance(value, (dict, list)):
                        values.append(str(value))
                    else:
                        values.append(str(value))
                else:
                    values.append(None)  # Columna faltante, insertar NULL
            data_to_insert.append(tuple(values))
        
        print(f"Total de registros preparados para inserción: {len(data_to_insert)}")
        
        # Insertar datos
        placeholders = ', '.join(['%s'] * len(required_columns))
        columns_str = ', '.join([f"`{col}`" for col in required_columns])
        insert_query = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
        
        # Insertar todos los registros de una vez (o en lotes si son muchos)
        if data_to_insert:
            cursor.executemany(insert_query, data_to_insert)
            connection.commit()
            print(f"{len(data_to_insert)} registros insertados exitosamente en la tabla '{table_name}'.")
        else:
            print("No hay datos para insertar después del procesamiento.")
        
    except mysql.connector.Error as error:
        print(f"Error al conectar o insertar en MySQL: {error}")
    except Exception as e:
        print(f"Error inesperado: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Cerrar conexión
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexión a MySQL cerrada.")


#matched_record(df)
df_importado = import_data()
insert_to_mysql(df_importado)