from fuzzFunctions import execute_dynamic_matching
import pandas as pd
import os
import mysql.connector
from datetime import datetime
from config_loader import get_final_column_weights
from config_loader import load_from_db, load_from_file, get_final_column_weights
from config_loader import menu_modificar_pesos, menu_consultar_auditoria

params_dict = {
    "sourceDatabase": "crm",          
    "destDatabase": "dbo",             
    "sourceTable": "clientes",         
    "destTable": "usuarios",         
    "src_dest_mappings": {
        "nombre": "first_name",  
        "apellido": "last_name",     
        "email": "email"
    }
}

print("¿Desea modificar algún peso de columna antes de ejecutar el matching? (s/n)")
if input().strip().lower() == "s":
    menu_modificar_pesos()
print("¿Desea consultar el historial de auditoría de cambios de pesos? (s/n)")
if input().strip().lower() == "s":
    menu_consultar_auditoria()
print("¿Desea continuar con la ejecución del matching? (s/n)")
if input().strip().lower() == "s":
    print("=== Valores en BD ===")
    db_data = pd.DataFrame(load_from_db())
    print(db_data)
    print("\n=== Valores en Archivo ===")
    file_data = pd.DataFrame(load_from_file())
    print(file_data)
    print("\n=== Pesos Finales (Resolviendo Conflictos) ===")
    pesos = get_final_column_weights()
    df_pesos = pd.DataFrame(list(pesos.items()), columns=["Columna", "Peso"])
    print(df_pesos)

params_dict["column_weights"] = get_final_column_weights()

resultados = execute_dynamic_matching(params_dict, score_cutoff=70)

# Aquí cargas los pesos ya resueltos (archivo o BD)
params_dict["column_weights"] = get_final_column_weights()

resultados = execute_dynamic_matching(params_dict, score_cutoff=70)

for r in resultados:
    print(f"Query: {r['match_query']}")
    print(f"Match Result: {r['match_result']}")
    print(f"Score: {r['score']}")
    print("-" * 40)

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
        return df
        
    except Exception as e:
        print(f"Error al leer el archivo: {e}")
        return None

def insert_to_mysql_with_sp(df):
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

        required_columns = [
            'nombre', 'apellido', 'email', 'match_query',
            'match_result', 'score', 'match_result_values',
            'destTable', 'sourceTable'
        ]

        # Filtrar por high score
        df['score_numeric'] = pd.to_numeric(
            df['score'].astype(str).str.replace('%', ''), 
            errors='coerce'
        )
        df_high = df[df['score_numeric'] >= 97].copy()

        if df_high.empty:
            print("No hay registros con score >= 97% para insertar.")
            return

        # Hora actual para todos los registros
        now = datetime.now()

        for _, row in df_high.iterrows():
            args = tuple(row.get(col, None) for col in required_columns)
            args += (now,)  
            cursor.callproc('insert_matched_record', args)

        connection.commit()
        print(f"{len(df_high)} registros insertados usando stored procedure.")

        # Preguntar al usuario si quiere ver los datos recién insertados
        ver_datos = input("¿Desea ver los datos que se insertaron en la tabla matched_record? (s/n): ").strip().lower()
        if ver_datos == "s":
            cursor.execute("""
                SELECT nombre, apellido, email, match_query, match_result, 
                       score, match_result_values, destTable, sourceTable, fecha_creacion, record_id
                FROM matched_record 
                ORDER BY fecha_creacion DESC
                LIMIT %s
            """, (len(df_high),))
            rows = cursor.fetchall()
            if rows:
                print("Datos insertados recientemente en matched_record:")
                for row in rows:
                    print(row)
            else:
                print("No se encontraron registros recién insertados.")

    except mysql.connector.Error as error:
        print(f"Error de MySQL: {error}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexión a MySQL cerrada.")



#matched_record(df)
#df_importado = import_data()
#insert_to_mysql_with_sp(df_importado)