from fuzzFunctions import execute_dynamic_matching
import pandas as pd
import os

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
matches_filtrados = [r for r in resultados if r.get('score', 0) > 70]  
df = pd.DataFrame(matches_filtrados)

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


#export_to_excel(df)
export_to_csv(df)
#print(resultados)