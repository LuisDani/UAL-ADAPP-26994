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
        "email": "email"
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
    export_choice = input("¿Desea exportar los resultados a un archivo Excel? (s/n): ").lower()
    if export_choice != 's':
        return 
    name = input("¿Cómo desea nombrar el archivo Excel (sin extensión)?: ")
    rows = input("¿Cuántas filas desea exportar? (Ingrese un número o 'all' para todas): ").lower()
    folder = "resultados_excel"
    if export_choice == 's':
        # crear la carpeta si no existe
        os.makedirs(folder, exist_ok=True)
        file_path = os.path.join(folder, f'{name}.xlsx')
        if rows == 'all':
            df_to_export = df
        else:
            try:
                num_rows = int(rows)
                if num_rows <= 0:
                    print("Número de filas no válido. Operación cancelada.")
                    return
                df_to_export = df.head(num_rows)
            except ValueError:
                print("Número de filas no válido. Operación cancelada.")
                return
        df_to_export.to_excel(file_path, index=False)
        print(f"Resultados exportados a '{file_path}'")

def export_to_csv(df):
    if df.empty:
        print("El DataFrame está vacío. No hay datos para exportar.")
        return
    export_choice = input("¿Desea exportar los resultados a un archivo CSV? (s/n): ").lower()
    if export_choice != 's':
        return 
    name = input("¿Cómo desea nombrar el archivo CSV (sin extensión)?: ")
    rows = input("¿Cuántas filas desea exportar? (Ingrese un número o 'all' para todas): ").lower()
    folder = "resultados_csv"
    if export_choice == 's':
        os.makedirs(folder, exist_ok=True)
        file_path = os.path.join(folder, f'{name}.csv')
        if rows == 'all':
            df_to_export = df
        else:
            try:
                num_rows = int(rows)
                if num_rows <= 0:
                    print("Número de filas no válido. Operación cancelada.")
                    return
                df_to_export = df.head(num_rows)
            except ValueError:
                print("Número de filas no válido. Operación cancelada.")
                return
        df_to_export.to_csv(file_path, index=False)
        print(f"Resultados exportados a '{file_path}'")
export_to_excel(df)
export_to_csv(df)