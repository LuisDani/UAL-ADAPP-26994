from fuzzFunctions import execute_dynamic_matching
import pandas as pd

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

export_or_print_result(matches_filtrados, df)