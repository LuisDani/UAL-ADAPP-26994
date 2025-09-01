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


def chose_output(matches_filtrados, df):
    choose =  input("¿Desea imprimir los resultados en un DataFrame o en un Diccionario? (df/d): ").lower()
    if choose == 'df':
        return df
    elif choose == 'd':
        return matches_filtrados
    else:
        print("Opción no válida. Por favor, elija 'df' o 'd'.")
        return None

filtro = chose_output(matches_filtrados, df)
print(filtro)