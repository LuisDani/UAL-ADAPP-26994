from fuzzFunctions import execute_dynamic_matching

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
print(matches_filtrados)