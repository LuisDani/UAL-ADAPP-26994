from rapidfuzz import fuzz
import mysql.connector

def connect_to_db(database):
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database=database
        )
        return connection
    except mysql.connector.Error as error:
        print(f"Error al conectar a MySQL ({database}): {error}")
        return None

def fuzzy_match_weighted(queryRecord, choices, column_weights, score_cutoff=0, debug=False):
    """
    Perform fuzzy matching with column weighting
    Args:
        queryRecord: dict with source record data
        choices: list of dicts with destination records
        column_weights: dict with column names as keys and weights as values
        score_cutoff: minimum score threshold
        debug: bool, if True prints each comparison
    """
    processor = lambda x: str(x).lower() if x is not None else ""
    best_match = None
    best_weighted_score = -1  # iniciar con -1 para siempre elegir el mejor

    for choice in choices:
        weighted_scores = []
        total_weight = 0
        
        for col, weight in column_weights.items():
            if col in queryRecord and col in choice:
                source_val = processor(queryRecord[col])
                dest_val = processor(choice[col])

                if source_val or dest_val:  # compara aunque uno sea vacío
                    score = fuzz.ratio(source_val, dest_val)
                    weighted_scores.append(score * weight)
                    total_weight += weight

        if total_weight > 0:
            weighted_avg_score = sum(weighted_scores) / total_weight
        else:
            weighted_avg_score = 0

        if debug:
            print(f"Comparando {queryRecord} con {choice} => score ponderado: {weighted_avg_score}")

        # siempre tomar el mejor score
        if best_match is None or weighted_avg_score > best_weighted_score:
            best_weighted_score = weighted_avg_score
            best_match = {
                'match_query': queryRecord,
                'match_result': choice,
                'score': weighted_avg_score,
                'match_result_values': choice
            }

    # fallback en caso de no encontrar coincidencias
    if best_match is None:
        best_match = {
            'match_query': queryRecord,
            'match_result': None,
            'score': 0,
            'match_result_values': {}
        }

    return best_match

def execute_dynamic_matching(params_dict, score_cutoff=0, debug=False):
    # conectar DBs
    conn_src = connect_to_db(params_dict.get("sourceDatabase", ""))
    if not conn_src:
        raise Exception("No se pudo conectar a la base de datos origen.")
    conn_dest = connect_to_db(params_dict.get("destDatabase", ""))
    if not conn_dest:
        raise Exception("No se pudo conectar a la base de datos destino.")

    cursor_src = conn_src.cursor(dictionary=True)
    cursor_dest = conn_dest.cursor(dictionary=True)

    src_cols = ", ".join(params_dict['src_dest_mappings'].keys())
    dest_cols = ", ".join(params_dict['src_dest_mappings'].values())

    cursor_src.execute(f"SELECT {src_cols} FROM {params_dict['sourceTable']}")
    source_data = [dict(row) for row in cursor_src.fetchall()]

    cursor_dest.execute(f"SELECT {dest_cols} FROM {params_dict['destTable']}")
    dest_data = [dict(row) for row in cursor_dest.fetchall()]

    conn_src.close()
    conn_dest.close()

    # usar pesos dinámicos
    column_weights = params_dict.get("column_weights", {})

    # mapear nombres de columnas
    column_mapping = params_dict.get("src_dest_mappings", {})

    matching_records = []

    for record in source_data:
        mapped_record = {}
        for src_col, value in record.items():
            mapped_record[column_mapping.get(src_col, src_col)] = value

        fm = fuzzy_match_weighted(mapped_record, dest_data, column_weights, score_cutoff, debug=debug)
        
        result_record = dict(record)
        result_record.update(fm)
        result_record.update({
            'destTable': params_dict['destTable'],
            'sourceTable': params_dict['sourceTable']
        })
        matching_records.append(result_record)

    return matching_records
