from rapidfuzz import process, fuzz
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

def fuzzy_match_weighted(queryRecord, choices, column_weights, score_cutoff=0):
    """
    Perform fuzzy matching with column weighting
    
    Args:
        queryRecord: dict with source record data
        choices: list of dicts with destination records
        column_weights: dict with column names as keys and weights as values
        score_cutoff: minimum score threshold
    """
    processor = lambda x: str(x).lower() if x is not None else ""
    
    best_match = None
    best_weighted_score = score_cutoff
    
    for choice in choices:
        weighted_scores = []
        total_weight = 0
        
        # Calculate weighted score for each column
        for col, weight in column_weights.items():
            if col in queryRecord and col in choice:
                source_val = processor(queryRecord[col])
                dest_val = processor(choice[col])
                
                if source_val and dest_val:  # Only compare if both values exist
                    score = fuzz.ratio(source_val, dest_val)
                    weighted_scores.append(score * weight)
                    total_weight += weight
        
        # Calculate final weighted average score
        if weighted_scores and total_weight > 0:
            weighted_avg_score = sum(weighted_scores) / total_weight
        else:
            weighted_avg_score = 0
        
        # Update best match if this score is better
        if weighted_avg_score >= best_weighted_score:
            best_weighted_score = weighted_avg_score
            best_match = {
                'match_query': queryRecord,
                'match_result': choice,
                'score': weighted_avg_score,
                'match_result_values': choice
            }
    
    # If no match found, return empty result
    if best_match is None:
        best_match = {
            'match_query': queryRecord,
            'match_result': None,
            'score': 0,
            'match_result_values': {}
        }
    
    return best_match

def execute_dynamic_matching(params_dict, score_cutoff=0):
    conn_src = connect_to_db(params_dict.get("sourceDatabase", ""))
    if not conn_src:
        raise Exception("No se pudo conectar a la base de datos origen.")

    conn_dest = connect_to_db(params_dict.get("destDatabase", ""))
    if not conn_dest:
        raise Exception("No se pudo conectar a la base de datos destino.")

    cursor_src = conn_src.cursor(dictionary=True)
    cursor_dest = conn_dest.cursor(dictionary=True)

    if 'src_dest_mappings' not in params_dict or not params_dict['src_dest_mappings']:
        raise ValueError("Debe proporcionar src_dest_mappings con columnas origen y destino")

    src_cols = ", ".join(params_dict['src_dest_mappings'].keys())
    dest_cols = ", ".join(params_dict['src_dest_mappings'].values())

    sql_source = f"SELECT {src_cols} FROM {params_dict['sourceTable']}"
    sql_dest   = f"SELECT {dest_cols} FROM {params_dict['destTable']}"

    cursor_src.execute(sql_source)
    src_rows = cursor_src.fetchall()
    source_data = [dict(row) for row in src_rows]

    cursor_dest.execute(sql_dest)
    dest_rows = cursor_dest.fetchall()
    dest_data = [dict(row) for row in dest_rows]

    conn_src.close()
    conn_dest.close()

    # Define column weights (you can make this configurable via params_dict)
    column_weights = {
        "first_name": 2,    # First name weight: 2
        "last_name": 3,     # Last name weight: 3  
        "email": 5          # Email weight: 5
    }
    
    # If you want to use different column names, you can map them
    # For example, if your source uses "nombre" but you want to weight it as "first_name"
    column_mapping = {
        "nombre": "first_name",
        "apellido": "last_name",
        "email": "email"
    }

    matching_records = []

    for record in source_data:
        # Map source column names to weighted column names
        mapped_record = {}
        for src_col, value in record.items():
            if src_col in column_mapping:
                mapped_record[column_mapping[src_col]] = value
            else:
                mapped_record[src_col] = value
        
        fm = fuzzy_match_weighted(mapped_record, dest_data, column_weights, score_cutoff)
        
        # Add original record data to results
        result_record = dict(record)
        result_record.update(fm)
        result_record.update({
            'destTable': params_dict['destTable'],
            'sourceTable': params_dict['sourceTable']
        })
        
        matching_records.append(result_record)

    return matching_records

# Keep the old function for backward compatibility
def fuzzy_match(queryRecord, choices, score_cutoff=0):
    # This is the original implementation for backward compatibility
    scorers = [fuzz.WRatio, fuzz.QRatio, fuzz.token_set_ratio, fuzz.ratio]
    processor = lambda x: str(x).lower()
    processed_query = processor(queryRecord)
    choices_data = []

    for choice in choices:
        dict_choices = dict(choice)
        queryMatch = ""
        dict_match_records = {}
        for k, v in dict_choices.items():
            if k != "DestRecordId":
                val = str(v) if v is not None else ""
                queryMatch += val
                dict_match_records[k] = v

        choices_data.append({
            'query_match': queryMatch,
            'dest_record_id': dict_choices.get('DestRecordId'),
            'match_record_values': dict_match_records
        })

    best_match = None
    best_score = score_cutoff

    for scorer in scorers:
        result = process.extractOne(
            query=processed_query,
            choices=[item['query_match'] for item in choices_data],
            scorer=scorer,
            score_cutoff=score_cutoff,
            processor=processor
        )

        if result:
            match_value, score, index = result
            if score >= best_score:
                matched_item = choices_data[index]
                best_match = {
                    'match_query': queryRecord,
                    'match_result': match_value,
                    'score': score,
                    'match_result_values': matched_item['match_record_values']
                }
        else:
            best_match = {
                'match_query': queryRecord,
                'match_result': None,
                'score': 0,
                'match_result_values': {}
            }
    return best_match