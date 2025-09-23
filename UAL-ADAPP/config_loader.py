import mysql.connector
import json
import os
from datetime import datetime
import pandas as pd

def connect_to_db(database="dbo"):
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database=database
    )

def load_from_db():
    """Carga los pesos desde la tabla config_pesos_columnas en la BD"""
    conn = connect_to_db("dbo")
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT columna, peso, fecha_actualizacion, usuario_actualizacion FROM config_pesos_columnas")
    rows = cursor.fetchall()
    conn.close()

    db_config = {}
    for row in rows:
        db_config[row["columna"]] = {
            "peso": row["peso"],
            "fecha_actualizacion": row["fecha_actualizacion"].strftime("%Y-%m-%d %H:%M:%S"),
            "usuario_actualizacion": row["usuario_actualizacion"]
        }
    return db_config

def load_from_file(file_path=r"C:\Users\Daniel\Desktop\mi_proyecto_git\UAL-ADAPP/config_pesos.json"):
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return json.load(f)
    return {}


def resolve_conflicts(db_config, file_config):
    """Si hay conflictos, toma el valor más reciente"""
    final_config = {}

    all_keys = set(db_config.keys()) | set(file_config.keys())
    for key in all_keys:
        db_entry = db_config.get(key)
        file_entry = file_config.get(key)

        if db_entry and file_entry:
            # Comparar fechas
            db_date = datetime.strptime(db_entry["fecha_actualizacion"], "%Y-%m-%d %H:%M:%S")
            file_date = datetime.strptime(file_entry["fecha_actualizacion"], "%Y-%m-%d %H:%M:%S")

            final_config[key] = db_entry if db_date >= file_date else file_entry

        elif db_entry:
            final_config[key] = db_entry
        elif file_entry:
            final_config[key] = file_entry

    # Reducimos a un dict 
    return {col: data["peso"] for col, data in final_config.items()}

def get_final_column_weights():
    db_config = load_from_db()
    file_config = load_from_file()
    return resolve_conflicts(db_config, file_config)
# ...existing code...

AUDIT_JSON_PATH = r"C:\Users\Daniel\Desktop\mi_proyecto_git\UAL-ADAPP\config_pesos_audit.json"

def log_audit_change(columna, peso_anterior, peso_nuevo, usuario_actualizacion):
    """Registra el cambio en un archivo JSON para auditoría local"""
    from datetime import datetime
    log_entry = {
        "columna": columna,
        "peso_anterior": peso_anterior,
        "peso_nuevo": peso_nuevo,
        "fecha_cambio": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "usuario_actualizacion": usuario_actualizacion
    }
    logs = []
    if os.path.exists(AUDIT_JSON_PATH):
        with open(AUDIT_JSON_PATH, "r") as f:
            try:
                logs = json.load(f)
            except Exception:
                logs = []
    logs.append(log_entry)
    with open(AUDIT_JSON_PATH, "w") as f:
        json.dump(logs, f, indent=2)

def modify_column_weight_json(columna, nuevo_peso, usuario_actualizacion="admin_file"):
    """Modifica el peso en el JSON, valida rango y registra auditoría"""
    file_path = r"C:\Users\Daniel\Desktop\mi_proyecto_git\UAL-ADAPP/config_pesos.json"
    config = load_from_file(file_path)
    if not (1 <= nuevo_peso <= 50):
        print("El peso debe estar entre 1 y 50. Cambio rechazado.")
        return False
    peso_anterior = config.get(columna, {}).get("peso", 0)
    config[columna] = {
        "peso": nuevo_peso,
        "fecha_actualizacion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "usuario_actualizacion": usuario_actualizacion
    }
    with open(file_path, "w") as f:
        json.dump(config, f, indent=2)
    log_audit_change(columna, peso_anterior, nuevo_peso, usuario_actualizacion)
    print(f"Peso de columna '{columna}' modificado de {peso_anterior} a {nuevo_peso}.")
    return True

def consultar_auditoria(columna=None, ordenar_por="fecha_cambio", descendente=True):
    """Consulta el historial de auditoría local"""
    if not os.path.exists(AUDIT_JSON_PATH):
        print("No hay registros de auditoría.")
        return
    with open(AUDIT_JSON_PATH, "r") as f:
        logs = json.load(f)
    if columna:
        logs = [l for l in logs if l["columna"] == columna]
    logs = sorted(logs, key=lambda x: x.get(ordenar_por, ""), reverse=descendente)
    if logs:
        df = pd.DataFrame(logs)
        print(df)
    else:
        print("No hay registros para mostrar.")


def simular_impacto_pesos(nuevos_pesos):
    """Simula el impacto de los nuevos pesos en datos de prueba"""
    # Datos de prueba (puedes modificar estos ejemplos)
    datos_prueba = [
        {"first_name": "Juan", "last_name": "Perez", "email": "juan.perez@email.com"},
        {"first_name": "Ana", "last_name": "Gomez", "email": "ana.gomez@email.com"},
    ]
    datos_destino = [
        {"first_name": "Juan", "last_name": "Pérez", "email": "juan.perez@email.com"},
        {"first_name": "Ana", "last_name": "Gómez", "email": "ana.gomez@email.com"},
    ]
    from rapidfuzz import fuzz
    resultados = []
    for i, record in enumerate(datos_prueba):
        scores = []
        for dest in datos_destino:
            total_weight = 0
            weighted_score = 0
            for col, peso in nuevos_pesos.items():
                s = fuzz.ratio(str(record.get(col, "")).lower(), str(dest.get(col, "")).lower())
                weighted_score += s * peso
                total_weight += peso
            final_score = weighted_score / total_weight if total_weight else 0
            scores.append(final_score)
        resultados.append({
            "Registro": i+1,
            "Datos": record,
            "Scores simulados": scores
        })
    df = pd.DataFrame(resultados)
    print("Simulación de impacto de nuevos pesos:")
    print(df)

def menu_modificar_pesos():
    """Menú interactivo para modificar pesos y simular impacto"""
    config = load_from_file()
    df = pd.DataFrame([
        {"Columna": col, "Peso": data["peso"]}
        for col, data in config.items()
    ])
    print("Pesos actuales:")
    print(df)
    columna = input("¿Qué columna desea modificar? (o ENTER para cancelar): ").strip()
    if not columna or columna not in config:
        print("Columna no válida o cancelado.")
        return
    try:
        nuevo_peso = int(input(f"Ingrese el nuevo peso para '{columna}' (1-50): "))
    except ValueError:
        print("Peso inválido.")
        return
    if not (1 <= nuevo_peso <= 50):
        print("El peso debe estar entre 1 y 50. Cambio rechazado.")
        return
    print("Simulando impacto de los nuevos pesos...")
    sim_pesos = dict(config)
    sim_pesos[columna] = {"peso": nuevo_peso}
    simular_impacto_pesos({k: v["peso"] for k, v in sim_pesos.items()})
    confirmar = input("¿Desea aplicar el cambio? (s/n): ").strip().lower()
    if confirmar == "s":
        modify_column_weight_json(columna, nuevo_peso)
    else:
        print("Cambio cancelado.")

def menu_consultar_auditoria():
    columna = input("¿Desea filtrar por alguna columna? (deje vacío para todas): ").strip()
    ordenar = input("¿Ordenar por fecha_cambio o columna? (default: fecha_cambio): ").strip() or "fecha_cambio"
    desc = input("¿Orden descendente? (s/n, default: s): ").strip().lower() != "n"
    consultar_auditoria(columna if columna else None, ordenar_por=ordenar, descendente=desc)