import mysql.connector
import csv
from datetime import datetime

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

def parse_date(date_str):
    try:
        return datetime.strptime(date_str.strip(), '%d/%m/%Y %H:%M')
    except ValueError:
        return None

# Nueva función genérica para insertar desde CSV
def insert_from_csv(connection, table, columns, csv_file, parse_funcs=None):
    cursor = connection.cursor()
    placeholders = ', '.join(['%s'] * len(columns))
    insert_query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    rows = []
    try:
        with open(csv_file, 'r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                data = [] 
                for i, col in enumerate(columns):
                    value = row[col]
                    if parse_funcs and parse_funcs.get(col):
                        value = parse_funcs[col](value)
                    data.append(value)
                rows.append(tuple(data))
        if rows:
            cursor.executemany(insert_query, rows)  # Usar executemany para eficiencia
            connection.commit()
            print(f"Se insertaron {cursor.rowcount} registros en la tabla {table}")
        else:
            print(f"No hay datos para insertar en {table}")
    except Exception as error:
        print(f"Error al insertar en {table}: {error}")
    finally:
        cursor.close()

def main():
    print("Iniciando inserción de datos...")

    print("\nInsertando datos en la tabla Clientes...")
    crm_conn = connect_to_db('crm')
    if crm_conn:
        insert_from_csv(
            crm_conn,
            'Clientes',
            ['cliente_id', 'nombre', 'apellido', 'email', 'fecha_registro'],
            './UAL-ADAPP/clientes.csv',
            parse_funcs={'cliente_id': int, 'fecha_registro': parse_date}
        )
        crm_conn.close()

    print("\nInsertando datos en la tabla Usuarios...")
    dbo_conn = connect_to_db('dbo')
    if dbo_conn:
        insert_from_csv(
            dbo_conn,
            'Usuarios',
            ['userId', 'username', 'first_name', 'last_name', 'email', 'password_hash', 'rol', 'fecha_creacion'],
            'usuarios.csv',
            parse_funcs={'userId': int, 'fecha_creacion': parse_date}
        )
        dbo_conn.close()

    print("\nProceso completado.")

if __name__ == "__main__":
    main()