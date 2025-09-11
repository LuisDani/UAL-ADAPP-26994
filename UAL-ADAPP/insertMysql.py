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

def insert_from_csv_using_sp(connection, csv_file, sp_name, param_order, parse_funcs=None):
    cursor = connection.cursor()
    rows = 0
    try:
        with open(csv_file, 'r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                data = []
                for col in param_order:
                    value = row[col]
                    if parse_funcs and parse_funcs.get(col):
                        value = parse_funcs[col](value)
                    data.append(value)
                cursor.callproc(sp_name, tuple(data))
                rows += 1
        connection.commit()
        print(f"Se insertaron {rows} registros usando {sp_name}")
    except Exception as error:
        print(f"Error al insertar con SP {sp_name}: {error}")
    finally:
        cursor.close()

def main():
    print("Iniciando inserción de datos con Stored Procedures...")

    # Insertar Clientes
    crm_conn = connect_to_db('crm')
    if crm_conn:
        insert_from_csv_using_sp(
            crm_conn,
            './UAL-ADAPP/clientes.csv',
            'insert_cliente',
            ['cliente_id', 'nombre', 'apellido', 'email', 'fecha_registro'],
            parse_funcs={'cliente_id': int, 'fecha_registro': parse_date}
        )
        crm_conn.close()

    # Insertar Usuarios
    dbo_conn = connect_to_db('dbo')
    if dbo_conn:
        insert_from_csv_using_sp(
            dbo_conn,
            'usuarios.csv',
            'insert_usuario',
            ['userId', 'username', 'first_name', 'last_name', 'email', 'password_hash', 'rol', 'fecha_creacion'],
            parse_funcs={'userId': int, 'fecha_creacion': parse_date}
        )
        dbo_conn.close()

    print("\nProceso completado.")

if __name__ == "__main__":
    main()
