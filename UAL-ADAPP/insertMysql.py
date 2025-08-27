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

def insert_clientes():
    """Clientes de la base de datos crm"""
    connection = connect_to_db('crm')
    if not connection:
        return
    
    cursor = connection.cursor()
    insert_query = """
    INSERT INTO Clientes (cliente_id, nombre, apellido, email, FechaRegistro)
    VALUES (%s, %s, %s, %s, %s)
    """
    
    try:
        with open('clientes.csv', 'r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                data = (
                    int(row['cliente_id']),
                    row['nombre'],
                    row['apellido'],
                    row['email'],
                    parse_date(row['fecha_registro'])
                )
                cursor.execute(insert_query, data)
        
        connection.commit()
        print(f"Se insertaron {cursor.rowcount} registros en la tabla Clientes (crm)")
    
    except Exception as error:
        print(f"Error al insertar clientes: {error}")
    finally:
        cursor.close()
        connection.close()

def insert_usuarios():
    """Usuarios de la base de datos dbo"""
    connection = connect_to_db('dbo')
    if not connection:
        return
    
    cursor = connection.cursor()
    insert_query = """
    INSERT INTO Usuarios (userId, username, first_name, last_name, email, 
                         password_hash, rol, fecha_creacion)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        with open('usuarios.csv', 'r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                data = (
                    int(row['userId']),
                    row['username'],
                    row['first_name'],
                    row['last_name'],
                    row['email'],
                    row['password_hash'],
                    row['rol'],
                    parse_date(row['fecha_creacion'])
                )
                cursor.execute(insert_query, data)
        
        connection.commit()
        print(f"Se insertaron {cursor.rowcount} registros en la tabla Usuarios (dbo)")
    
    except Exception as error:
        print(f"Error al insertar usuarios: {error}")
    finally:
        cursor.close()
        connection.close()

def main():
    # Inserta datos
    print("Iniciando inserción de datos...")
    
    # crm
    print("\nInsertando datos en la tabla Clientes...")
    insert_clientes()
    
    # dbo
    print("\nInsertando datos en la tabla Usuarios...")
    insert_usuarios()
    
    print("\nProceso completado.")

if __name__ == "__main__":
    main()