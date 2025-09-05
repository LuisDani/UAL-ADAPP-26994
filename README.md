Documentación de funciones

## 1. connect_to_azure_sql

**Descripción:**  
Establece una conexión con una base de datos Azure SQL Server usando pyodbc.

**Parámetros:**
- server (str): Nombre o dirección del servidor SQL.
- database (str): Nombre de la base de datos.
- username (str): Usuario para autenticación.
- password (str): Contraseña del usuario.

**Retorna:**  
- Objeto de conexión 'pyodbc.Connection' para interactuar con la base de datos.

---

## 2. fuzzy_match

**Descripción:**  
Realiza una comparación entre un registro de consulta y una lista de registros usando varios algoritmos de RapidFuzz.

**Parámetros:**
- queryRecord (str): Cadena que representa el registro a comparar.
- choices (list): Lista de registros (cada uno como diccionario).
- score_cutoff (int, opcional): Puntaje mínimo para considerar una coincidencia (por defecto 0).

**Retorna:**  
- Diccionario con la mejor coincidencia encontrada, incluyendo:
  - 'match_query': Consulta original.
  - 'match_result': Valor coincidente.
  - 'score': Puntaje de similitud.
  - 'match_result_values': Valores del registro coincidente.

---

## 3. execute_dynamic_matching

**Descripción:**  
Obtiene registros de dos tablas SQL, compara cada registro de la tabla origen con los de la tabla destino usando coincidencia difusa y retorna los resultados.

**Parámetros:**
- params_dict (dict): Diccionario con parámetros de conexión, nombres de tablas, esquemas y mapeos de columnas.
- score_cutoff (int, opcional): Puntaje mínimo para coincidencia (por defecto 0).

**Retorna:**  
- Lista de diccionarios, cada uno con los datos del registro origen, resultado de la coincidencia difusa y metadatos. El diccionario arrojará valores cuyas coincidencias sean mayores al 70%.

---

## 4. insert_from_csv

**Descripción:**  
Función genérica para insertar datos desde un archivo CSV a cualquier tabla de MySQL. Permite especificar la tabla, las columnas, el archivo CSV y funciones de transformación para cada columna (por ejemplo, convertir fechas o IDs).

**Parámetros:**
- `connection` (`mysql.connector.connection.MySQLConnection`): Conexión activa a la base de datos.
- `table` (`str`): Nombre de la tabla destino.
- `columns` (`list`): Lista de nombres de columnas a insertar.
- `csv_file` (`str`): Ruta al archivo CSV de origen.
- `parse_funcs` (`dict`, opcional): Diccionario con funciones para transformar los valores de columnas específicas.

**Retorna:**  
- No retorna valor. Inserta los datos en la tabla indicada e imprime el resultado por consola.

**Características principales:**
- Permite reutilizar la misma función para cualquier tabla y archivo CSV.
- Utiliza `executemany` para insertar múltiples registros en una sola operación, mejorando la eficiencia.
- Facilita el mantenimiento y la extensión del código para nuevas tablas o archivos.

**Ejemplo de uso:**
```python
insert_from_csv(
    connection,
    'Clientes',
    ['cliente_id', 'nombre', 'apellido', 'email', 'fecha_registro'],
    'clientes.csv',
    parse_funcs={'cliente_id': int, 'fecha_registro': parse_date}
)

## 6. implementacion del stored procedure

Para mejorar el rendimiento del sistema al insertar datos en la tabla `matched_record`, se reemplazó la lógica de inserción desde Python por una stored procedure ejecutada directamente desde MySQL.

### Cambios implementados:
- Se creó la stored procedure `sp_insertcrmusuarios_01`.
- Se modificó el archivo `rapidfuzz_tables.py` para invocar esta stored procedure.
- La lógica ahora filtra solo registros con `score >= 97%` para insertar.

### Cómo ejecutar la stored procedure

Antes de ejecutar el código, asegúrate de haber creado la SP en MySQL
