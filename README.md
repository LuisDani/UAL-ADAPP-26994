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
- Objeto de conexión `pyodbc.Connection` para interactuar con la base de datos.

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
  - 'match_result_values': Valores del registro  coincidente.

---

## 3. execute_dynamic_matching

**Descripción:**  
Obtiene registros de dos tablas SQL, compara cada registro de la tabla origen con los de la tabla destino usando coincidencia difusa y retorna los resultados.

**Parámetros:**
- params_dict (dict): Diccionario con parámetros de conexión, nombres de tablas, esquemas y mapeos de columnas.
- score_cutoff (int, opcional): Puntaje mínimo para coincidencia (por defecto 0).

**Retorna:**  
- Lista de diccionarios, cada uno con los datos del registro origen, resultado de la coincidencia difusa y metadatos de las