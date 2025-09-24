from config_loader import simular_impacto_pesos

# Ejemplo 1: Email dominante
simular_impacto_pesos({"first_name": 5, "last_name": 5, "email": 40})

# Ejemplo 2: Pesos iguales
simular_impacto_pesos({"first_name": 20, "last_name": 20, "email": 20})

# Ejemplo 3: Solo nombre y apellido
simular_impacto_pesos({"first_name": 25, "last_name": 25, "email": 0})