DELIMITER //

DROP PROCEDURE IF EXISTS insert_matched_record;

CREATE PROCEDURE insert_matched_record(
    IN p_nombre TEXT,
    IN p_apellido TEXT,
    IN p_email TEXT,
    IN p_match_query TEXT,
    IN p_match_result TEXT,
    IN p_score TEXT,
    IN p_match_result_values TEXT,
    IN p_destTable TEXT,
    IN p_sourceTable TEXT,
    IN p_fecha_creacion DATETIME
)
BEGIN
    DECLARE new_id INT;
    DECLARE new_record_id VARCHAR(10);

    -- Tomamos el último número usado
    SELECT IFNULL(MAX(CAST(SUBSTRING(record_id, 3) AS UNSIGNED)), 0) + 1
    INTO new_id
    FROM matched_record;

    -- Construimos el nuevo ID con ceros a la izquierda
    SET new_record_id = CONCAT('DR', LPAD(new_id, 4, '0'));

    -- Insertamos el registro
    INSERT INTO matched_record (
        record_id, nombre, apellido, email, match_query, match_result,
        score, match_result_values, destTable, sourceTable, fecha_creacion
    )
    VALUES (
        new_record_id, p_nombre, p_apellido, p_email, p_match_query, p_match_result,
        p_score, p_match_result_values, p_destTable, p_sourceTable, p_fecha_creacion
    );
END //

DELIMITER ;


DELIMITER //
CREATE PROCEDURE insert_cliente(
    IN p_cliente_id INT,
    IN p_nombre TEXT,
    IN p_apellido TEXT,
    IN p_email TEXT,
    IN p_fecha_registro DATETIME
)
BEGIN
    INSERT INTO Clientes(
        cliente_id, nombre, apellido, email, fecha_registro
    ) VALUES (
        p_cliente_id, p_nombre, p_apellido, p_email, p_fecha_registro
    );
END //
DELIMITER ;


DELIMITER //
CREATE PROCEDURE insert_usuario(
    IN p_userId INT,
    IN p_username TEXT,
    IN p_first_name TEXT,
    IN p_last_name TEXT,
    IN p_email TEXT,
    IN p_password_hash TEXT,
    IN p_rol TEXT,
    IN p_fecha_creacion DATETIME
)
BEGIN
    INSERT INTO Usuarios(
        userId, username, first_name, last_name, email, password_hash, rol, fecha_creacion
    ) VALUES (
        p_userId, p_username, p_first_name, p_last_name, p_email, p_password_hash, p_rol, p_fecha_creacion
    );
END //
DELIMITER ;


DELIMITER $$

CREATE PROCEDURE sp_update_peso_columna (
    IN p_columna VARCHAR(100),
    IN p_peso INT,
    IN p_usuario_actualizacion VARCHAR(100)
)
BEGIN
    DECLARE v_count INT;
    DECLARE v_peso_anterior INT;

    -- Validar rango permitido
    IF p_peso < 1 OR p_peso > 50 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'El peso debe estar entre 1 y 50';
    END IF;

    SELECT COUNT(*) INTO v_count
    FROM config_pesos_columnas
    WHERE columna = p_columna;

    IF v_count > 0 THEN
        SELECT peso INTO v_peso_anterior FROM config_pesos_columnas WHERE columna = p_columna;
        UPDATE config_pesos_columnas
        SET peso = p_peso,
            usuario_actualizacion = p_usuario_actualizacion,
            fecha_actualizacion = NOW()
        WHERE columna = p_columna;
        -- Registrar en auditoría
        INSERT INTO config_pesos_audit (columna, peso_anterior, peso_nuevo, fecha_cambio, usuario_actualizacion)
        VALUES (p_columna, v_peso_anterior, p_peso, NOW(), p_usuario_actualizacion);
    ELSE
        INSERT INTO config_pesos_columnas (columna, peso, usuario_actualizacion)
        VALUES (p_columna, p_peso, p_usuario_actualizacion);
        -- Registrar en auditoría (peso anterior = 0)
        INSERT INTO config_pesos_audit (columna, peso_anterior, peso_nuevo, fecha_cambio, usuario_actualizacion)
        VALUES (p_columna, 0, p_peso, NOW(), p_usuario_actualizacion);
    END IF;
END$$

DELIMITER ;