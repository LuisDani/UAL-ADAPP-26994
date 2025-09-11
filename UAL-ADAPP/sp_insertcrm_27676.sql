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
