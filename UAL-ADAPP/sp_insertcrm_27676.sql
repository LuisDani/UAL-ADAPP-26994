DELIMITER //

CREATE PROCEDURE insert_matched_record(
    IN p_nombre TEXT,
    IN p_apellido TEXT,
    IN p_email TEXT,
    IN p_match_query TEXT,
    IN p_match_result TEXT,
    IN p_score TEXT,
    IN p_match_result_values TEXT,
    IN p_destTable TEXT,
    IN p_sourceTable TEXT
)
BEGIN
    INSERT INTO matched_record (
        nombre, apellido, email, match_query, match_result,
        score, match_result_values, destTable, sourceTable
    )
    VALUES (
        p_nombre, p_apellido, p_email, p_match_query, p_match_result,
        p_score, p_match_result_values, p_destTable, p_sourceTable
    );
END //

DELIMITER ;
