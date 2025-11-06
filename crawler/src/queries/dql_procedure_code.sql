WITH edi AS (
    SELECT DISTINCT
        arr.sv202_2 AS code
    FROM
        orc_db.edi837
        CROSS JOIN UNNEST(loop2000b.loop2300.loop2400) AS t(arr)
)
SELECT
    DISTINCT e.code
FROM
    edi e
    LEFT JOIN (
        SELECT
            code
        FROM
            {ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_SCHEMA}.{ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_NAME}
    ) p ON e.code = p.code
WHERE
    p.code IS NULL;
