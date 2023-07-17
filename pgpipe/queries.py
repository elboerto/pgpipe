from psycopg.sql import SQL

primary_key_query = SQL(
    """
        SELECT a.attname
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = {}::regclass AND i.indisprimary;
        """
)

data_type_query = SQL(
    """
        SELECT
          column_name,
          data_type
        FROM
          information_schema.columns
        WHERE
          table_name = {}
          AND table_schema = {}
          AND column_name in (
        """
)

first_chunk_element_query = SQL(
    """
        SELECT
            {}
        FROM
            {}.{}
        ORDER BY
            {} ASC
        LIMIT 1
        """
)

last_chunk_element_query = SQL(
    """
        SELECT
            {}
        FROM
            {}.{}
        ORDER BY
            {} DESC
        LIMIT 1
        """
)
