import psycopg2
from psycopg2.extras import execute_batch


def load_to_postgres(
    records,
    columns,
    column_types,
    db_config,
    table_name,
    batch_size=1000
):
    """
    Load records into PostgreSQL, creating the table if it does not exist.

    Parameters:
    records : list[tuple]
        Data records matching the column order.
    columns : list[str]
        Ordered list of column names.
    column_types : dict
        Mapping of column name to PostgreSQL type.
    db_config : dict
        PostgreSQL connection parameters.
    table_name : str
        Target PostgreSQL table name.
    batch_size : int, optional
        Number of records inserted per batch.
    """
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Check if PostgreSQL table exists
    check_table_sql = """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = %s
            );
        """
    cursor.execute(check_table_sql, (table_name,))
    table_exists = cursor.fetchone()[0]

    # if not, define the schema and create table
    if not table_exists:
        column_defs = ", ".join(
            f"{col} {column_types[col]}" for col in columns
        )

        create_table_sql = f"""
                CREATE TABLE {table_name} (
                    {column_defs}
                );
            """
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"Created PostgreSQL table '{table_name}'")
    else:
        print(f"PostgreSQL table '{table_name}' already exists")

    # dynamically, create INSERT command
    placeholders = ", ".join(["%s"] * len(columns))
    column_list = ", ".join(columns)

    insert_sql = f"""
           INSERT INTO {table_name} ({column_list})
           VALUES ({placeholders});
       """
    execute_batch(
        cursor,
        insert_sql,
        records,
        page_size=batch_size
    )

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Insertion of records into PostgreSQL table '{table_name}' complete!")
