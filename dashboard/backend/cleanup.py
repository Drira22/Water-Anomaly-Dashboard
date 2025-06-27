# dashboard/backend/cleanup.py

import psycopg2

POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'yorkshire',
    'user': 'yorkshire',
    'password': 'yorkshire'
}

def cleanup_all_flow_tables():
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        SELECT tablename FROM pg_tables
        WHERE schemaname = 'public' AND tablename LIKE 'flow_%';
    """)
    tables = cur.fetchall()

    for table_name in tables:
        cur.execute(f'DROP TABLE IF EXISTS "{table_name[0]}" CASCADE;')
        print(f"Dropped table: {table_name[0]}")

    conn.commit()
    cur.close()
    conn.close()

if __name__ == '__main__':
    cleanup_all_flow_tables()
