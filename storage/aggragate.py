import psycopg2
import vars

def write_message_to_database(message):
    try:
        conn = psycopg2.connect(
            dbname= vars.DATA_BASE_NAME,
            user=vars.DATA_BASE_USER,
            password=vars.DATA_BASE_PASS,
            host=vars.DATA_BASE_URL,
            port=vars.DATA_BASE_PORT
        )

        cursor = conn.cursor()

        # Assuming you have a "messages" table with columns "id" and "message"
        sql = """INSERT INTO messages (message) VALUES (%s) RETURNING id;"""

        cursor.execute(sql, (message,))
        inserted_id = cursor.fetchone()[0]

        conn.commit()
        cursor.close()
        conn.close()
    except psycopg2.Error as e:
        print(f"Error writing to the database: {e}")
