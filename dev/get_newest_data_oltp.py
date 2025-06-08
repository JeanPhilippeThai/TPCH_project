import logging

import postgres_conn
import psycopg2

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - [%(levelname)s]  %(message)s"
)


def get_date_latest_export():

    try:
        logging.info("Connexion à la base de données...")
        conn = postgres_conn.postgres_connect()
        cur = conn.cursor()

        query_last_loaded_date = (
            "SELECT key, value FROM elt_metadata WHERE key = 'last_loaded_date' "
        )
        cur.execute(query_last_loaded_date)
        result = cur.fetchone()

        if result:
            last_loaded_date = result[1]
            logging.info(
                "Date de la derniere transaction exportée vers BigQuery: %s",
                last_loaded_date,
            )
        else:
            last_loaded_date = None
            logging.warning("Aucune date trouvée pour 'last_loaded_date'")

        postgres_conn.postgres_disconnect(cur, conn)
        return last_loaded_date

    except Exception as e:
        logging.error("Erreur lors de l'accès à la base de données: %s", e)
        return None
