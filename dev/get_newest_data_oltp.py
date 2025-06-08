import psycopg2
import logging
from dotenv import load_dotenv
import os

logging.basicConfig(
    level=logging.INFO, 
    format='[%(asctime)s] - %(levelname)s - %(message)s'
)

def get_newest_data_oltp():

    load_dotenv()  # charge les variables depuis .env
    logging.info("Chargement des variables d'environnement")

    # Configuration de la connexion PostgreSQL
    conn_params = {
        'host': os.getenv('DB_HOST'),
        'port': int(os.getenv('DB_PORT')),
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }

    try:
        logging.info("Connexion à la base de données...")
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        query_last_loaded_date = "SELECT key, value FROM elt_metadata WHERE key = 'last_loaded_date' "
        cur.execute(query_last_loaded_date)
        result = cur.fetchone()

        if result:
            last_loaded_date = result[1]
            logging.info(f"Date de la derniere transaction exportée vers BigQuery: {last_loaded_date}")
        else:
            last_loaded_date = None
            logging.warning("Aucune date trouvée pour 'last_loaded_date'")
            
        return last_loaded_date


    except Exception as e:
        logging.error(f"Erreur lors de l'accès à la base de données: {e}")
        return None