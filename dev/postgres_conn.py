import logging
import os

import psycopg2
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] - %(levelname)s - %(message)s"
)


def postgres_connect():

    logging.info("Chargement des variables d'environnement")
    load_dotenv()  # charge les variables depuis .env

    # Configuration de la connexion PostgreSQL
    conn_params = {
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT")),
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
    }

    conn = psycopg2.connect(**conn_params)

    return conn


def postgres_disconnect(cur, conn):
    logging.info("Fermeture de la connection Postgres")
    cur.close()
    conn.close()
