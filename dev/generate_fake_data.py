"""Module pour générer des données fictives pour l'OLTP Postgres"""

import logging
import random
from datetime import datetime, timedelta

import postgres_conn
import psycopg2
from psycopg2 import sql

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - [%(levelname)s]  %(message)s"
)


def generate_fake_data():
    """Génère des données fictives pour Postgres"""
    conn, cur = _connect_to_database()
    max_orderkey, last_order_date = _get_last_order_data(cur)
    customers = _get_random_customers(cur)
    parts = _get_random_parts(cur)

    new_orders, new_lineitems = _generate_order_data(
        max_orderkey, last_order_date, customers, parts
    )

    _insert_data(cur, new_orders, new_lineitems)
    _close_connection(conn, cur, new_orders)


def _connect_to_database():
    """Établit la connexion à la base de données"""
    logging.info("Connexion à la base de données...")
    conn = postgres_conn.postgres_connect()
    return conn, conn.cursor()


def _get_last_order_data(cur):
    """Récupère les dernières infos de commande"""
    cur.execute("SELECT MAX(o_orderkey), MAX(o_orderdate) FROM orders;")
    max_orderkey, last_order_date = cur.fetchone()
    max_orderkey = max_orderkey or 0
    last_order_date = last_order_date or datetime.today()
    logging.info(
        "Dernière orderkey: %s, dernière date: %s", max_orderkey, last_order_date
    )
    return max_orderkey, last_order_date


def _get_random_customers(cur, limit=100):
    """Récupère des clients aléatoires"""
    cur.execute("SELECT c_custkey FROM customer ORDER BY random() LIMIT %s;", (limit,))
    return [row[0] for row in cur.fetchall()]


def _get_random_parts(cur, limit=300):
    """Récupère des produits aléatoires"""
    cur.execute("SELECT p_partkey FROM part ORDER BY random() LIMIT %s;", (limit,))
    return [row[0] for row in cur.fetchall()]


def _generate_order_data(
    max_orderkey, last_order_date, customers, parts, order_count=100
):
    """Génère les données de commandes et lineitems"""
    new_orders = []
    new_lineitems = []

    for i in range(1, order_count + 1):
        order_data = _create_order(
            max_orderkey + i, last_order_date + timedelta(days=i), customers
        )
        new_orders.append(order_data)

        lineitems = _create_lineitems_for_order(order_data[0], parts, i, order_data[3])
        new_lineitems.extend(lineitems)

    return new_orders, new_lineitems


def _create_order(orderkey, order_date, customers):
    """Crée une commande unique"""
    return (
        orderkey,
        random.choice(customers),
        "O",  # order_status
        order_date,
        0.0,  # totalprice (calculé plus tard)
        random.choice(["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]),
        f"Clerk#{random.randint(1000, 9999)}",
        0,  # shippriority
        "New generated order",
    )


def _create_lineitems_for_order(
    orderkey, parts, order_index, order_date, items_per_order=3
):
    """Crée les lineitems pour une commande"""
    lineitems = []
    for j in range(items_per_order):
        lineitems.append(
            _create_lineitem(
                orderkey,
                j + 1,
                parts[(order_index - 1) * items_per_order + j],
                order_date,
            )
        )
    return lineitems


def _create_lineitem(orderkey, line_number, partkey, order_date):
    """Crée un lineitem unique"""
    quantity = random.randint(1, 50)
    extendedprice = quantity * random.uniform(10.0, 100.0)

    return (
        orderkey,
        line_number,
        partkey,
        random.randint(1, 100),  # suppkey
        quantity,
        extendedprice,
        round(random.uniform(0, 0.1), 2),  # discount
        round(random.uniform(0, 0.08), 2),  # tax
        "N",  # returnflag
        "O",  # linestatus
        order_date + timedelta(days=random.randint(1, 30)),  # shipdate
        order_date + timedelta(days=random.randint(1, 30)),  # commitdate
        order_date + timedelta(days=random.randint(1, 30)),  # receiptdate
        "DELIVER IN PERSON",  # shipinstruct
        "AIR",  # shipmode
        "New generated lineitem",  # comment
    )


def _insert_data(cur, new_orders, new_lineitems):
    """Insère les données dans la base"""
    logging.info("Insertion des nouvelles lignes")

    insert_orders_query = """
    INSERT INTO orders
    (o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_totalprice, o_orderpriority, o_clerk, o_shippriority, o_comment)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cur.executemany(insert_orders_query, new_orders)

    insert_lineitems_query = """
    INSERT INTO lineitem
    (l_orderkey, l_linenumber, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, l_tax,
     l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cur.executemany(insert_lineitems_query, new_lineitems)


def _close_connection(conn, cur, new_orders):
    """Ferme la connexion à la base de données"""
    conn.commit()
    postgres_conn.postgres_disconnect(cur, conn)
    logging.info(
        "Insertion terminée : %d commandes générées avec 3 items chacune.",
        len(new_orders),
    )
