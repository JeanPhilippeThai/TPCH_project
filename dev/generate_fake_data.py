import os
import random
from datetime import datetime, timedelta

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql


def generate_fake_data():

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
    cur = conn.cursor()

    # 1. Récupérer la dernière orderkey et la dernière date de commande
    cur.execute("SELECT MAX(o_orderkey), MAX(o_orderdate) FROM orders;")
    max_orderkey, last_order_date = cur.fetchone()
    if max_orderkey is None:
        max_orderkey = 0
    if last_order_date is None:
        last_order_date = datetime.today()

    print(f"Dernière orderkey: {max_orderkey}, Dernière date: {last_order_date}")

    # 2. Récupérer quelques customers au hasard pour assigner aux commandes
    cur.execute("SELECT c_custkey FROM customer ORDER BY random() LIMIT 100;")
    customers = [row[0] for row in cur.fetchall()]

    # 3. Récupérer des parts (produits) au hasard pour les lineitems
    cur.execute("SELECT p_partkey FROM part ORDER BY random() LIMIT 300;")
    parts = [row[0] for row in cur.fetchall()]

    # 4. Préparer les insertions
    new_orders = []
    new_lineitems = []

    for i in range(1, 101):
        orderkey = max_orderkey + i
        custkey = random.choice(customers)
        order_date = last_order_date + timedelta(days=i)

        # Autres champs nécessaires dans orders (peut varier selon schéma)
        order_status = "O"
        totalprice = 0.0  # on peut calculer plus tard si besoin
        clerk = f"Clerk#{random.randint(1000, 9999)}"
        shippriority = 0
        comment = "New generated order"
        orderpriority = random.choice(
            ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]
        )

        new_orders.append(
            (
                orderkey,
                custkey,
                order_status,
                order_date,
                totalprice,
                orderpriority,
                clerk,
                shippriority,
                comment,
            )
        )

        # Pour chaque commande, 3 items
        for j in range(3):
            line_number = j + 1
            partkey = parts[(i - 1) * 3 + j]  # 3 parts par commande
            suppkey = random.randint(1, 100)  # suppkey aléatoire, adapter si besoin
            quantity = random.randint(1, 50)
            extendedprice = quantity * random.uniform(10.0, 100.0)
            discount = round(random.uniform(0, 0.1), 2)
            tax = round(random.uniform(0, 0.08), 2)
            returnflag = "N"
            linestatus = "O"
            shipdate = order_date + timedelta(days=random.randint(1, 30))
            commitdate = order_date + timedelta(days=random.randint(1, 30))
            receiptdate = order_date + timedelta(days=random.randint(1, 30))
            shipinstruct = "DELIVER IN PERSON"
            shipmode = "AIR"
            comment_line = "New generated lineitem"

            new_lineitems.append(
                (
                    orderkey,
                    line_number,
                    partkey,
                    suppkey,
                    quantity,
                    extendedprice,
                    discount,
                    tax,
                    returnflag,
                    linestatus,
                    shipdate,
                    commitdate,
                    receiptdate,
                    shipinstruct,
                    shipmode,
                    comment_line,
                )
            )

    # 5. Insertion dans orders
    insert_orders_query = """
    INSERT INTO orders 
    (o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_totalprice, o_orderpriority, o_clerk, o_shippriority, o_comment)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cur.executemany(insert_orders_query, new_orders)

    # 6. Insertion dans lineitem
    insert_lineitems_query = """
    INSERT INTO lineitem 
    (l_orderkey, l_linenumber, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, l_tax,
     l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cur.executemany(insert_lineitems_query, new_lineitems)

    # 7. Commit et fermeture
    conn.commit()
    cur.close()
    conn.close()
    print("Insertion terminée : 100 commandes générées avec 3 items chacune.")
