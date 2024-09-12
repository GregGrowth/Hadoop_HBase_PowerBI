#!/usr/bin/env python3.5

import sys
import happybase

current_client = None
current_quantity = 0
client = None

# Connexion à HBase
connection = happybase.Connection('127.0.0.1', 9090)
connection.open()
table = connection.table('maTable')

# Stockage des clients et quantités totales
client_data = {}

# Lecture des lignes depuis l'entrée standard
for line in sys.stdin:
    line = line.strip()

    # Extraire les informations du client et de la quantité depuis mapper.py
    try:
        client, department, product, quantity = line.split(',')
        quantity = int(quantity)
    except ValueError:
        # Ignorer les lignes mal formées
        continue

    # Aggrégation des quantités par client
    if client in client_data:
        client_data[client] += quantity
    else:
        client_data[client] = quantity

# Trier les clients par quantité en ordre décroissant
sorted_clients = sorted(client_data.items(), key=lambda x: x[1], reverse=True)

# Sélectionner les 10 meilleurs clients
top_10_clients = sorted_clients[:10]

# Écrire les résultats et les stocker dans HBase
for index, (client, total_quantity) in enumerate(top_10_clients):
    print('{}\t{}'.format(client, total_quantity))
    table.put(b'%i' % index, {b'cf:client': client, b'cf:quantity': str(total_quantity)})

# Fermer la connexion HBase
connection.close()
