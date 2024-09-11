#!/usr/bin/env python3.5

import sys
import csv

# Filtre sur les départements et les années
valid_departments = set(['53', '61', '75', '28'])
start_year = 2008
end_year = 2012


# Fonction de nettoyage
def clean_data(row):
    # Nettoyage de chaque élément du CSV (suppression des espaces et des guillemets)
    row = [item.strip().replace('"', '') for item in row]
    return row


# Fonction de transformation des types
def transform_types(row):
    try:
        # Transformer les champs pertinents
        client_name = row[0].strip('"')  # Colonne du nom du client, suppression des guillemets
        department = row[1].strip()  # Département
        year = int(row[2])  # Année (doit être un entier)
        product = row[3].strip()  # Produit commandé
        quantity = int(row[4])  # Quantité (doit être un entier)
        return client_name, department, year, product, quantity
    except ValueError:
        # Si une conversion échoue, ignorer la ligne
        return None


# Lecture des lignes du CSV depuis l'entrée standard
for line in sys.stdin:
    line = line.strip()
    # Lire les colonnes du fichier CSV
    reader = csv.reader([line], delimiter=',')
    for row in reader:
        # Nettoyage des données
        row = clean_data(row)

        # Transformer les types
        transformed_row = transform_types(row)
        if transformed_row:
            client_name, department, year, product, quantity = transformed_row

            # Appliquer le filtre sur les années et les départements
            if start_year <= year <= end_year and department in valid_departments:
                # Émettre le client et la quantité commandée comme clé-valeur
                print('{},{},{},{}'.format(client_name, department, product, quantity))
