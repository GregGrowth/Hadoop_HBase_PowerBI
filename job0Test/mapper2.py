#!/usr/bin/env python
"""mapper.py"""

import csv
import sys

# Les informations proviennent de l'entrée standard
reader = csv.reader(sys.stdin)  # Lire l'entrée standard (les données)
header = next(reader)  # Ignorer la première ligne si c'est l'en-tête

for row in reader:
    try:
        # Vérification de la validité de la ligne
        if len(row) != 25:
            continue  # Ignorer les lignes mal formées

        # Extraire les champs nécessaires
        nomcli = row[2]  # Nom des clients
        prenomcli = row[3]  # Prénom des clients
        cpcli = row[4]  # Code postal
        villecli = row[5]  # Nom de la ville
        codcde = row[6]  # Code commande
        datcde = row[7]  # Date de commande
        codobj = row[14]  # Code de l'objet
        # qte = row[15] # Quantité de la commande
        libobj = row[17]  # Nom de l'objet
        # points = row[20] # Points de fidelité

        # Convertir la date de commande
        if len(datcde.split('-')) != 3:
            continue

        year, month, day = datcde.split('-')

        # Vérifier si la date de commande est entre 2008 et 2012 et si le code postal commence par 53, 61, 75, ou 28
        if ('2008' <= year <= '2012') and (
                cpcli.startswith('53') or cpcli.startswith('61') or cpcli.startswith('75') or cpcli.startswith('28')):

            try:
                qte = int(row[15])  # Quantité de la commande
                points = int(row[20])  # Points de fidelité
                print('%s;%s;%s;%s;%s;%s;%i;%s;%s;%i' % (
                nomcli, prenomcli, cpcli, villecli, codcde, datcde, qte, codobj, libobj, points))

            except ValueError:
                continue

    except Exception as e:
        # En cas d'erreur (par exemple format de date invalide), ignorer la ligne
        continue

'''
hadoop jar hadoop-streaming-2.7.2.jar -file mapper.py -mapper "python3 mapper.py" -file reducer.py -reducer "python3 reducer.py" -input input/word.txt -output output01
'''