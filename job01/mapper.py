#!/usr/bin/env python
"""mapper.py"""

import csv
import sys

# Les informations proviennent de l'entrée standard
reader = csv.reader(sys.stdin)  # Lire l'entrée standard (les données)
header = next(reader)  # Ignorer la première ligne si c'est l'en-tête

for row in reader:
    try:
        # Vérification de la validité de la ligne (assurer qu'elle contient 25 colonnes)
        if len(row) != 25:
            continue  # Ignorer les lignes mal formées

        # Extraire les champs nécessaires
        codcli, nomcli, prenomcli, cpcli, villecli, codcde, datcde, codobj, libobj = row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[14], row[17]

        # Vérification des valeurs nulles critiques
        if any(not field or field.strip() == '' for field in [codcli, nomcli, prenomcli, cpcli, villecli, codcde, datcde, codobj, libobj]):
            continue  # Ignorer les lignes avec des champs critiques vides

        # Vérifier la validité de la date de commande (année entre 2008 et 2012)
        if len(datcde.split('-')) != 3:
            continue

        # Récupérer l'année, le mois et le jour de la commande
        year, _, _ = datcde.split('-')  # Le jour peut inclure une partie horaire

        # Vérifier si la date de commande est entre 2008 et 2012 et si le code postal commence par 53, 61, 75, ou 28
        if '2008' <= year <= '2012' and (cpcli.startswith('53') or cpcli.startswith('61') or cpcli.startswith('75') or cpcli.startswith('28')):
            try:
                qte = int(row[15])  # Quantité de la commande
                points = int(row[20])  # Points de fidélité
                if points <= 0:
                    continue  # Ignorer les points négatifs

                # Émettre la ligne avec les informations filtrées
                print('%s;%s;%s;%s;%s;%s;%s;%i;%s;%s;%i' % (
                    codcli, nomcli, prenomcli, cpcli, villecli, codcde, datcde, qte, codobj, libobj, points))

            except ValueError:
                continue  # Ignorer les lignes avec des qte ou points non valides

    except Exception as e:
        # Loguer l'erreur et continuer
        sys.stderr.write("Erreur : {}\n".format(e))
        continue


'''
hadoop jar hadoop-streaming-2.7.2.jar \
    -file mapper.py -mapper "python3 mapper.py" \
    -file reducer.py -reducer "python3 reducer.py" \
    -input input/dataw_fro03.csv -output output
'''
