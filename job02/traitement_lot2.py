import csv
import happybase
from datetime import datetime

# Nom du fichier CSV
csv_file = 'dataw_fro03.csv'

def est_date_valide(date_str):
    """Verifie si la date est valide pour plusieurs formats possibles."""
    formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d']
    for fmt in formats:
        try:
            datetime.strptime(date_str, fmt)
            return True
        except ValueError:
            continue
    return False

def extraire_annee(date_str):
    """Extrait l'annee de la date au format 'YYYY-MM-DD HH:MM:SS' ou 'YYYY-MM-DD'."""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').year
    except ValueError:
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').year
        except ValueError:
            return None

def inserer_csv_dans_hbase(csv_file):
    try:
        # Connexion a HBase
        connection = happybase.Connection('127.0.0.1', 9090)
        table = connection.table('fromagerie')

        with open(csv_file, mode='r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)

            # Initialiser un compteur pour les clés de ligne
            row_key_counter = 0

            # Utiliser un batch pour optimiser l'insertion des données
            with table.batch() as batch:
                for index, row in enumerate(csv_reader):
                    # Filtrer les colonnes avec des valeurs NULL
                    ligne_filtree = {k: v for k, v in row.items() if v not in ['NULL', '']}

                    # Verifier si la date est valide et ne pas importer l’annee 2004
                    colonne_date = 'datcde'  # Colonne contenant la date
                    valeur_date = ligne_filtree.get(colonne_date)
                    if valeur_date:
                        if not est_date_valide(valeur_date):
                            print("Ignorer la ligne {} en raison de date invalide : {}".format(index, valeur_date))
                            continue
                        if extraire_annee(valeur_date) == 2004:
                            print("Ignorer la ligne {} en raison de l’annee 2004".format(index))
                            continue

                    # Utiliser un compteur pour les clés de ligne
                    cle_ligne = str(row_key_counter)
                    row_key_counter += 1
                    print("Insertion de la ligne avec la cle {}".format(cle_ligne))
                    try:
                        batch.put(cle_ligne, {
                            b'cf:' + k.encode('utf-8'): v.encode('utf-8')
                            for k, v in ligne_filtree.items()
                        })
                    except Exception as e:
                        print("Erreur lors de l’insertion de la ligne avec la cle {}: {}".format(cle_ligne, e))
    except Exception as e:
        print("Erreur lors de la connexion a HBase ou de la lecture du fichier CSV: {}".format(e))
    finally:
        # Toujours fermer la connexion
        connection.close()

# Inserer les donnees CSV dans la table HBase
inserer_csv_dans_hbase(csv_file)


"""
./start-hadoop.sh
./services_hbase_thrift.sh
python3 hbase_fromagerie.py
"""
