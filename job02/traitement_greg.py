import sys
import csv
import happybase
from datetime import datetime

# Connexion à HBase
connection = happybase.Connection('127.0.0.1', 9090)
connection.open()

# Suppression de la table si elle existe
try:
    connection.disable_table('fromagerie')
    connection.delete_table('fromagerie')
    print('fromagerie est supprimee')
except happybase.NoSuchTableError:
    print("Table 'fromagerie' n'existe pas")
except Exception as e:
    print("Erreur lors de la suppression de la table : " + str(e))

# Création de la table 'fromagerie' avec une famille de colonnes 'cf' (colonne famille)
try:
    fam_fromagerie = {
        'cf': dict()  # Famille de colonnes unique 'cf'
    }
    connection.create_table('fromagerie', fam_fromagerie)
    print("Table fromagerie est creee")
except Exception as e:
    print("Erreur lors de la creation de la table : " + str(e))

# Accès à la table 'fromagerie'
fromagerie = connection.table('fromagerie')

# Lire les données à partir de l'entrée standard
reader = csv.reader(sys.stdin)
header = next(reader)  # Ignorer l'en-tête

rowkey_counter = 0  # Initialiser le compteur de RowKey

def is_valid_datetime(datetime_str):
    """Valide si la date est au format AAAA-MM-JJ HH:MM:SS."""
    try:
        datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        return True
    except ValueError:
        return False

try:
    with fromagerie.batch() as batch:  # Utilisation du batch pour optimiser les insertions
        for row in reader:
            rowkey_counter += 1

            # Filtrer l'année 2004
            datetime_value = row[7]  # colonne de la date avec horaire
            if not is_valid_datetime(datetime_value):
                print("Ligne ignorée (date invalide : {}) : {}".format(datetime_value, row))
                continue

            year = int(datetime_value.split('-')[0])
            if year == 2004:
                print("Ligne ignorée (année 2004) : {}".format(row))
                continue

            # Créer le dictionnaire à insérer sans les champs vides
            data = {}
            columns = [
                ('cf:codcli', row[0]),
                ('cf:genrecli', row[1]),
                ('cf:nomcli', row[2]),
                ('cf:prenomcli', row[3]),
                ('cf:cpcli', row[4]),
                ('cf:villecli', row[5]),
                ('cf:codcde', row[6]),
                ('cf:datcde', row[7]),  # Date avec horaire
                ('cf:timbrecli', row[8]),
                ('cf:timbrecde', row[9]),
                ('cf:Nbcolis', row[10]),
                ('cf:cheqcli', row[11]),
                ('cf:barchive', row[12]),
                ('cf:bstock', row[13]),
                ('cf:codobj', row[14]),
                ('cf:qte', row[15]),
                ('cf:Colis', row[16]),
                ('cf:libobj', row[17]),
                ('cf:Tailleobj', row[18]),
                ('cf:Poidsobj', row[19]),
                ('cf:points', row[20]),
                ('cf:indispobj', row[21]),
                ('cf:libcondit', row[22]),
                ('cf:prixcond', row[23]),
                ('cf:puobj', row[24])
            ]

            # Ne pas inclure les champs vides (NULL)
            for col, value in columns:
                if value:  # Exclut les champs vides
                    data[col.encode('utf-8')] = value.encode('utf-8')

            # Insertion dans la table avec une rowkey unique
            rowkey = 'row{}'.format(rowkey_counter).encode('utf-8')
            batch.put(rowkey, data)

    print("Données ajoutées à la table 'fromagerie'")
except Exception as e:
    print("Erreur lors de l'insertion : {}".format(e))
finally:
    connection.close()


"""
./start-hadoop.sh
./services_hbase_thrift.sh
python3 hbase_fromagerie.py
"""