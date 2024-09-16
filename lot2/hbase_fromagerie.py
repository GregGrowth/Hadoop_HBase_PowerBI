import sys
import csv
import happybase
from datetime import datetime

def is_valid_datetime(datetime_str):
    """Valide si la date est au format AAAA-MM-JJ HH:MM:SS."""
    try:
        # Assumer que la date inclut l'heure au format 'YYYY-MM-DD HH:MM:SS'
        datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        return True
    except ValueError:
        return False

# Connexion a HBase
connection = happybase.Connection('127.0.0.1', 9090)
connection.open()

# Suppression de la table si elle existe
try:
    connection.disable_table('fromagerie')
    connection.delete_table('fromagerie')
    print('fromagerie est supprimee')
except:
    pass

# Creation de la table 'fromagerie' avec une famille de colonnes 'cf' (colonne famille)
try:
    fam_fromagerie = {
        'cf': dict()  # Famille de colonnes unique 'cf'
    }
    connection.create_table('fromagerie', fam_fromagerie)
    print("Table fromagerie est creee")
    connection.enable_table('fromagerie')
except:
    pass

# Acces a la table 'fromagerie'
fromagerie = connection.table('fromagerie')

# Verifier que le fichier CSV est passe en argument
if len(sys.argv) != 2:
    print("Usage: python3 hbase_fromagerie.py <csv_file>")
    sys.exit(1)

# Lire le fichier CSV à partir du premier argument de la ligne de commande
csv_file = sys.argv[1]

rowkey_counter = 0  # Initialiser le compteur de RowKey

try:
    with open(csv_file, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)

        # Supprimer cette ligne si vous voulez traiter la première ligne de données
        header = next(reader)  # Ignorer l'en-tête

        for row in reader:
            rowkey_counter += 1

            # Filtrer l'annee 2004
            datetime_value = row[7]  # colonne de la date avec horaire
            if not is_valid_datetime(datetime_value):
                print("Ligne ignoree (date invalide) : " + str(row))
                continue

            year = int(datetime_value.split('-')[0])
            if year == 2004:
                print("Ligne ignoree (annee 2004) : " + str(row))
                continue

            # Creer le dictionnaire a inserer sans les champs vides
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
                if value and value.strip() != '' and value.upper() != 'NULL':  # Exclut les champs vides ou 'NULL'
                    data[col.encode('utf-8')] = value.encode('utf-8')

            # Insertion dans la table avec une rowkey unique
            rowkey = 'row{}'.format(rowkey_counter).encode('utf-8')
            fromagerie.put(rowkey, data)

    print("Donnees ajoutees a la table 'fromagerie'")
except Exception as e:
    print("Erreur lors de insertion : " + str(e))
