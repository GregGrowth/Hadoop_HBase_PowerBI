import sys
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt

# Dictionnaire pour accumuler les points de fidélité par client
clients_data = {}

# Lecture des données en entrée depuis STDIN
for line in sys.stdin:
    # Suppression des espaces blancs autour de la ligne
    line = line.strip()

    # Split des données en colonnes (séparées par un point-virgule dans cet exemple)
    mapper = line.split(';')
    if len(mapper) != 11:
        continue  # On ignore les lignes invalides

    # Extraction des colonnes
    codcli = mapper[0]
    nomcli = mapper[1]
    prenomcli = mapper[2]
    cpcli = mapper[3]
    villecli = mapper[4]
    codcde = mapper[5]
    datcde = mapper[6]
    qte = mapper[7]
    codobj = mapper[8]
    libobj = mapper[9]
    points = mapper[10]

    try:
        # Conversion de la quantité et des points en entiers
        qte = int(qte)
        points = int(points)
    except ValueError:
        continue  # On ignore les lignes mal formatées

    # Calcul du produit points * quantité
    fidelite = points * qte

    # Si le client existe déjà dans le dictionnaire, on cumule
    if codcli in clients_data:
        clients_data[codcli]['qte'] += qte
        clients_data[codcli]['fidelite'] += fidelite
        if libobj in clients_data[codcli]['objets']:
            clients_data[codcli]['objets'][libobj] += qte
        else:
            clients_data[codcli]['objets'][libobj] = qte

    else:
        # Sinon, on initialise l'entrée pour ce client
        clients_data[codcli] = {
            'nom': nomcli,
            'prenom': prenomcli,
            'ville': villecli,
            'cp': cpcli[:2],  # On prend les 2 premiers chiffres pour le département
            'libobj': libobj,
            'qte': qte,
            'fidelite': fidelite,
            'objets': {libobj: qte}
        }

# Trier les clients par fidélité (du plus élevé au plus bas)
sorted_clients = sorted(clients_data.items(), key=lambda x: x[1]['fidelite'], reverse=True)

# Prendre uniquement les 10 premiers
top_clients = sorted_clients[:10]


# Afficher les 10 clients les plus fidèles avec leurs objets commandés et quantités
print("Les 10 clients les plus fideles :")
for codcli, data in top_clients:
    print("\nClient: {} {}\nVille: {}, Departement: {}\nFidelite: {}\nObjets commandes:".format(
        data['nom'], data['prenom'], data['ville'], data['cp'], data['fidelite']
    ))

    # Afficher les objets commandés et leurs quantités
    for libobj, qte in data['objets'].items():
        print("\t- {}: Quantite {}".format(libobj, qte))

# Créer une liste pour stocker les données pour le fichier Excel
excel_data = []

# Création du fichier PDF pour les graphiques
output_pdf_file = '/datavolume1/resultat.pdf'
with PdfPages(output_pdf_file) as pdf:
    for codcli, data in top_clients:
        # Ajouter les données dans la liste pour l'exportation Excel
        for libobj, qte in data['objets'].items():
            excel_data.append([data['nom'], data['prenom'], data['ville'], data['cp'], data['fidelite'], libobj, qte])

        # Création du graphe pour chaque client (répartition des objets commandés)
        labels = list(data['objets'].keys())
        sizes = list(data['objets'].values())

        plt.figure()
        plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
        plt.axis('equal')  # Pour avoir un graphe circulaire
        plt.title("Repartition des objets commandes pour " + data['nom'] + " " + data['prenom'])

        # Sauvegarder le graphe dans le PDF
        pdf.savefig()
        plt.close()

# Exporter les données dans un fichier Excel
df = pd.DataFrame(excel_data, columns=['Nom', 'Prenom', 'Ville', 'Departement', 'Fidelite', 'Objet', 'Quantite'])
output_excel_file = '/datavolume1/resultat.xlsx'
df.to_excel(output_excel_file, index=False)

print("\nExportation terminee : Fichier Excel et PDF generes.")