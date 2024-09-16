# Projet Hadoop Big Data - Analyse des Données Clients "fromagerie"

Ce projet fait partie du **Projet BIG DATA** dans le cadre du **POEC Data Analyst**. Il consiste à utiliser l'écosystème Hadoop pour traiter des volumes massifs de données structurées et non structurées, tout en développant des applications analytiques à l'aide de **MapReduce**, **HBase** et **Power BI**.

## Lien du GitHub
https://github.com/Lufgt/projetHadoop

## Objectif du projet

L'objectif du projet est de traiter un fichier de données clients (`dataw_fro03.csv`) en utilisant MapReduce dans Hadoop. Le projet est divisé en deux lots :

- **Lot 1** : Filtrage et agrégation des commandes clients pour générer des statistiques de fidélité.
- **Lot 2** : Intégration avec HBase et génération de visualisations à l'aide de Power BI.

### Les livrables incluent :

- Un ensemble d'applications Big Data avec analyses sous MapReduce et dashboards via **Power BI** ou **ELK**.
- Un rapport détaillé comprenant des algorithmes d'analyse, des recommandations, et des fichiers de données qualifiées.
- Des exports sous forme de fichiers Excel et graphiques PDF.

Le projet se déroulera entre le **11/09/2024 et le 17/09/2024**.

## Contributeurs

- **Lucas FANGET**
- **Melissa KUNEGEL**
- **Grégoire DELCROIX**

**Intervenant** :
- **Christophe GERMAIN**

---

## Lot 1 : Traitement des commandes clients

### Fonctionnalités principales

#### Mapper
- **Filtrage des données** : Le mapper filtre les commandes entre 2008 et 2012 provenant des départements 53, 61, 75, ou 28.
- **Extraction des informations clients et commandes** : Il extrait des informations telles que le nom, le prénom, le code postal, la ville, et les objets commandés.

#### Reducer
- **Agrégation des données clients** : Le reducer calcule la somme des points de fidélité multipliée par la quantité commandée pour chaque client.
- **Classement des clients fidèles** : Il classe les clients en fonction de leur fidélité et affiche les 10 clients les plus fidèles.
- **Génération de rapports** : Exportation des résultats sous forme de fichiers Excel et graphiques PDF.

### Installation et Lancement

#### Pré-requis
- Hadoop 2.7.2
- Python 3.5
- Bibliothèques Python : `matplotlib`, `pandas`

#### Étapes d'installation

1. Télécharger ou cloner le projet :
    ```bash
    git clone "LIEN A COMPLETER"
    cd 
    ```

2. Configurer Hadoop et démarrer les services :
    ```bash
    ./start-hadoop.sh
    ```

3. Copier le fichier de données dans HDFS :
    ```bash
    hdfs dfs -mkdir -p input
    hdfs dfs -put dataw_fro03.csv input/
    ```

4. Lancer le job Hadoop :
    Pour exécuter le job MapReduce, lancez le script `lot1.sh` :
    ```bash
    bash lot1.sh
    ```

#### Vérification des résultats
Les résultats du job sont accessibles dans HDFS :
```bash
hdfs dfs -cat output_lot1_exo1/part-00000
```


## Lot 2 : Intégration avec HBase et Power BI

### Fonctionnalités principales
- **Intégration avec HBase** : Les données clients traitées sont stockées dans une base HBase pour une gestion efficace des données.
- **Connexion avec Power BI** : Les données HBase sont visualisées à l'aide de Power BI via une connexion ODBC.

### Installation et Lancement

#### Pré-requis
- Hadoop 2.7.2
- HBase
- Power BI avec connexion ODBC
- Bibliothèques Python : `happybase`, `pandas`

#### Étapes d'installation

1. **Configurer et démarrer HBase** :
    ```bash
    ./services_hbase_thrift.sh
    start-hbase.sh
    ./hbase_odbc_rest.sh
    ```
   
2. **Connexion avec Power BI** :
   - Configurez la connexion ODBC dans Power BI.
   - Importez les données depuis HBase pour visualiser les clients fidèles et générer des rapports.

### Structure du projet

```bash
projet_hadoop/
│
├── lot1/
│   ├── lot1.sh                  # Script pour exécuter le job MapReduce
│   ├── mapper_lot1.py           # Fichier mapper pour le traitement des données
│   ├── reducer_lot1.py          # Fichier reducer pour l'agrégation des données
├── lot2/
│   ├── hbase_import.sh          # Script pour importer les données dans HBase
│   ├── powerbi.odbc             # Configuration ODBC pour Power BI
├── README.md                    # Ce fichier
├── requirements.txt             # requirements.txt
