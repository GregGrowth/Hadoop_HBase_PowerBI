#!/bin/bash

# Copier le fichier jar nécessaire pour le streaming Hadoop
cp /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar .

# Démarrer Hadoop
./start-hadoop.sh

# Vérification si le répertoire 'input' existe, sinon le créer
if ! hdfs dfs -test -e input; then
  hdfs dfs -mkdir -p input
fi

# Copier le fichier CSV dans le répertoire 'input' sur HDFS
if ! hdfs dfs -put dataw_fro03.csv input; then
  echo "Erreur lors du téléchargement des données dans HDFS."
  exit 1
fi

# Vérification si le répertoire de sortie existe, et le supprimer s'il est présent
if hdfs dfs -test -e output_lot1_exo1; then
  hdfs dfs -rm -r output_lot1_exo1
fi

# Exécuter le job Hadoop en streaming avec les fichiers mapper et reducer
if ! hadoop jar hadoop-streaming-2.7.2.jar -file mapper_lot1.py -mapper "python3 mapper_lot1.py" \
  -file reducer_lot1.py -reducer "python3 reducer_lot1.py" \
  -input input/dataw_fro03.csv -output output_lot1_exo1; then
  echo "Erreur lors de l'exécution du job Hadoop."
  exit 1
fi

echo "Le job Hadoop s'est terminé avec succès."
