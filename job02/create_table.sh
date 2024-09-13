#!/bin/bash

# Nom de la table
TABLE_NAME="fromagerie"
COLUMN_FAMILY="cf"

# Vérifier si la table existe avant de la créer
echo "Vérification si la table $TABLE_NAME existe déjà..."
hbase shell <<EOF
exists '$TABLE_NAME'
EOF

# Si elle n'existe pas, la créer
echo "Création de la table HBase $TABLE_NAME avec la colonne familiale $COLUMN_FAMILY"
hbase shell <<EOF
create '$TABLE_NAME', '$COLUMN_FAMILY'
EOF
echo "Table $TABLE_NAME créée avec succès."
