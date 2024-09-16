#!/bin/bash

# Nom de la table
TABLE_NAME="fromagerie"
# Nom de la colonne familiale
COLUMN_FAMILY="cf"

# Créer la table
echo "Création de la table HBase $TABLE_NAME avec la colonne familiale $COLUMN_FAMILY"
hbase shell <<EOF
create '$TABLE_NAME', '$COLUMN_FAMILY'
EOF

echo "Table $TABLE_NAME créée avec succès."
