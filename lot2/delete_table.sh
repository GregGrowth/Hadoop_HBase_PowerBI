#!/bin/bash

# Nom de la table
TABLE_NAME="fromagerie"

# Supprimer la table
echo "Suppression de la table HBase $TABLE_NAME"
hbase shell <<EOF
disable '$TABLE_NAME'
drop '$TABLE_NAME'
EOF

echo "Table $TABLE_NAME supprimée avec succès."