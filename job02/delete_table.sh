#!/bin/bash

# Nom de la table
TABLE_NAME="fromagerie"

# Vérification avant suppression
echo "Vérification si la table $TABLE_NAME existe déjà..."
hbase shell <<EOF
exists '$TABLE_NAME'
EOF

# Désactiver et supprimer la table
echo "Suppression de la table HBase $TABLE_NAME"
hbase shell <<EOF
disable '$TABLE_NAME'
drop '$TABLE_NAME'
EOF
echo "Table $TABLE_NAME supprimée avec succès."
