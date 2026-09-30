#!/bin/bash
# PARTIE 2 - Export des collections MongoDB vers des CSV importables dans Gephi
# Usage : bash gephi/export_for_gephi.sh            (sous-graphe suspect, recommandé)
#         bash gephi/export_for_gephi.sh full       (graphe complet)
SUFFIX="_suspect"; [ "$1" == "full" ] && SUFFIX=""
docker exec mongodb mongoexport --db fraud_lab --collection graph_nodes${SUFFIX} --type=csv \
  --fields Id,Label,account_type,in_partners,out_partners,total_received,total_sent,pass_through_ratio,fraud_involved \
  --out /gephi/nodes${SUFFIX}.csv
docker exec mongodb mongoexport --db fraud_lab --collection graph_edges${SUFFIX} --type=csv \
  --fields Source,Target,Type,Weight,total_amount,fraud_count,suspicious \
  --out /gephi/edges${SUFFIX}.csv
echo "Fichiers générés dans ./gephi : nodes${SUFFIX}.csv et edges${SUFFIX}.csv"
