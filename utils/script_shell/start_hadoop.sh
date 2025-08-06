#!/bin/bash

echo "🔍 Vérification de JAVA_HOME..."
echo "JAVA_HOME=$JAVA_HOME"

if [ -z "$JAVA_HOME" ]; then
  echo "❌ JAVA_HOME n'est pas défini !"
  exit 1
fi

echo "🚀 Démarrage de HDFS..."
start-dfs.sh

echo "🚀 Démarrage de YARN..."
start-yarn.sh

echo "📡 Services Hadoop en cours :"
jps
