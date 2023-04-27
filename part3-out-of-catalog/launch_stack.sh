#!/bin/sh

echo "[START] OOC Stack"

read -p "Enter the s3 location of your embeddings file : " embeddings_file
read -p "Enter the s3 location of your movie node file : " movie_node_file

embeddings_file="s3://<bucket-name>/path/embedding.csv"
movie_node_file="s3://<bucket-name>/path/movie.csv"


cd cdk
npx cdk bootstrap -a "python3 app.py" -v
npx cdk synth -a "python3 app.py" -v

npx cdk deploy -a "python3 app.py" --all --parameters  cdk-opensearch:embeddingsFile=$embeddings_file --parameters cdk-opensearch:movieNodeFile=$movie_node_file