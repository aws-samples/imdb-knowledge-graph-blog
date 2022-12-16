#!/bin/sh

echo "[START] OOC Stack"

read -p "Enter the s3 location of your embeddings file : " embeddings_file
read -p "Enter the s3 location of your movie node file : " movie_node_file

# embeddings_file="s3://mlsl-imdb-data/experiments/GNN_embeddings/link-pred-1639374574-1639375259-v53/embedding.csv"
# movie_node_file="s3://mlsl-imdb-data/prime/graph/nodes/movie.csv"
embeddings_file="s3://imdb-demo-us-east-1/embedding.csv"
movie_node_file="s3://imdb-demo-us-east-1/movie.csv"


cd cdk
npx cdk bootstrap -a "python3 app.py" -v
npx cdk synth -a "python3 app.py" -v
#npx cdk deploy -a "python3 app.py" cdk-vpc
npx cdk deploy -a "python3 app.py" --all --parameters  cdk-opensearch:embeddingsFile=$embeddings_file --parameters cdk-opensearch:movieNodeFile=$movie_node_file