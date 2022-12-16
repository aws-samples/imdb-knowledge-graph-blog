from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
import os
import json
import pandas as pd
import ast
import cfnresponse


def initialize_ops():
    """
    Initialize OpenSearch client
    """
    # For example, my-test-domain.us-east-1.es.amazonaws.com
    endpoint = os.environ.get("opensearch_url")
    host = endpoint.replace("https://", endpoint)
    port = 443
    region = os.environ.get("AWS_REGION")  # e.g. us-west-1  # e.g. us-west-1

    service = "es"
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        service,
        session_token=credentials.token,
    )

    ops = OpenSearch(
        hosts=[{"host": host, "port": 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=100000,
    )

    return ops


def create_index(index, ops):
    """
    This function will create an knn index using knn-specific settings
    """
    if not ops.indices.exists(index=index):
        index_settings = {
            "settings": {
                "index.knn": True,
                "index.knn.space_type": "cosinesimil",
                "analysis": {
                    "analyzer": {
                        "default": {"type": "standard", "stopwords": "_english_"}
                    }
                },
            },
            "mappings": {
                "properties": {
                    "embeddings": {
                        "type": "knn_vector",
                        "dimension": 64,  # replace with your embedding size
                    }
                }
            },
        }

        ops.indices.create(index=index, body=json.dumps(index_settings))
        print("Created the openssearch index successufly ")
    else:
        print("opensearch index already exists")


def post_request(index_name, movies, ops):
    """
    Bulk uploads text index documents
    """
    data = ""
    for movie in movies:
        data += (
            '{ "index": { "_index": "'
            + index_name
            + '", "_id": "'
            + movie["id"]
            + '" } }\n'
        )
        data += (
            '{ "year": '
            + movie["year"]
            + ', "poster": "'
            + movie["poster"]
            + '","embeddings": '
            + str(movie["embeddings"])
            + ', "title": "'
            + movie["title"]
            + '" }\n '
        )

    response = ops.bulk(data)
    return response


def post_request_emb(index_name, movies, ops):
    """
    Bulk uploads knn index documents
    """
    data = ""
    for movie in movies:
        data += (
            '{ "index": { "_index": "'
            + index_name
            + '", "_id": "'
            + movie["id"]
            + '" } }\n'
        )
        data += (
            '{ "year": '
            + movie["year"]
            + ', "poster": "'
            + movie["poster"]
            + '","embeddings": '
            + str(movie["embeddings"])
            + ', "title": "'
            + movie["title"]
            + '"}\n'
        )

    response = ops.bulk(data)
    return response


def ingest_data_into_ops(df, ops, ops_index="ooc_knn", post_method=post_request_emb):
    """
    Batch input data and upload to index
    :param df: Input data frame with movie embedding and metadata
    :param ops: opensearch client
    :param ops_index: index name for opensearch
    :param post_method: name of the function that bulk uploads data to index
    :return: upload response
    """
    movies, i = [], 1
    for ids, tt_ids, embedding, name, _, year, poster in df.values:
        movie = {
            "id": tt_ids,
            "embeddings": embedding,
            "title": name,
            "year": str(year),
            "poster": poster,
        }
        movies.append(movie)
        if i % 10000 == 0:
            response = post_method(ops_index, movies, ops)
            print(f"Processing line {i}")
            movies = []
        i += 1
    response = post_method(ops_index, movies, ops)

    return response


def merge_data(embedding_file, movie_node_file):
    """
    Merge metadata and embedding files
    :param embedding_file: Embedding file from KG training from blogpost 2
    :param movie_node_file: movie_node_file created for KG
    :return:
    """
    embed_df = pd.read_csv(embedding_file)
    embed_df["embedding"] = embed_df["embedding"].apply(lambda x: ast.literal_eval(x))
    movie_df = pd.read_csv(movie_node_file)
    merged_df = pd.merge(
        embed_df,
        movie_df[["name:String", "~id", "year:Int", "poster:String"]],
        left_on="nodes",
        right_on="~id",
    )
    return merged_df


# Lambda execution starts here
def lambda_handler(event, context):

    embedding_file = os.environ.get("embeddings_file")
    movie_node_file = os.environ.get("movie_node_file")
    print("Merging files")
    merged_df = merge_data(embedding_file, movie_node_file)
    print("Embeddings and metadata files merged")

    print("Initializing OpenSearch client")
    ops = initialize_ops()
    indices = ops.indices.get_alias().keys()
    print("Current indices are :", indices)

    # This will take 5 minutes
    print("Creating knn index")
    # Create the index using knn settings. Creating OOC text is not needed
    create_index("ooc_knn", ops)
    print("knn index created!")

    print("Uploading the data for knn index")
    response = ingest_data_into_ops(
        merged_df, ops, ops_index="ooc_knn", post_method=post_request_emb
    )
    print(response)
    print("Upload complete for knn index")

    print("Uploading the data for fuzzy word search index")
    response = ingest_data_into_ops(
        merged_df, ops, ops_index="ooc_text", post_method=post_request
    )
    print("Upload complete for fuzzy word search index")
    # Create the response and add some extra content to support CORS
    response = {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "isBase64Encoded": False,
    }
    return response
