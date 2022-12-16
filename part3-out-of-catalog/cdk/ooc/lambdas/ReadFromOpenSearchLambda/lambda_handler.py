from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
import os
import json

index_name = "ooc_text"
knn_index_name = "ooc_knn"


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


def get_recommendations(embedding, recs_per_movie, es):
    query = {
        "size": recs_per_movie + 1,
        "query": {"knn": {"embeddings": {"vector": embedding, "k": recs_per_movie}}},
    }
    result = es.search(index=knn_index_name, body=query)

    return result["hits"]["hits"]


def get_movies(search_text, num_movies, recs_per_movie, es):
    query = {
        "size": num_movies,
        "fields": ["title", "embeddings", "year"],
        "query": {"match": {"title": search_text}},
    }
    result = es.search(index=index_name, body=query)

    r = {"results": []}
    for idx, h in enumerate(result["hits"]["hits"]):
        rec = get_recommendations(h["_source"]["embeddings"], recs_per_movie, es)
        rec_movies = [f"{r['_source']['title']} ({r['_source']['year']})" for r in rec]
        rec_id = [f"{r['_id']}" for r in rec]
        rec_poster = [f"{r['_source']['poster']}" for r in rec]
        r["results"].append(
            {
                "result": f"{h['_source']['title']} ({h['_source']['year']})",
                "recs": rec_movies,
                "ttId": h["_id"],
                "recs_id": rec_id,
                "rec_poster": rec_poster,
            }
        )
    return r


# Lambda execution starts here
def lambda_handler(event, context):

    # Put the user query into the query DSL for more accurate search results.
    # Note that certain fields are boosted (^).
    print(event)

    query = event["queryStringParameters"]["q"]
    num_movies = int(event["queryStringParameters"]["numMovies"])
    recs_per_movie = int(event["queryStringParameters"]["numRecs"])

    ops = initialize_ops()

    res = get_movies(query, num_movies, recs_per_movie, ops)

    # Create the response and add some extra content to support CORS
    response = {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "isBase64Encoded": False,
    }

    # Add the search results to the response
    # response['body'] = json.dumps([s1['_source']['title'] for s1 in s['hits']['hits']])
    response["body"] = json.dumps(res)
    return response
