import boto3
import pandas as pd
import numpy as np
import pickle
import os
import requests
import json
import zipfile
import logging
import time
from time import strftime, gmtime, sleep
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from datetime import datetime
from urllib.parse import urlparse
from sagemaker.s3 import S3Downloader

# How often to check the status
UPDATE_DELAY_SECONDS = 15
HOME_DIRECTORY = os.path.expanduser("~")


def signed_request(method, url, data=None, params=None, headers=None, service=None):
    request = AWSRequest(
        method=method, url=url, data=data, params=params, headers=headers
    )
    session = boto3.Session()
    credentials = session.get_credentials()
    try:
        frozen_creds = credentials.get_frozen_credentials()
    except AttributeError:
        print("Could not find valid IAM credentials in any the following locations:\n")
        print(
            "env, assume-role, assume-role-with-web-identity, sso, shared-credential-file, custom-process, "
            "config-file, ec2-credentials-file, boto-config, container-role, iam-role\n"
        )
        print(
            "Go to https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html for more "
            "details on configuring your IAM credentials."
        )
        return request
    SigV4Auth(frozen_creds, service, boto3.Session().region_name).add_auth(request)
    return requests.request(
        method=method, url=url, headers=dict(request.headers), data=data
    )


def load_configuration():
    with open(f"{HOME_DIRECTORY}/graph_notebook_config.json") as f:
        data = json.load(f)
        host = data["host"]
        port = data["port"]
        if data["auth_mode"] == "IAM":
            iam = True
        else:
            iam = False
    return host, port, iam


def get_host():
    host, port, iam = load_configuration()
    return host


def get_training_job_name(prefix: str):
    return f"{prefix}-{int(time.time())}"


def check_ml_enabled():
    host, port, use_iam = load_configuration()
    response = signed_request(
        "GET", url=f"https://{host}:{port}/ml/modeltraining", service="neptune-db"
    )
    if response.status_code != 200:
        print(
            """This Neptune cluster \033[1mis not\033[0m configured to use Neptune ML.
Please configure the cluster according to the Amazpnm Neptune ML documentation before proceeding."""
        )
    else:
        print("This Neptune cluster is configured to use Neptune ML")


def get_neptune_ml_job_output_location(job_name: str, job_type: str):
    assert job_type in [
        "dataprocessing",
        "modeltraining",
        "modeltransform",
    ], "Invalid neptune ml job type"

    host, port, use_iam = load_configuration()

    response = signed_request(
        "GET",
        service="neptune-db",
        url=f"https://{host}:{port}/ml/{job_type}/{job_name}",
        headers={"content-type": "application/json"},
    )
    result = json.loads(response.content.decode("utf-8"))
    if result["status"] != "Completed":
        logging.error(
            "Neptune ML {} job: {} is not completed".format(job_type, job_name)
        )
        return
    return result["processingJob"]["outputLocation"]


def get_modeltraining_job_output_location(training_job_name: str):
    assert (
        training_job_name is not None
    ), "Neptune ML training job name id should be passed, if training job s3 output is missing"
    return get_neptune_ml_job_output_location(training_job_name, "modeltraining")


def get_embeddings(
    training_job_name: str, download_location: str = "./model-artifacts"
):
    training_job_s3_output = get_modeltraining_job_output_location(training_job_name)
    if not training_job_s3_output:
        return

    download_location = os.path.join(download_location, training_job_name)
    os.makedirs(download_location, exist_ok=True)
    # download embeddings and mapping info

    S3Downloader.download(
        os.path.join(training_job_s3_output, "embeddings/"),
        os.path.join(download_location, "embeddings/"),
    )

    entity_emb = np.load(os.path.join(download_location, "embeddings", "entity.npy"))

    return entity_emb


def get_mapping(training_job_name: str, download_location: str = "./model-artifacts"):
    training_job_s3_output = get_modeltraining_job_output_location(training_job_name)
    if not training_job_s3_output:
        return

    download_location = os.path.join(download_location, training_job_name)
    os.makedirs(download_location, exist_ok=True)
    # download embeddings and mapping info

    S3Downloader.download(training_job_s3_output, download_location)
