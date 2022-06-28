from kfp.v2.dsl import component
from collections import namedtuple
from typing import NamedTuple #Union, Dict, Tuple, List

@component(
    base_image="python:3.9",
    packages_to_install=["google-cloud-storage"]

)
def create_bucket(bucket_name:str, project_id:str):
    """
	Params:
		bucket_name: The bucket name to be created in GCP.
		project_id:  The GCP project id were the bucket will be created.

	Return: None
    """

    from google.cloud import storage
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "COLDLINE"
    new_bucket = storage_client.create_bucket(bucket, project=project_id, location="us")
    
    print(
        f"""Bucket criado {new_bucket.name}
        na location {new_bucket.location}
        com storage class {new_bucket.storage_class}""")
    
    return new_bucket
