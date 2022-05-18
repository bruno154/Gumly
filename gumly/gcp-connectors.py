import logging
from google.cloud import storage
from google.cloud import bigquery
from typing import List, NamedTuple, Union, Dict, Tuple



def create_bucket(bucket_name:str, project_id:str):
    """ A function that creates a bucket on GCP project
	Params:
		bucket_name: The bucket name to be created in GCP.
		project_id:  The GCP project id were the bucket will be created.

	Return: None
    """
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "COLDLINE"
    new_bucket = storage_client.create_bucket(bucket, project=project_id, location="us")
    
    print(
        f"""Bucket criado {new_bucket.name}
        na location {new_bucket.location}
        com storage class {new_bucket.storage_class}""")
    
    return new_bucket

def create_bq_dataset(dataset_name:str, project_id:str):

	""" A function that creates a dataset on bigquery to a specific project on GCP

    Params:
        dataset_name: the datset name to be created in GCP
        project_id: The GCP project id were the bucket will be created.

    Return: None
	"""

	# Estaciando o client do bigquery 
	client = bigquery.Client(project=project_id)

	# Criando dataset_id
	dataset_id = f"{client.project}.{dataset_name}"

	# Criando o dataset no bigquery
	dataset = bigquery.Dataset(dataset_id)
	dataset.location = "US"
	dataset = client.create_dataset(dataset, timeout=30)

	print(f"Dataset criado {client.project}.{dataset.dataset_id}")

	return dataset

def get_table_schema(table_name, schema):

    """
    
    """
    
    client_bq = bigquery.Client(project=schema)
    table = client_bq.get_table(table_name)
    table_schema = [{"name": schema.name, "type": schema.field_type} for schema in table.schema]
    return table_schema

def retrieve_data_bq(query: str,
                     project_id: str,
                     df_to_bq: pd.DataFrame = None,
                     destination_table: str = None,
                     schema: dict = None,
                     sql_type: str = "SELECT",
                     verbose: bool = False
                     ) ->NamedTuple("dataset", [("data", pd.DataFrame)]): 
    """
    Params:
        query:
        project_id:
        df_to_bq:
        destination_table:
        schema:
        sql_type:
        verbose:
    
    Return:
        dataframe: A Dataframe object with the result of the query
    
    """

    # Teste do tipo do parametro sql_type
    if  isinstance(sql_type, str):
            sql_type = sql_type
    else:
        raise Exception(f"Argument sql_type must be a string")

    # Client
    bq_client = bigquery.Client(project=project_id)

    if sql_type == 'SELECT':

        # Retriving the data
        if verbose:
            logging.debug(query)
        data = (
            bq_client.query(query)
            .result()
            .to_dataframe(create_bqstorage_client=True)
        )

        dataframe = namedtuple("dataset", ["data"])
        return dataframe(data=data)
    else:
        if verbose:
            logging.debug(query)
        df_to_bq.to_gbq(destination_table=destination_table, project_id=project_id, if_exists="fail", table_schema=schema)
        if verbose:
            return print(f"Created table {destination_table} at project {project_id} and schema {schema}")