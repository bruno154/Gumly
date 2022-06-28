import logging
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from pandas.io.json import build_table_schema




def create_bucket(bucket_name:str, project_id:str):
    """ A function that creates a bucket on GCP project
	Params:
		bucket_name: The bucket name to be created in GCP.
		project_id:  The GCP project id were the bucket will be created.

	Return: None
    """
    
    storage_client = storage.Client(project=project_id)
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

def get_schema_bqtable(table_name, project_id):

    """A function to get the schema form a table in Bigquery

    Params:
        table_name: The table name were we will get the schema from.
        project_id: The id of the project were the table is. 

    Return: 
        List with table_schema information
    
    """
    
    client_bq = bigquery.Client(project=project_id)
    table = client_bq.get_table(table_name)
    table_schema = [{"name": schema.name, "type": schema.field_type} for schema in table.schema]
    return table_schema

def bq_select_create(project_id: str,
                     query: str = None,
                     df_to_bq: pd.DataFrame = None,
                     dataset: str = None,
                     destination_table: str = None,
                     sql_type: str = "SELECT",
                     verbose: bool = False
                     )-> pd.DataFrame: 
    
    """ A function to retrieve data from bigquery as a pandas dataframe and 
        to create a table from a pandas dataframe on bigquery. 

    Params:
        query:             The sql query
        project_id:        The GCP project id were the bucket will be created. 
        df_to_bq:          The dataframe from were the bq table will be created.
        dataset:           The name of the dataset in bigquery.
        destination_table: The path were the new bq talble will be created. In the form projectId.datasetId.tableId
        sql_type:          If the operation is going to be `SELECT` to retrieve data or `CREATE` to create a new table.
        verbose:           Enable logging.
    
    Return:
        dataframe: A Dataframe object with the result of the query
    
    """

    # Teste do tipo do parametro sql_type
    if  isinstance(sql_type, str):
            sql_type = sql_type
    else:
        raise Exception("Argument sql_type must be a string")

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
        
        return data

    elif sql_type == 'CREATE':

        destination_table = f'{project_id}.{dataset}.{destination_table}'
        df_to_bq.to_gbq(destination_table=destination_table, project_id=project_id, if_exists="fail")
        
        if verbose:

            logging.debug(query)
            print(f"Created table {destination_table} at project {project_id}")
    else:

        raise Exception(f"Forbidden operation the parameter sql_type shoulbe either SELECT or CREATE.")







