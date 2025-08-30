from google.cloud import bigquery
import pandas as pd
import os
from google.api_core.exceptions import NotFound
import uuid
import pandas_gbq


"""
1. read_table(table_id : str, n_records : int = 10)
2. create_table(table_id : str, schema : list = None): Creates a  bigquery.Table with the provided schema.

3. load_table_from_file_csv(table_id : str, input_path : str, input_file_name : str): Loads records from .csv file. 
When provided a schema a  bigquery.Table will be created. Otherwise it's infered from the .csv data.
If not job config provided, a basic config will be used (CSV format, skip first row and autotect schema True or False depending if passed a schema)

4. load_table_from_dataframe(table_id : str, df : pd.DataFrame, append_new_data : bool = False)

5. add_records_to_table(table_id : str, input_path : str, input_file_name : str)


"""

class LoadToBigQuery:
    def __init__(self, project_id, dataset_id, schema : list = None, job_config : dict = None):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client()
        self.schema = schema
        self.tables_in_dataset = self.get_list_of_tables()

        self.job_config_appending = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"   # or "WRITE_TRUNCATE" to replace everything
        )

        autodetect_schema = False if schema else True

        self.basic_job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # skip header row
            autodetect=autodetect_schema,  
            write_disposition="WRITE_TRUNCATE", 
        )
 
        self.job_config = self.return_job_config() if job_config else job_config

        # self.job_config = job_config if job_config is None else self.return_job_config()


    def return_job_config(self):
        job_config = bigquery.LoadJobConfig(**self.job_config)
        return job_config

    def get_list_of_tables(self) -> list:
        dataset = f"{self.project_id}.{self.dataset_id}"
        tables = self.client.list_tables(dataset)
        tables_id = [table.table_id for table in tables]
        #print(f"Tables in dataset {dataset}: {tables_id}")
        return tables_id
    
    def table_exists(self, table_id: str) -> bool:
        table_id = f"{self.project_id}.{self.dataset_id}.{table_id}"
        try:
            self.client.get_table(table_id)
            return True
        except NotFound:
            return False
        
    def delete_table(self, table_id : str):
        table_id = f"{self.project_id}.{self.dataset_id}.{table_id}"
        self.client.delete_table(table_id, not_found_ok=True)
        print(f"Deleted table {table_id}")

    def read_table(self, table_id : str, n_records : int = 10) -> pd.DataFrame:
        
        if self.table_exists(table_id):
            print(f"Table {table_id} exists.")
        else:
            print(f"Table {table_id} does not exist.")

        query = f"""
            SELECT *
            FROM `{self.project_id}.{self.dataset_id}.{table_id}`
            LIMIT {n_records}
        """
        df = self.client.query(query).to_dataframe()
        return df
    
    def create_table(self, table_id : str, schema : list = None) -> bigquery.Table:             
        if not self.schema and not schema: 
            raise ValueError(f"Schema must be provided and cannot be empty to create a table {table_id}.")

        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        
        table = bigquery.Table(table_ref, schema=self.schema)
        table = self.client.create_table(table, exists_ok=True)  # overwrite if exists
        print(f"Table {table.table_id} created with schema.")
        return table
    
    # def load_table_from_file_csv(self, table_id : str, input_path : str, input_file_name : str, append_new_data : bool = False) -> bigquery.LoadJob:
    def load_table_from_file_csv(self, table_id : str, input_path : str, input_file_name : str, append_new_data : bool = False) -> bigquery.LoadJob:

        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        if self.schema:
            table = self.create_table(table_id)
        else:
            table = table_ref

        job_config = self.job_config if self.job_config else self.basic_job_config

        with open(f"{input_path}{input_file_name}.csv", "rb") as source_file:
            job = self.client.load_table_from_file(source_file, table, job_config=job_config)

        job.result()
        return job
    
    def load_table_from_dataframe(self, table_id : str, df : pd.DataFrame, append_new_data : bool = False):
        if df.shape[0] == 0:
            raise ValueError(f"Provided dataframe is empty. shape: {df.shape}")
        
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        if self.schema:
            table = self.create_table(table_ref)
        else:
            table = table_ref

        job_config = self.job_config if self.job_config else self.basic_job_config

        if append_new_data:
            job_config = self.job_config_appending

        job = self.client.load_table_from_dataframe(df, table, job_config )

        job.result()
        return job
    
    def add_records_to_table(self, table_id : str, input_path : str, input_file_name : str):
        """
        Upsert new records into BigQuery table using MERGE.
        Only rows with new `etag` values are inserted.
        """

        import inspect
        print(f"[DEBUG] add_records_to_table called from {inspect.stack()[1].function}")
        # ---------- Step 0: Read CSV ----------
        input_file_path = f"{input_path}{input_file_name}.csv"

        if not os.path.exists(input_file_path):
            raise FileNotFoundError(f"The file {input_file_path} does not exist.")
        
        df = pd.read_csv(f"{input_file_path}", parse_dates=["published"])
        
        if df.empty:
            raise ValueError(f"Resulting dataframe from {input_file_path} is empty. shape: {df.shape}")

        # ---------- Step 1: Check main table ----------
        if not self.table_exists(table_id):
            raise ValueError(f"Target table: {table_id} does not exist.")
        
        main_table_name = f"{self.project_id}.{self.dataset_id}.{table_id}" 
        main_table = self.client.get_table(main_table_name)
        main_table_schema = main_table.schema

        n_initial_records = main_table.num_rows
        print(f"Main table: {main_table_name}, rows={main_table.num_rows}")

        # ---------- Step 2: Create staging table ----------
        #Avoid staging table overwrite create_table may not be dropping or truncating it first.
        #If the staging table has residual data, the MERGE will treat them all as "new". Better to use a UUID or timestamp suffix

        staging_id = f"staging_{uuid.uuid4().hex[:8]}"
        staging_table_name = f"{self.project_id}.{self.dataset_id}.{staging_id}"

        # Ensure staging table is clean
        try:
            self.client.delete_table(staging_table_name)
            print(f"Deleted old staging table {staging_table_name}")
        except NotFound:
            pass

        staging_table = self.create_table(staging_id, main_table_schema)

        # Load DataFrame into staging
        load_job = self.client.load_table_from_dataframe(df, staging_table, job_config=self.job_config_appending)
        load_job.result()
        print(f"Loaded {df.shape[0]} rows into staging table {staging_table_name}")


        # ---------- Step 3: MERGE ----------
        #Insert only rows whose etag is not present in the main table
        #In BigQuery, column names must be specified in MERGE statements

        columns = [field.name for field in main_table_schema]
        insert_cols = ", ".join(columns)
        values = ", ".join([f"S.{col}" for col in columns])
        
        if len(df.columns) != len(columns):
            raise ValueError(f"Number of columns from target table id: {main_table_name}\
                             doesnt match with the ones provided in the .csv file ({len(df.columns)}vs{len(columns)})")

        merge_sql = f"""
            MERGE `{main_table_name}` T
            USING `{staging_table_name}` S
            ON T.etag = S.etag
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({values})
        """

        query_job = self.client.query(merge_sql)
        query_job.result()

        n_final_records = self.client.get_table(main_table_name).num_rows
        print(f"Inserted {n_final_records - n_initial_records} new records into main table {main_table_name}")

        # ---------- Step 4: Cleanup staging ----------
        self.client.delete_table(staging_table_name, not_found_ok=True)
        print(f"Deleted staging table {staging_table_name}")

        return query_job

if __name__ == "__main__":

    project_id = "swiftie-friend" 
    dataset_id = "social_media"
    main_table= "youtube_music_videos_v2"

    input_path="../data/yt_musics_videos/"
    input_file_name="youtube_taylor_last_10_mv"


    schema = [
        bigquery.SchemaField("etag", "STRING"),
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("published", "TIMESTAMP"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("link", "STRING"),
        bigquery.SchemaField("thumbnails", "STRING"),
        bigquery.SchemaField("official_mv", "BOOLEAN"),
        bigquery.SchemaField("behind_scenes_mv", "BOOLEAN")
    ]


    conf = {
        "source_format": bigquery.SourceFormat.CSV,
        "skip_leading_rows": 1,   # skip header row
        "autodetect": False,       # don't let BigQuery detect schema
    }

    #test = bigquery.LoadJobConfig(**conf)

    load_bq = LoadToBigQuery(project_id, dataset_id, schema, None)


    new_table = "youtube_music_videos_v4"
    # load_bq.delete_table(new_table)
    # job = load_bq.load_table_from_file_csv(new_table, input_path, input_file_name)

    ob = load_bq.add_records_to_table(new_table, input_path, input_file_name)
    #print(load_bq.read_table(new_table, 1))

