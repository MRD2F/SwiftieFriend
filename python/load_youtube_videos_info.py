from google.cloud import bigquery
import pandas as pd

class LoadToBigQuery:
    def __init__(self, project_id, dataset_id, schema : list = None, job_config : dict = None):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client()
        self.schema = schema

        self.job_config_appending = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"   # or "WRITE_TRUNCATE" to replace everything
        )

        self.basic_job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # skip header row
            autodetect=False,      # Dont allow BigQuery detect schema
        )
 
        self.job_config = self.return_job_config() if job_config else job_config

        # self.job_config = job_config if job_config is None else self.return_job_config()


    def return_job_config(self):
        job_config = bigquery.LoadJobConfig(**self.job_config)
        return job_config

    def read_table(self, table_id : str, n_records : int = 10) -> pd.DataFrame:
        query = f"""
        SELECT *
        FROM `{self.project_id}.{self.dataset_id}.{table_id}`
        LIMIT {n_records}
        """
        df = self.client.query(query).to_dataframe()
        return df
    
    def create_table(self, table_id : str, schema : list = None):             
        if not self.schema and not schema: 
            raise ValueError(f"Schema must be provided and cannot be empty to create a table {table_id}.")

        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        
        table = bigquery.Table(table_ref, schema=self.schema)
        table = self.client.create_table(table, exists_ok=True)  # overwrite if exists
        print(f"Table {table.table_id} created with schema.")
        return table
    
    def load_table_from_file_csv(self, table_id : str, input_path : str, input_file_name : str):
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
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        if self.schema:
            table = self.create_table(table_id)
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
        Used MERGE method for upsert (insert + update) only insert rows with new etag values 
        1) Load df into a temporary/staging table.
        2) Run a MERGE SQL statement to upsert into the target table.
        """
        df = pd.read_csv(f"{input_path}{input_file_name}.csv", parse_dates=["published"])

        table_id_new = "new_mv"
        table_name_new = f"{self.project_id}.{self.dataset_id}.{table_id_new}"  # temp dataset/table
        
        main_table_name = f"{self.project_id}.{self.dataset_id}.{table_id}" 

        main_table = self.client.get_table(main_table_name)
        print(
            "Loaded {} rows and {} columns to {}".format(
                main_table.num_rows, len(main_table.schema), main_table
            )
        )

        main_table_schema = main_table.schema

        # Step 1: Load the new DataFrame into a staging table (overwrite each time)

        table_new = self.create_table(table_name_new, main_table_schema)

        job = self.client.load_table_from_dataframe(
            df,
            table_new,
            job_config=self.job_config_appending,
        )
        job.result()

        # Step 2: Insert only rows whose etag is not present in the main table
        #In BigQuery, column names must be specified in MERGE statements

        columns = [field.name for field in main_table.schema]
        insert_cols = ", ".join(columns)
        values = ", ".join([f"S.{col}" for col in columns])
        
        merge_sql = f"""
            MERGE `{main_table_name}` T
            USING `{table_new}` S
            ON T.etag = S.etag
            WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({values})
        """

        query_job = self.client.query(merge_sql)
        query_job.result()
        print("Inserted only new rows based on etag")
        stats = query_job._properties["statistics"]["query"]

        print("Inserted:", stats.get("numDmlInsertedRowCount", 0))
        print("Updated:", stats.get("numDmlUpdatedRowCount", 0))
        print("Deleted:", stats.get("numDmlDeletedRowCount", 0))

        return query_job





from load_youtube_videos_info import LoadToBigQuery
project_id = "swiftie-friend" 
dataset_id = "social_media"
main_table= "youtube_music_videos_v2"

schema = [
    bigquery.SchemaField("etag", "STRING"),
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("published", "TIMESTAMP"),
    bigquery.SchemaField("description", "STRING"),
    bigquery.SchemaField("link", "STRING"),
    bigquery.SchemaField("thumbnails", "STRING"),
]


conf = {
    "source_format": bigquery.SourceFormat.CSV,
    "skip_leading_rows": 1,   # skip header row
    "autodetect": False,       # don't let BigQuery detect schema
}

test = bigquery.LoadJobConfig(**conf)

load_bq = LoadToBigQuery(project_id, dataset_id, schema, None)

print(load_bq.read_table(main_table, 10))