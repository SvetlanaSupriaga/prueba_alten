from google.cloud import bigquery


class BQLoader:
    def __init__(self, project_id: str, dataset_id: str, table_id: str):
        """
        Inicializa el cliente de BigQuery y define el destino de carga.
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.client = bigquery.Client(project=project_id)

    def load_data(self, data: list):
        """
        Carga una lista de registros JSON en una tabla de BigQuery.
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"

        job = self.client.load_table_from_json(
            data,
            table_ref,
            write_disposition="WRITE_APPEND"
        )

        job.result()
