
from api_extract import APIExtractor
from bq_load import BQLoader


def run():
    # Inicializamos el extractor de la API
    api_extractor = APIExtractor(
        api_url="https://jsonplaceholder.typicode.com/posts"
    )

    # Descargamos los datos de la API (100 registros aprox.)
    data = api_extractor.fetch_data()

    # Inicializamos el cargador de BigQuery
    bq_loader = BQLoader(
        project_id="my-gcp-project",
        dataset_id="SANDBOX_prueba_alten",
        table_id="api_data"
    )

    # Cargamos los datos en BigQuery
    bq_loader.load_data(data)


if __name__ == "__main__":
    run()
