import requests


class APIExtractor:
    def __init__(self, api_url: str):
        self.api_url = api_url

    def fetch_data(self) -> list:
        response = requests.get(self.api_url)
        response.raise_for_status()
        return response.json()
