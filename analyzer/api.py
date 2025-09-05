import json
import requests

class SparkRestAPI():
    def __init__(self, base_url, app_id):
        self.base_url = base_url
        self.app_id = app_id

    def get(self, endpoint: str)-> json:
        url = f"{self.base_url}/{endpoint}"
        try:
            response = requests.get(url)
            response.raise_for_status() # check response status code, if it's not 200-299 it will raise an error
        except requests.exceptions.HTTPError as err:
            print(f"HTTP error occurred: {str(err)}")
        except requests.exceptions.RequestException as err:
            print(f"Error occurred: {str(err)}")
        else:
            response_json = response.json()
            return response_json

    def get_sql(self) -> json:
        endpoint = f"api/v1/applications/{self.app_id}/sql"
        return self.get(endpoint)

    def get_stage(self) -> json:
        endpoint = f"api/v1/applications/{self.app_id}/stages?withSummaries=true&quantiles=0.0,0.25,0.5,0.75,1.0"
        return self.get(endpoint)

    def get_job(self) -> json:
        endpoint = f"api/v1/applications/{self.app_id}/jobs"
        return self.get(endpoint)