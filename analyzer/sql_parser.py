



def get_scan_parquet_metrics(app_id: str):
    metrics_dict = {}
    try:
        response = requests.get(f"{SPARK_API_ENDPOINT}/{app_id}/sql")
        sql = json.loads(response.text)
        print(response.text)
        for item in sql:
            sql_id = item['id']
            metrics_dict[sql_id] = {}
            for node in item['nodes']:
                if node['nodeName'] == 'Scan parquet':
                    node_metrics = {}
                    for metric in node['metrics']:
                        node_metrics[metric['name']] = metric['value']
                    metrics_dict[sql_id] = node_metrics    
        return metrics_dict
    except Exception as e:
        print(f"Error in fetching metrics: {str(e)}")