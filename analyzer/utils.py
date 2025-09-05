import requests
import json
import time

from pyspark.sql import SparkSession

# Spark UIs API endpoint, 'local' is Spark's default master URL, replace 'local' with your Spark master
SPARK_API_ENDPOINT = "http://localhost:4041/api/v1/applications"


def get_spark_app_id(spark: SparkSession) -> str:
    try:
        return spark.sparkContext.applicationId
    except Exception as e:
        print(f"Error in fetching Spark Apps: {str(e)}")
        

        
def sizeof_fmt(num: float, suffix: str = "B") -> str:
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f} {unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def reverse_sizeof_fmt(size_string: str, suffix: str = "B") -> float:
    units = {"": 0, "Ki": 1, "Mi": 2, "Gi": 3, "Ti": 4, "Pi": 5, "Ei": 6, "Zi": 7, "Yi": 8}
    
    # Boşlukla ayrılmış son iki değeri al
    size, unit = size_string.split()[-2:]
    size = float(size)

    # İlgili çarpanı bulmak için sözlükten ilgili uniti al
    power = units[unit.replace(suffix, '')]

    # İlgili çarpan kadar 1024 ile çarp
    return size * (1024 ** power)


import numpy as np

def detect_anomalies(data, m=1.4):
    data_min = min(data)
    data_range = max(data) - data_min
    if data_range == 0:  # if all data points are the same
        return False
    else:
        scaled_data = [(item - data_min) / data_range for item in data]

        # Detect outliers
        mean = np.mean(scaled_data)
        std_dev = np.std(scaled_data)

        return not any((abs(mean - value) > m * std_dev) for value in scaled_data)