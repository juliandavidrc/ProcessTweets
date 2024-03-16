from typing import List, Tuple
from datetime import datetime
import pandas as pd
import pyspark
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json

def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    
    #Read file with json.loads()   
    data = [json.loads(line) for line in open(file_path, 'r')]
    #convert to dataframe
    df = pd.DataFrame(data)
    #transformation and renaming columns steps
    df["created_at"] = pd.to_datetime(df["date"]).dt.strftime('%Y-%m-%d')
    df['username'] = df['user'].apply(lambda d: d['username'])
    df['user_id'] = df['user'].apply(lambda d: d['id'])

    dfres = df.groupby(['created_at','username'])['id'].count().reset_index(name="count").sort_values("count", ascending=False) 

    print(dfres.head(10))
    return dfres


if __name__ == "__main__":
    file_path = "data/farmers-protest-tweets-2021-2-4.json"
    q1_time(file_path)