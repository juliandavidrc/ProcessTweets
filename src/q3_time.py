from typing import List, Tuple
from datetime import datetime
import pandas as pd
import pyspark
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json

def q3_time(file_path: str) -> List[Tuple[str, int]]:
    
    ##Read file with json.loads()   
    data = [json.loads(line) for line in open(file_path, 'r')]
    #convert to dataframe
    dfaux = pd.DataFrame(data)
    #Selection only the columns to process to enhance time and usage memory consumption
    dfres = dfaux[['content']]
    #Creating string for each content    
    """ Extract @username from content field, generate value_counts
        and replace @ symbol to retrieve username.
    """
    df = (dfres['content'].str.extractall(r'(\@\w+)')[0].value_counts()
                        .rename_axis('username').reset_index())        
    df['username']=[sub.replace('@', '') for sub in df['username']]
    
    #print(list(df.head(10).itertuples(index=False, name=None)))
    df_ret = list(df.head(10).itertuples(index=False, name=None))
    return df_ret


if __name__ == "__main__":
    file_path = "data/farmers-protest-tweets-2021-2-4.json"    
    q3_time(file_path)    