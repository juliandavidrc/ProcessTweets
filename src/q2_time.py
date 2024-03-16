from typing import List, Tuple
from datetime import datetime
import pandas as pd
import pyspark
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import emoji
import json

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    
    ##Read file with json.loads()   
    data = [json.loads(line) for line in open(file_path, 'r')]
    #convert to dataframe
    df = pd.DataFrame(data)
    #transformation and renaming columns steps    
    #df['user_id'] = df['user'].apply(lambda d: d['id'])
    #Duplicate tweets by user_id findings
    #print(df.shape)
    df=df.drop_duplicates(subset=['content','id'])
    print(df.shape)
    #dfres = df.groupby(['content','user_id'])['id'].count().reset_index(name="count").sort_values("count", ascending=False)      
    #print(dfres.query('count > 1').head(10))  
    
    
    dfres = df[['content','id']]

    text = dfres['content'].str.cat(sep='\n')    
    out = (pd.DataFrame(emoji.emoji_list(text)).value_counts('emoji')
            .rename_axis('Smiley').rename('Count').reset_index()
            .assign(Type=lambda x: x['Smiley'].apply(emoji.demojize)))
   
    print(list(out[['Smiley','Count']].head(10).itertuples(index=False, name=None)))
    


if __name__ == "__main__":
    file_path = "data/farmers-protest-tweets-2021-2-4.json"
    #file_path = "data/farmers-tweets.json"
    q2_time(file_path)