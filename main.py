from elasticsearch import Elasticsearch
from pandas import DataFrame
from datetime import datetime
import warnings
import argparse
import duckdb
from jsonpath_ng import jsonpath, parse
import os



parser = argparse.ArgumentParser()
parser.add_argument("--elasticsearchUrl","-e",help="Elasticsearch URL",required=True)
parser.add_argument("--indexName","-i",help="Name of the elasticsearch index to extract",required=True)
parser.add_argument("--fields","-f",help="Fieldnames to extract (comma seperated like field1,field2)",required=True)
parser.add_argument("--limit","-l",help="Limit number of rows (0 for all rows)",default=0,required=False)
parser.add_argument("--chunk",help="Chunk size",default=1000,required=False)
parser.add_argument("--keepCSV",help="Keep CSV file",action=argparse.BooleanOptionalAction)

warnings.filterwarnings("ignore")

args = parser.parse_args()
def extractIndex(url,indexName,field_names,chunk_size,limit,keepCSV):
    print(f"{limit} {keepCSV}")
    count=0
    es = Elasticsearch([url],verify_certs=False)
    
    fields = field_names.split(",")
    _fieldPath={}
    for f in fields:
        jsonpath_expression = parse(f"$.{f}")
        _fieldPath[f] = jsonpath_expression
    print(fields)

    print("Search started", datetime.now())
    size = es.count(index=indexName)
    size = size["count"]
    if limit > 0 :
        size = limit
    print(f"Getting {size} records")
    index_offset = 0
    result = []

    print("Offset",index_offset)

    res = es.search(index=indexName, size=chunk_size,scroll = '10m',
                    fields=fields)
    scroll_id = res['_scroll_id']
    print("Scroll id",scroll_id)
    print("Search finished",datetime.now())
    hits = res["hits"]["hits"]

    for r in hits :
        count += 1
        row = {}
        for f in fields:
            match = _fieldPath[f].find(r["_source"])
            row[f] =  match[0].value
        if count > size:
            break

        result.append(row)

    df = DataFrame(result)
    df.to_csv(f"{indexName}.csv", index = False)

    if count > size :
        df.to_csv(f"{indexName}.csv", mode='a', header=False, index=False)
        duckdb.sql(
            f" copy (select * from read_csv('{indexName}.csv',AUTO_DETECT=TRUE,HEADER=TRUE,PARALLEL=TRUE)) to '{indexName}.parquet' (format 'PARQUET' )")
        print(duckdb.sql(f"select * from read_parquet('{indexName}.parquet') limit 10").fetchall())
        return # no need to scroll
    index_offset = len(result)

    while index_offset < size:
        result= []
        resp = es.scroll(
            scroll_id=scroll_id,
            scroll='10m',  # time value for search
        )

        hits = resp["hits"]["hits"]
        for r in hits:
            count += 1
            row = {}
            for f in fields:
                match = _fieldPath[f].find(r["_source"])
                row[f] = match[0].value
            result.append(row)
            if count > size:
                break
        index_offset += len(result)
        print("No of result",index_offset,datetime.now())
        df.to_csv(f"{indexName}.csv", mode='a',header=False ,index=False)
    duckdb.sql(f" copy (select * from read_csv('{indexName}.csv',AUTO_DETECT=TRUE,HEADER=TRUE,PARALLEL=TRUE)) to '{indexName}.parquet' (format 'PARQUET' )")
    if not keepCSV:
        os.remove(f"{indexName}.csv")

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    extractIndex(args.elasticsearchUrl,args.indexName,args.fields,int(args.chunk),int(args.limit),args.keepCSV)
