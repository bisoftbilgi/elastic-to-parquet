# Elastic to parquet

Elastic to parquet is command line tool to offload elasticsearch index into a parquet file

These are some usage scenarios:

```
python main.py --elasticsearchUrl https://username:password@localhost:9200/  --indexName application_logs_idex --fields field1,field2,field3.subfield1
```

Also there are some extra parameters

````
--limit : if you want to limit number of exported documents , this parameter is genrally used for testing purposes (default is unlimited)
````

```
--chunk : this parameter is used for adjusting scroll size for reading elasticsearch index (default is 1000)
```
