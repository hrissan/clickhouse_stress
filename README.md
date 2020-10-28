# clickhouse_stress

Tool to stress `clickhouse` (https://clickhouse.tech/docs/en/) by inserting lots of data.

Installation 
```
$> go get -u github.com/hrissan/clickhouse_stress
```


Stressing locally installed `clickhouse` instance using 50 connections with chunks of 200000 bytes. Will automatically create test tables in default db.
```
$> ~/go/bin/clickhouse_stress

2020/10/28 03:10:51 stats: inserts 176, inserted 33.569336 MB
2020/10/28 03:10:52 stats: inserts 331, inserted 63.133240 MB
2020/10/28 03:10:53 stats: inserts 480, inserted 91.552734 MB
2020/10/28 03:10:54 stats: inserts 631, inserted 120.353699 MB
2020/10/28 03:10:55 stats: inserts 782, inserted 149.154663 MB
2020/10/28 03:10:56 stats: inserts 941, inserted 179.481506 MB
```

Dropping created test tables from locally installed `clickhouse` instance.
```
$> ~/go/bin/clickhouse_stress -drop
```

Stressing `clickhouse` on server `my_host:13338` using 90 connections with chunks of 20 bytes.

```
$> ~/go/bin/clickhouse_stress -max-conn=90 -srv=my_host:13338 -data-size=20 
```

Stressing `clickhouse` by inserting into custom table (must be created manually and be ready to accept `-data-size` random bytes as data in `RowBinary` format). 

```
$> ~/go/bin/clickhouse_stress -table="clickhouse_stress2_buffer(time,key0,key1,key2,key3)" -data-size=200 
```

Enjoy!