# neo4j-load-parquet

Experiments on loading parquet files

## Build

JDK 17 is required.
Ignore the tons of warnings, it's an experiment

```bash
mvn -Dfast clean package
```

### Running

#### Server side batches (default)

```bash
java -jar target/neo4j-load-parquet-1.0-SNAPSHOT.jar -abolt://localhost:7687 -uneo4j -pverysecret --label=Test ~/tmp/yellow_tripdata_2023-04.parquet
```

#### Client side batches (slow and crappy)

**NOTE**: Client side batches are implemented as a number of statements per transaction. The code is using explicit transactions, using a parameterized query object and relies on the combined driver and server infrastructure to tune this. Right now, buffering statements and then using a halfway decent sized transaction is the only way to do client side batching with the Neo4j Java Driver. If you one needs more, it's through Cypher.  

```bash
java -jar target/neo4j-load-parquet-1.0-SNAPSHOT.jar -abolt://localhost:7687 -uneo4j -pverysecret --mode=CLIENT_SIDE_BATCHING --batch-size=100 --label=Test2 ~/tmp/yellow_tripdata_2023-04.parquet
```

#### As stored procedure

This PoC can also be dropped into the `plugin` folder.
After a server restart a procedure `loadParquet` is available. 
This procedure takes one argument that should look like a URI and tries to resolve that to a Parquet file.
The URI can be a local file with or without `file://` scheme or any - by Java - supported URL. Credentials, headers etc. are not yet supported.
The stored procedure returns a map per row in the parquet file.

```cypher
CALL loadParquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet') 
YIELD row 
RETURN count(row) as numRows;
```

Or ingest all data for further processing (Note: Downloaded Parquet files are not cached and dl will add to the processing time. Use local files for benchmarking.

```cypher
CALL loadParquet('/Users/msimons/tmp/yellow_tripdata_2023-04.parquet') 
YIELD row 
CALL {
  WITH row 
  CREATE (r:Ride) SET r = row
} IN TRANSACTIONS of 100000 rows;
```
