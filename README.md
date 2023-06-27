# neo4j-load-parquet

Experiments on loading parquet files

## Build

JDK 17 is required.
Ignore the tons of warnings, it's an experiment

```bash
mvn -Dfast clean package
```

## Server side batches (default)

```bash
java -jar target/neo4j-load-parquet-1.0-SNAPSHOT.jar -abolt://localhost:7687 -uneo4j -pverysecret --label=Test ~/tmp/yellow_tripdata_2023-04.parquet
```

## Client side batches (slow and crappy)

```bash
java -jar target/neo4j-load-parquet-1.0-SNAPSHOT.jar -abolt://localhost:7687 -uneo4j -pverysecret --mode=CLIENT_SIDE_BATCHING --batch-size=100 --label=Test2 ~/tmp/yellow_tripdata_2023-04.parquet
```
