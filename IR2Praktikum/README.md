# Requirements

- a MySQL server with 'Wortschatz' database
- HBase instance

# Configuration

All configureation files are placed in ~/.conf/sql2hbase/

*hbase.properties*:
```
zooKeeperQuorum     = zookeeper1.example.com,zookeeper2.example.com,zookeeper3.example.com
zooKeeperClientPort = 2181
tablePostfix = abc # postfix that will be added to all table names
```

*sql_database.properties*:
```
dbClass = com.mysql.jdbc.Driver
dbHost = mysql.example.com
dbPort = 3306
dbName = deu_news_2012_10K
dbUser = user
dbPassword = passwd
```

# Migration

1. To migrate make sure both databases are configured an running.
2. Pick a migration class:
  - Sql2HBase --> run all migrations simultaneously
  - CooccurrenceEmigrationMangager --> cooccurrences
  - SentenceEmigrationManager --> sentences
  - WordEmigrationManager --> words
  - SourceEmigrationManager --> sources
3. Run the programm with `java -cp /PFAD_ZUR_JAR/sqlToHbase.jar MIGRATOR_CLASS`
4. Wait...

# Performance Tests

To run performance tests use the following command

`java -cp /PFAD_ZUR_JAR/sqlToHbase.jar PerformanceTests`

The resulting CSV files can be found in the jar folder.
