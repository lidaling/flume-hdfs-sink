# hdfs sink add impala table data loading logic.

### add more jar dependencies:
	
	```
	cd $FLUME_HOME/lib
	wget http://central.maven.org/maven2/org/apache/hive/hive-jdbc/1.2.1/hive-jdbc-1.2.1.jar
	wget http://central.maven.org/maven2/org/apache/hive/hive-service/1.2.1/hive-service-1.2.1.jar
	wget http://central.maven.org/maven2/org/apache/hive/hive-common/1.2.1/hive-common-1.2.1.jar
	wget http://central.maven.org/maven2/org/apache/hive/hive-metastore/1.2.1/hive-metastore-1.2.1.jar
	```

### update jar dependencies:

```
	cd $FLUME_HOME/lib
	rm httpcore*.jar httpclient*.jar
	wget http://central.maven.org/maven2/org/apache/httpcomponents/httpcore/4.3/httpcore-4.3.jar
	wget http://central.maven.org/maven2/org/apache/httpcomponents/httpclient/4.3/httpclient-4.3.jar
```

### config example:

```
	agtest.sources =rudpl
	agtest.sinks =hdfs-sink
	agtest.channels =cudpl

	agtest.sources.rudpl.type = netcat
	agtest.sources.rudpl.bind = localhost
	agtest.sources.rudpl.port = 44444

	agtest.sinks.hdfs-sink.type = hdfs
	agtest.sinks.hdfs-sink.hdfs.path = hdfs://cdh-master:8020/tmp/test1
	agtest.sinks.hdfs-sink.hdfs.fileType = DataStream
	agtest.sinks.hdfs-sink.hdfs.batchSize = 3
	# custom hdfs-impala configure
	agtest.sinks.hdfs-sink.tableLocation=/impala/tbs/test1
	agtest.sinks.hdfs-sink.tableName=default.test1
	agtest.sinks.hdfs-sink.impalaUrl=jdbc:hive2://192.168.0.94:21050/;auth=noSasl


	agtest.channels.cudpl.type = memory
	agtest.channels.cudpl.capacity = 1000
	agtest.channels.cudpl.transactionCapacity = 100

	agtest.sources.rudpl.channels = cudpl
	agtest.sinks.hdfs-sink.channel = cudpl
```

### start flume-ng


```
	bin/flume-ng agent -c conf -f conf/flume-conf.properties -name agtest &
```


    
