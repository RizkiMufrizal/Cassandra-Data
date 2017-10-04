# Cassandra-Data

Cassandra-Data is library for Testing Insert And Read Performance Cassandra Database

## How To Use

You can run the jar file using this command

```bash
java -jar Cassandra-Data-0.0.1.jar host=127.0.0.1 port=9042 portThrift=9160 username=cassandra password=cassandra keySpace=testingcassandra table=data_test totalData=50000 clusterName="Test Cluster" consistencyLevel=ONE compression=NONE
```

For Consistency Level, you can use for this options :

 * ANY
 * ONE
 * TWO
 * THREE
 * QUORUM
 * LOCAL_ONE
 * LOCAL_QUORUM
 * EACH_QUORUM
 * ALL
 
 For Compression, you can use for this options :
 
 * GZIP
 * NONE
 
 ## Author

* [Rizki Mufrizal](https://github.com/RizkiMufrizal)

## License

Cassandra-Data is Open Source software released under the Apache 2.0 license.
