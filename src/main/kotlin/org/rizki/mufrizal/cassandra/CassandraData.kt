package org.rizki.mufrizal.cassandra

import org.rizki.mufrizal.cassandra.connection.CassandraDatabaseConnection
import org.rizki.mufrizal.cassandra.query.InsertCassandra
import org.rizki.mufrizal.cassandra.query.ReadCassandra

/**
 *
 * @Author Rizki Mufrizal <mufrizalrizki@gmail.com>
 * @Web <https://RizkiMufrizal.github.io>
 * @Since 03 October 2017
 * @Time 10:25 PM
 * @Project Cassandra-Data
 * @Package org.rizki.mufrizal.cassandra
 * @File CassandraData
 *
 */

class CassandraData {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val host = args[0].split("=")[1]
            val port = args[1].split("=")[1].toInt()
            val portThrift = args[2].split("=")[1].toInt()
            val username = args[3].split("=")[1]
            val password = args[4].split("=")[1]

            val keySpace = args[5].split("=")[1]
            val table = args[6].split("=")[1]
            val totalData = args[7].split("=")[1].toInt()
            val clusterName = args[8].split("=")[1]
            val consistencyLevel = args[9].split("=")[1]
            val compression = args[10].split("=")[1]

            val session = CassandraDatabaseConnection.connectCassandra(host = host, port = portThrift, username = username, password = password, clusterName = clusterName)
            session?.execute("CREATE TABLE IF NOT EXISTS $keySpace.$table (\n" +
                    "    key text,\n" +
                    "    id text,\n" +
                    "    nama text,\n" +
                    "    PRIMARY KEY (key)\n" +
                    ") WITH read_repair_chance = 0.0\n" +
                    "   AND dclocal_read_repair_chance = 0.1\n" +
                    "   AND gc_grace_seconds = 864000\n" +
                    "   AND bloom_filter_fp_chance = 0.01\n" +
                    "   AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }\n" +
                    "   AND comment = ''\n" +
                    "   AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy' }\n" +
                    "   AND compression = { 'sstable_compression' : 'org.apache.cassandra.io.compress.LZ4Compressor' }\n" +
                    "   AND default_time_to_live = 0\n" +
                    "   AND speculative_retry = '99.0PERCENTILE'\n" +
                    "   AND min_index_interval = 128\n" +
                    "   AND max_index_interval = 2048;")
            session?.execute("CREATE INDEX IF NOT EXISTS ${table}_id_idx ON $keySpace.$table (id);")
            println("Success Create Table")

            session?.execute("TRUNCATE $keySpace.$table")
            println("Success Deleted From Table")

            session?.close()
            session?.cluster?.close()

            val performanceInsert = InsertCassandra.insertDataCassandra(host = host, port = port, username = username, password = password, keySpace = keySpace, table = table, totalData = totalData, consistencyLevel = consistencyLevel, compression = compression)
            val performanceRead = ReadCassandra.readDataCassandra(host = host, port = port, username = username, password = password, keySpace = keySpace, table = table, totalData = totalData, consistencyLevel = consistencyLevel, compression = compression)

            println(performanceInsert)
            println(performanceRead)
        }
    }
}