package org.rizki.mufrizal.cassandra.query

import org.apache.cassandra.thrift.*
import org.apache.thrift.TException
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TFramedTransport
import org.apache.thrift.transport.TSocket
import org.apache.thrift.transport.TTransportException
import java.io.UnsupportedEncodingException
import java.nio.ByteBuffer

/**
 *
 * @Author Rizki Mufrizal <mufrizalrizki@gmail.com>
 * @Web <https://RizkiMufrizal.github.io>
 * @Since 03 October 2017
 * @Time 10:36 PM
 * @Project Cassandra-Data
 * @Package org.rizki.mufrizal.cassandra.query
 * @File ReadCassandra
 *
 */

class ReadCassandra {
    companion object {
        @JvmStatic
        @Throws(TTransportException::class, AuthorizationException::class, TException::class, UnsupportedEncodingException::class)
        fun readDataCassandra(host: String, port: Int, username: String, password: String, totalData: Int, keySpace: String, table: String, consistencyLevel: String, compression: String): String {
            TFramedTransport(TSocket(host, port)).use { transport ->
                val protocol = TBinaryProtocol(transport)

                val client = Cassandra.Client(protocol)

                transport.open()

                val authenticationCassandra = hashMapOf<String, String>()
                authenticationCassandra.put("username", username)
                authenticationCassandra.put("password", password)
                val authenticationRequest = AuthenticationRequest(authenticationCassandra)
                client.login(authenticationRequest)

                client.set_keyspace(keySpace)

                val startTotal = System.currentTimeMillis()
                var lowLatency = 1000L
                var highLatency = 0L

                for (i in 1..totalData) {
                    val sql = "select nama from $table where key='$i'"
                    val startTime = System.currentTimeMillis()
                    client.execute_cql3_query(ByteBuffer.wrap(sql.toByteArray(charset("UTF-8"))), Compression.valueOf(compression), ConsistencyLevel.valueOf(consistencyLevel))
                    val elapsedTime = System.currentTimeMillis() - startTime
                    if (elapsedTime > highLatency) {
                        highLatency = elapsedTime
                    }
                    if (elapsedTime < lowLatency) {
                        lowLatency = elapsedTime
                    }
                }
                val elapseTime = System.currentTimeMillis() - startTotal

                transport.flush()
                transport.close()

                val elapseTimes = elapseTime / 1000
                return "[Read]Elapse time $elapseTimes s, TPS ${totalData.toLong() / elapseTimes} s, highest latency $highLatency ms, Low latency $lowLatency ms, average ${elapseTime / totalData.toLong()} ms"
            }
        }
    }
}