package org.rizki.mufrizal.cassandra.connection

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session

/**
 *
 * @Author Rizki Mufrizal <mufrizalrizki@gmail.com>
 * @Web <https://RizkiMufrizal.github.io>
 * @Since 27 September 2017
 * @Time 10:02 AM
 * @Project Cassandra-Data
 * @Package org.rizki.mufrizal.cassandra.connection
 * @File CassandraDatabaseConnection
 *
 */
class CassandraDatabaseConnection {
    companion object {
        @JvmStatic
        fun connectCassandra(host: String, port: Int, username: String? = null, password: String? = null, clusterName: String? = null): Session? = Cluster.builder().withClusterName(clusterName).addContactPoint(host).withPort(port).withCredentials(username, password).build().connect()
    }
}