package simpledb.jdbc.embdded

import simpledb.jdbc.DriverAdapter
import simpledb.server.SimpleDB
import java.sql.Connection
import java.sql.SQLException
import java.util.*

class EmbeddedDriver: DriverAdapter() {
    override fun connect(url: String?, info: Properties?): Connection {
        if (url == null) throw SQLException("url is null")
        val db = SimpleDB(url)
        return EmbeddedConnection(db)
    }
}