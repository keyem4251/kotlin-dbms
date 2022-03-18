package simpledb.jdbc

import java.sql.*
import java.util.*
import java.util.logging.Logger

abstract class DriverAdapter: Driver {
    override fun acceptsURL(url: String): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun connect(url: String?, info: Properties?): Connection {
        throw SQLException("operation not implemented")
    }

    override fun getMajorVersion(): Int {
        return 0
    }

    override fun getMinorVersion(): Int {
        return 0
    }

    override fun getPropertyInfo(url: String?, info: Properties?): Array<DriverPropertyInfo> {
        throw NotImplementedError("not implemented")
    }

    override fun jdbcCompliant(): Boolean {
        return false
    }

    override fun getParentLogger(): Logger {
        throw SQLFeatureNotSupportedException("operation not implemented")
    }
}