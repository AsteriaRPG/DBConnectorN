package jp.asteria.dbconnector

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection

object Database {
    @PublishedApi
    internal lateinit var dataSource: HikariDataSource

    @PublishedApi
    internal var conn: Connection? = null

    /**
     * コネクション取得
     *
     * @return transactionスコープ外ではnullしか返ってこない
     */
    fun getConnection(): Connection? = conn

    inline fun <T> transaction(statement: () -> T): T {
        try {
            conn = dataSource.connection
            conn?.autoCommit = false
            val result = statement()
            conn?.commit()
            return result
        } catch (e: Exception) {
            conn?.rollback()
            throw e
        } finally {
            conn?.autoCommit = true
            conn?.close()
            conn = null
        }
    }

    internal fun connect(url: String, driver: String = "", user: String = "", password: String = "") {
        val config = HikariConfig()
        config.jdbcUrl = url
        config.driverClassName = driver
        config.username = user
        config.password = password
        dataSource = HikariDataSource(config)
    }

    internal fun close() {
        dataSource.close()
    }
}
