package jp.asteria.dbconnector

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection
import javax.sql.DataSource

object Database {
    @PublishedApi
    internal lateinit var dataSource: HikariDataSource

    @PublishedApi
    internal var conn: Connection? = null

    /**
     * DBの種類
     */
    lateinit var type: DBType
        private set

    /**
     * transaction{}のブロックでのみ有効なConnectionを返す
     *
     * @return transactionスコープ外ではnullしか返ってこない
     */
    fun getTransactionConnection(): Connection? = conn

    /**
     * データソース取得
     *
     * @return
     */
    fun getDataSource(): DataSource = dataSource

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

    internal fun connect(type: DBType, url: String, driver: String = "", user: String = "", password: String = "") {
        this.type = type

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
