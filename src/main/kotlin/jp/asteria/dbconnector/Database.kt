package jp.asteria.dbconnector

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection
import javax.sql.DataSource

object Database {
    @PublishedApi
    internal lateinit var dataSource: HikariDataSource

    private class TransactionScope(val connection: Connection) {
        var rollbackFailure: Throwable? = null
    }
    private val scopes = ThreadLocal<TransactionScope>()

    // 旧inline呼び出しのJVMアクセサーを維持する。新しい呼び出しはtransactionへ集約する。
    @PublishedApi
    internal var conn: Connection?
        get() = scopes.get()?.connection
        set(value) {
            if (value == null) scopes.remove()
            else if (scopes.get()?.connection !== value) scopes.set(TransactionScope(value))
        }

    lateinit var type: DBType
        private set

    /** 現在のスレッドのtransaction内でだけ有効。接続をスコープ外へ持ち出さない。 */
    fun getTransactionConnection(): Connection? = scopes.get()?.connection

    fun getDataSource(): DataSource = dataSource

    /** 最外のスコープが接続を所有し、入れ子は同じ接続のセーブポイントを使う。 */
    fun <T> transaction(statement: () -> T): T {
        val parent = scopes.get()
        if (parent != null) return nested(parent, statement)

        return dataSource.connection.use { connection ->
            val scope = TransactionScope(connection)
            scopes.set(scope)
            try {
                connection.autoCommit = false
                try {
                    val result = statement()
                    scope.rollbackFailure?.let { throw IllegalStateException("Nested transaction cleanup failed", it) }
                    connection.commit()
                    result
                } catch (failure: Throwable) {
                    cleanup(failure) { connection.rollback() }
                    throw failure
                }
            } finally {
                scopes.remove()
            }
        }
    }

    private fun <T> nested(scope: TransactionScope, statement: () -> T): T {
        val connection = scope.connection
        val savepoint = connection.setSavepoint()
        try {
            val result = statement()
            connection.releaseSavepoint(savepoint)
            return result
        } catch (failure: Throwable) {
            cleanup(failure) { connection.rollback(savepoint) }?.let { scope.rollbackFailure = it }
            cleanup(failure) { connection.releaseSavepoint(savepoint) }?.let { scope.rollbackFailure = it }
            throw failure
        }
    }

    /** 後始末の失敗で元の例外を隠さない。戻り値は最外スコープのコミット抑止に使う。 */
    private fun cleanup(original: Throwable, action: () -> Unit): Throwable? = try {
        action()
        null
    } catch (failure: Throwable) {
        if (failure !== original) original.addSuppressed(failure)
        failure
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
        if (::dataSource.isInitialized) dataSource.close()
    }
}
