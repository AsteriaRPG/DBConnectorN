package jp.asteria.dbconnector

import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.Transaction
import org.jetbrains.exposed.v1.core.statements.StatementContext
import org.jetbrains.exposed.v1.core.statements.GlobalStatementInterceptor
import org.jetbrains.exposed.v1.core.statements.StatementType
import org.jetbrains.exposed.v1.jdbc.JdbcTransaction
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import java.util.Collections
import java.util.WeakHashMap

/** SQL保存とORMの混在を拒否し、失敗を捕捉されても同じDBの最外保存を取り消す。 */
object SqlStoreTransactions {
    private class Guard {
        val tables = mutableSetOf<String>()
        var rejected = false
        var failed = false
    }
    // Exposed clears userData after rollback, including an automatic rollback caused by
    // a caught nested SQLException. Keep the guard for the transaction object's life.
    // Values contain no reference to their keys, so finished transactions can be collected.
    private val guards = Collections.synchronizedMap(WeakHashMap<JdbcTransaction, Guard>())
    private fun guard(scope: JdbcTransaction): Guard = synchronized(guards) {
        guards.getOrPut(scope) { Guard() }
    }
    private val writeTypes = setOf(StatementType.INSERT, StatementType.UPDATE, StatementType.DELETE)
    @Volatile private var registered = false
    private val interceptor = object : GlobalStatementInterceptor {
        override fun beforeExecution(transaction: Transaction, context: StatementContext) {
            if (context.statement.type !in writeTypes) return
            var scope = transaction as? JdbcTransaction ?: return
            val db = scope.db
            while (true) {
                val guard = guards[scope]
                if (guard != null && context.statement.targets.any { it.tableName in guard.tables }) {
                    // flushはDAOのpending値も変更するため、例外を捕捉されてもcommitさせない。
                    var rejectedScope: JdbcTransaction? = transaction as JdbcTransaction
                    while (rejectedScope != null && rejectedScope.db === db) {
                        guards[rejectedScope]?.rejected = true
                        rejectedScope = rejectedScope.outerTransaction
                    }
                    error("ORM writes cannot be mixed with protected SQL stores")
                }
                scope = scope.outerTransaction?.takeIf { it.db === db } ?: return
            }
        }

        override fun beforeCommit(transaction: Transaction) {
            var scope = transaction as? JdbcTransaction ?: return
            val db = scope.db
            while (true) {
                check(guards[scope]?.rejected != true) {
                    "Rejected ORM writes require the whole SQL transaction to roll back"
                }
                check(guards[scope]?.failed != true) {
                    "A failed SQL write requires the whole transaction to roll back"
                }
                scope = scope.outerTransaction?.takeIf { it.db === db } ?: return
            }
        }
    }

    /** DB処理の開始前に一度登録する。稼働中のプラグイン単独再ロードは対象外。 */
    @Synchronized
    fun initialize() {
        if (!registered) {
            JdbcTransaction.globalInterceptors.add(interceptor)
            registered = true
        }
    }

    fun <T> write(protectedTables: Collection<Table> = emptyList(), action: () -> T): T {
        check(registered) { "SQL transaction service is not initialized" }
        check(Database.getTransactionConnection() == null) { "SQL stores cannot join Database.transaction" }
        return transaction {
            protect(this, protectedTables)
            try {
                action()
            } catch (failure: Throwable) {
                var scope: JdbcTransaction? = this
                while (scope != null && scope.db === db) {
                    val current = scope
                    val guard = guard(current)
                    guard.failed = true
                    scope = scope.outerTransaction
                }
                throw failure
            }
        }
    }

    /** 親にも保護を残し、後発childと親commit時のflushに適用する。 */
    private fun protect(scope: JdbcTransaction, tables: Collection<Table>) {
        if (tables.isEmpty()) return
        var current: JdbcTransaction? = scope
        while (current != null && current.db === scope.db) {
            val transaction = current
            val guard = guard(transaction)
            guard.tables += tables.map { it.tableName }
            current = transaction.outerTransaction
        }
    }
}
