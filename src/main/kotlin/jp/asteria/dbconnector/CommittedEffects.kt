package jp.asteria.dbconnector

import org.jetbrains.exposed.v1.core.Key
import org.jetbrains.exposed.v1.core.Transaction
import org.jetbrains.exposed.v1.core.statements.StatementInterceptor
import org.jetbrains.exposed.v1.jdbc.JdbcTransaction
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Exposedの最外commit後に処理を予約し、取消・再試行・停止済み所有者の予約を破棄する。
 * scheduleは別の実行機会へ予約する関数を渡し、その場でactionを実行しない。
 * actionはID・不変値だけを保持し、transactionやDAOを持ち出さない。
 */
open class CommittedEffects(
    private val schedule: (() -> Unit) -> Unit,
    private val onFailure: (Throwable) -> Unit
) : AutoCloseable {
    private class Batch {
        val actions = mutableListOf<() -> Unit>()
        var finished = false
    }
    private val key = Key<Batch>()
    private val closed = AtomicBoolean(false)

    fun dispatch(action: () -> Unit) {
        if (closed.get()) return
        isolate { schedule { if (!closed.get()) isolate(action) } }
    }

    fun enqueue(transaction: JdbcTransaction, action: () -> Unit) {
        if (closed.get()) return
        var batch = transaction.getUserData(key)
        if (batch == null || batch.finished) {
            batch = Batch()
            transaction.putUserData(key, batch)
            val registered = batch
            transaction.registerInterceptor(object : StatementInterceptor {
                override fun afterCommit(transaction: Transaction) {
                    if (registered.finished) return
                    registered.finished = true
                    val pending = registered.actions.toList()
                    registered.actions.clear()
                    if (closed.get()) return
                    isolate {
                        val jdbc = transaction as JdbcTransaction
                        val parent = jdbc.outerTransaction
                        if (parent != null && parent.db === jdbc.db) {
                            pending.forEach { enqueue(parent, it) }
                        } else {
                            // Exposedのcommit callback内ではDBやDSLを実行しない。
                            schedule {
                                for (effect in pending) {
                                    if (closed.get()) break
                                    isolate(effect)
                                }
                            }
                        }
                    }
                }
                override fun afterRollback(transaction: Transaction) {
                    registered.finished = true
                    registered.actions.clear()
                }
            })
        }
        batch.actions += action
    }

    private fun isolate(action: () -> Unit) {
        try { action() } catch (failure: Throwable) {
            if (failure is VirtualMachineError || failure is ThreadDeath) throw failure
            try { onFailure(failure) } catch (reportFailure: Throwable) {
                if (reportFailure is VirtualMachineError || reportFailure is ThreadDeath) throw reportFailure
                if (reportFailure !== failure) failure.addSuppressed(reportFailure)
            }
        }
    }
    override fun close() { closed.set(true) }
}
