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

        fun finish(): List<() -> Unit>? {
            if (finished) return null
            finished = true
            val pending = actions.toList()
            actions.clear()
            return pending
        }

        fun discard() {
            finished = true
            actions.clear()
        }
    }

    private val key = Key<Batch>()
    private val closed = AtomicBoolean(false)

    fun dispatch(action: () -> Unit) {
        if (closed.get()) return
        isolate {
            schedule {
                if (!closed.get()) isolate(action)
            }
        }
    }

    fun enqueue(transaction: JdbcTransaction, action: () -> Unit) {
        if (closed.get()) return
        val existing = transaction.getUserData(key)
        val batch = if (existing == null || existing.finished) {
            registerBatch(transaction)
        } else {
            existing
        }
        batch.actions += action
    }

    private fun registerBatch(transaction: JdbcTransaction): Batch {
        val batch = Batch()
        transaction.putUserData(key, batch)
        transaction.registerInterceptor(object : StatementInterceptor {
            override fun afterCommit(transaction: Transaction) {
                val pending = batch.finish() ?: return
                if (closed.get()) return
                isolate { forwardCommittedActions(transaction as JdbcTransaction, pending) }
            }

            override fun afterRollback(transaction: Transaction) {
                batch.discard()
            }
        })
        return batch
    }

    private fun forwardCommittedActions(transaction: JdbcTransaction, pending: List<() -> Unit>) {
        val parent = transaction.outerTransaction
        if (parent != null && parent.db === transaction.db) {
            pending.forEach { enqueue(parent, it) }
            return
        }

        // Exposedのcommit callback内ではDBやDSLを実行しない。
        schedule {
            for (effect in pending) {
                if (closed.get()) break
                isolate(effect)
            }
        }
    }

    private fun isolate(action: () -> Unit) {
        try {
            action()
        } catch (failure: Throwable) {
            rethrowFatal(failure)
            reportFailure(failure)
        }
    }

    private fun reportFailure(failure: Throwable) {
        try {
            onFailure(failure)
        } catch (reportFailure: Throwable) {
            rethrowFatal(reportFailure)
            if (reportFailure !== failure) failure.addSuppressed(reportFailure)
        }
    }

    private fun rethrowFatal(failure: Throwable) {
        if (failure is VirtualMachineError || failure is ThreadDeath) throw failure
    }

    override fun close() {
        closed.set(true)
    }
}
