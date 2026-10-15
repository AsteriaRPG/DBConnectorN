package jp.asteria.dbconnector

import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.core.DatabaseConfig
import org.jetbrains.exposed.v1.jdbc.transactions.TransactionManager
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import java.nio.file.Files
import java.sql.SQLException
import kotlin.test.*

class CommittedEffectsTest {
    private lateinit var db: Database
    private lateinit var path: java.nio.file.Path
    private val queue = ArrayDeque<() -> Unit>()
    private val calls = mutableListOf<String>()
    private val failures = mutableListOf<Throwable>()
    private lateinit var effects: CommittedEffects
    @BeforeTest fun setup() {
        path=Files.createTempFile("committed-effects-", ".db")
        db=Database.connect("jdbc:sqlite:$path",driver="org.sqlite.JDBC")
        effects=CommittedEffects({ queue.addLast(it) }, { failures+=it })
        transaction(db) { exec("CREATE TABLE entries (id INT PRIMARY KEY)") }
    }
    @AfterTest fun cleanup() {
        effects.close();TransactionManager.closeAndUnregister(db);Files.deleteIfExists(path)
    }
    private fun drain() { while(queue.isNotEmpty()) queue.removeFirst()() }
    private fun count() = transaction(db) { exec("SELECT COUNT(*) FROM entries") { it.next();it.getInt(1) } }
    @Test fun executesInOrderOnlyAfterCommitAndOutsideTheAmbientTransaction() {
        transaction(db) {
            exec("INSERT INTO entries VALUES (1)")
            effects.enqueue(this) { assertNull(TransactionManager.currentOrNull());calls+="one";assertEquals(1,count()) }
            effects.enqueue(this) { calls+="two" }
            assertTrue(queue.isEmpty());assertTrue(calls.isEmpty())
        }
        assertTrue(calls.isEmpty());assertEquals(1,queue.size)
        drain();assertEquals(listOf("one","two"),calls);assertTrue(failures.isEmpty())
    }
    @Test fun outerRollbackDiscardsAllNestedEffects() {
        assertFailsWith<IllegalStateException> {
            transaction(db) {
                effects.enqueue(this) { calls+="outer" }
                transaction(db) { exec("INSERT INTO entries VALUES (1)");effects.enqueue(this) { calls+="inner" } }
                error("rollback fixture")
            }
        }
        drain();assertTrue(calls.isEmpty());assertTrue(queue.isEmpty());assertEquals(0,count())
    }
    @Test fun savepointChildCommitWaitsForTheParent() {
        TransactionManager.closeAndUnregister(db)
        db=Database.connect("jdbc:sqlite:$path",driver="org.sqlite.JDBC",databaseConfig=DatabaseConfig { useNestedTransactions=true })
        transaction(db) {
            effects.enqueue(this) { calls+="outer-before" }
            transaction(db) { effects.enqueue(this) { calls+="child" };exec("INSERT INTO entries VALUES (1)") }
            effects.enqueue(this) { calls+="outer-after" }
            assertTrue(queue.isEmpty())
        }
        drain();assertEquals(listOf("outer-before","child","outer-after"),calls);assertEquals(1,count())
    }
    @Test fun savepointRollbackDiscardsOnlyChildEffects() {
        TransactionManager.closeAndUnregister(db)
        db=Database.connect("jdbc:sqlite:$path",driver="org.sqlite.JDBC",databaseConfig=DatabaseConfig { useNestedTransactions=true })
        transaction(db) {
            effects.enqueue(this) { calls+="before" }
            assertFailsWith<IllegalArgumentException> {
                transaction(db) { effects.enqueue(this) { calls+="child" };exec("INSERT INTO entries VALUES (1)");throw IllegalArgumentException("fixture") }
            }
            effects.enqueue(this) { calls+="after" }
        }
        drain();assertEquals(listOf("before","after"),calls);assertEquals(0,count())
    }
    @Test fun parentRollbackAlsoDiscardsPromotedSavepointEffects() {
        TransactionManager.closeAndUnregister(db)
        db=Database.connect("jdbc:sqlite:$path",driver="org.sqlite.JDBC",databaseConfig=DatabaseConfig { useNestedTransactions=true })
        assertFailsWith<IllegalStateException> {
            transaction(db) {
                transaction(db) { effects.enqueue(this) { calls+="child" };exec("INSERT INTO entries VALUES (1)") }
                error("parent rollback")
            }
        }
        drain();assertTrue(calls.isEmpty());assertEquals(0,count())
    }
    @Test fun sqlRetryDispatchesOnlyTheSuccessfulAttempt() {
        var attempts=0
        transaction(db) {
            maxAttempts=2;minRetryDelay=0;maxRetryDelay=0
            val attempt=++attempts
            effects.enqueue(this) { calls+="$attempt" }
            exec("INSERT INTO entries VALUES ($attempt)")
            if(attempt==1) throw SQLException("retry fixture")
        }
        drain();assertEquals(2,attempts);assertEquals(listOf("2"),calls);assertEquals(1,count())
    }
    @Test fun effectFailuresDoNotPreventOtherEffectsOrUndoSavedData() {
        transaction(db) {
            exec("INSERT INTO entries VALUES (1)")
            effects.enqueue(this) { throw IllegalArgumentException("callback fixture") }
            effects.enqueue(this) { calls+="later" }
        }
        drain();assertEquals(listOf("later"),calls);assertEquals(1,failures.size);assertEquals(1,count())
    }
    @Test fun scheduleFailureCannotTurnACommittedTransactionIntoARetry() {
        effects=CommittedEffects({ throw IllegalArgumentException("scheduler fixture") },{ failures+=it })
        var attempts=0
        transaction(db) { attempts++;exec("INSERT INTO entries VALUES (1)");effects.enqueue(this) { calls+="never" } }
        assertEquals(1,attempts);assertEquals(1,count());assertEquals(1,failures.size);assertTrue(calls.isEmpty())
    }
    @Test fun closeInvalidatesBothQueuedAndLaterEffects() {
        transaction(db) { effects.enqueue(this) { calls+="queued" } }
        effects.close();drain()
        transaction(db) { effects.enqueue(this) { calls+="later" } }
        drain();assertTrue(calls.isEmpty());assertTrue(queue.isEmpty())
    }
    @Test fun closeDuringDispatchSuppressesTheRestOfTheBatch() {
        transaction(db) {
            effects.enqueue(this) { calls+="first";effects.close() }
            effects.enqueue(this) { calls+="second" }
        }
        drain();assertEquals(listOf("first"),calls)
    }
    @Test fun failureReporterCannotStopLaterEffectsOrUndoTheCommit() {
        effects=CommittedEffects({ queue.addLast(it) }, { error("reporter fixture") })
        val failure=IllegalArgumentException("callback fixture")
        transaction(db) {
            exec("INSERT INTO entries VALUES (1)")
            effects.enqueue(this) { throw failure }
            effects.enqueue(this) { calls+="later" }
        }
        drain();assertEquals(listOf("later"),calls);assertEquals(1,count())
        assertEquals("reporter fixture",failure.suppressed.single().message)
    }
    @Test fun runtimeNotificationsAreDelayedAndClosedWithTheSameOwner() {
        effects.dispatch { calls+="runtime" }
        assertTrue(calls.isEmpty());drain();assertEquals(listOf("runtime"),calls)
        effects.dispatch { calls+="cancelled" };effects.close();drain()
        assertEquals(listOf("runtime"),calls)
    }
    @Test fun runtimeSubscriberFailureDoesNotStopOtherNotifications() {
        effects.dispatch { error("runtime fixture") }
        effects.dispatch { calls+="later" }
        drain();assertEquals(1,failures.size);assertEquals(listOf("later"),calls)
    }

    @Test fun separateOwnersDoNotSharePendingBatchesOrCancellation() {
        val other = CommittedEffects({ queue.addLast(it) }, { failures += it })
        try {
            transaction(db) {
                effects.enqueue(this) { calls += "cancelled-owner" }
                other.enqueue(this) { calls += "other-before" }
                effects.enqueue(this) { calls += "cancelled-owner-again" }
                other.enqueue(this) { calls += "other-after" }
                assertTrue(queue.isEmpty())
            }
            effects.close()
            drain()
            assertEquals(listOf("other-before", "other-after"), calls)
            assertTrue(failures.isEmpty())
        } finally { other.close() }
    }

    @Test fun closeBeforeOuterCommitSuppressesPromotedChildReservations() {
        TransactionManager.closeAndUnregister(db)
        db = Database.connect("jdbc:sqlite:$path", driver = "org.sqlite.JDBC",
            databaseConfig = DatabaseConfig { useNestedTransactions = true })
        transaction(db) {
            exec("INSERT INTO entries VALUES (1)")
            transaction(db) { effects.enqueue(this) { calls += "child" } }
            effects.close()
            assertTrue(queue.isEmpty())
        }
        drain()
        assertTrue(calls.isEmpty())
        assertTrue(queue.isEmpty())
        assertEquals(1, count())
    }
}
