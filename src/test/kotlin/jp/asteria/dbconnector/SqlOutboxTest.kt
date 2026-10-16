package jp.asteria.dbconnector

import org.jetbrains.exposed.v1.core.DatabaseConfig
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.transactions.TransactionManager
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import java.nio.file.Files
import java.util.UUID
import kotlin.test.*

class SqlOutboxTest {
    private val route = SqlOutbox.Route("producer", "consumer", "profile-v1", SqlOutbox.DeliveryClass.SQL_ONCE)
    private fun fixture(nested: Boolean = false, action: () -> Unit) {
        val path = Files.createTempFile("outbox-", ".db")
        val db = Database.connect("jdbc:sqlite:$path", "org.sqlite.JDBC", databaseConfig = DatabaseConfig { useNestedTransactions = nested })
        SqlStoreTransactions.initialize()
        try {
            SqlOperationLedger.initializeSchema()
            SqlOutbox.initializeSchema()
            transaction { exec("CREATE TABLE counters (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)"); exec("INSERT INTO counters VALUES(1,10),(2,0)") }
            action()
        } finally { TransactionManager.closeAndUnregister(db); Files.deleteIfExists(path) }
    }
    private fun amount(id: Int): Int = transaction { exec("SELECT amount FROM counters WHERE id=$id") { it.next(); it.getInt(1) }!! }
    private fun sql(statement: String) { TransactionManager.current().exec(statement) }
    private fun source(route: SqlOutbox.Route = this.route, eventId: UUID = UUID.randomUUID(),
                       operationId: UUID = UUID.randomUUID(), value: Int = 1,
                       outcome: SqlOperationLedger.State = SqlOperationLedger.State.COMMITTED): SqlOutbox.Event {
        var event: SqlOutbox.Event? = null
        SqlStoreTransactions.write {
            SqlOperationLedger.enlist(route.owner, operationId, "source/$operationId", CanonicalRequest(mapOf("value" to value)),
                listOf(1), mapOf("version" to "v1")) {
                sql("UPDATE counters SET amount=amount-1 WHERE id=1")
                event = SqlOutbox.enlist(route, eventId, operationId, CanonicalRequest(mapOf("schemaVersion" to 1, "value" to value)))
                SqlOperationLedger.Outcome(outcome, byteArrayOf(1))
            }
        }
        return requireNotNull(event)
    }
    private fun consume(lease: SqlOutbox.Lease, action: () -> CanonicalRequest = {
        sql("UPDATE counters SET amount=amount+1 WHERE id=2"); CanonicalRequest(mapOf("applied" to 1))
    }): SqlOutbox.SqlDelivery = SqlStoreTransactions.write { SqlOutbox.consumeSql(lease, action) }
    private fun expire(event: SqlOutbox.Event) = transaction {
        exec("UPDATE persistence_outbox SET leaseUntilUtcMs=0 WHERE owner='${event.route.owner}' AND eventId='${event.eventId}'")
    }

    @Test fun sourceAndOutboxCommitTogetherAndPayloadIsDefensive() = fixture {
        val event = source()
        assertEquals(9, amount(1))
        event.payloadBytes().fill(0)
        val stored = transaction { requireNotNull(SqlOutbox.query(route.owner, event.eventId)) }
        assertEquals(event.payloadHash, CanonicalRequest.sha256(stored.payloadBytes()))
        transaction { assertEquals(listOf(event.eventId), SqlOutbox.candidates("consumer").map { it.eventId }) }
        val replay = SqlStoreTransactions.write {
            SqlOutbox.enlist(route, UUID.randomUUID(), event.sourceOperationId,
                CanonicalRequest(mapOf("schemaVersion" to 1, "value" to 1)))
        }
        assertEquals(event.eventId, replay.eventId)
        assertFailsWith<SqlOutbox.Conflict> { SqlStoreTransactions.write {
            SqlOutbox.enlist(route, event.eventId, event.sourceOperationId, CanonicalRequest(mapOf("value" to 99)))
        } }
    }

    @Test fun outerFailureAndRejectedSourceNeverLeaveAnEvent() = fixture {
        val operation = UUID.randomUUID(); val event = UUID.randomUUID()
        assertFailsWith<IllegalStateException> { transaction {
            maxAttempts = 1
            source(eventId = event, operationId = operation)
            error("after source result")
        } }
        assertEquals(10, amount(1))
        transaction { assertNull(SqlOutbox.query(route.owner, event)); assertNull(SqlOperationLedger.query(route.owner, operation)) }
        assertFailsWith<IllegalStateException> { source(eventId = event, operationId = operation, outcome = SqlOperationLedger.State.REJECTED) }
        assertEquals(10, amount(1))
        transaction { assertNull(SqlOutbox.query(route.owner, event)) }
    }

    @Test fun savepointEnqueueWaitsForTheRealRootOutcome() = fixture(nested = true) {
        val event = source()
        assertEquals(SqlOutbox.State.PENDING, transaction { SqlOutbox.query(route.owner, event.eventId)?.state })
        assertEquals(9, amount(1))
    }

    @Test fun aCommittedSourceCannotBeUsedToMintALaterUncoordinatedEvent() = fixture {
        val operation = UUID.randomUUID()
        SqlStoreTransactions.write {
            SqlOperationLedger.enlist(route.owner, operation, "source/$operation", CanonicalRequest(mapOf("value" to 1)),
                listOf(1), emptyMap()) { SqlOperationLedger.Outcome(SqlOperationLedger.State.COMMITTED, byteArrayOf(1)) }
        }
        val event = UUID.randomUUID()
        assertFailsWith<IllegalStateException> { SqlStoreTransactions.write {
            SqlOutbox.enlist(route, event, operation, CanonicalRequest(mapOf("value" to 1)))
        } }
        transaction { assertNull(SqlOutbox.query(route.owner, event)) }
    }

    @Test fun sqlConsumerAndInboxAndAckAreOneUnitAndReplayNeverAppliesTwice() = fixture {
        val event = source()
        val lease = requireNotNull(SqlOutbox.claim(route, event.eventId, UUID.randomUUID()))
        assertFailsWith<IllegalStateException> { consume(lease) {
            sql("UPDATE counters SET amount=amount+1 WHERE id=2"); error("consumer failed")
        } }
        assertEquals(0, amount(2))
        val first = consume(lease) as SqlOutbox.SqlDelivery.Committed
        val again = consume(lease) { error("replayed consumer") } as SqlOutbox.SqlDelivery.Committed
        assertFalse(first.replayed); assertTrue(again.replayed); assertEquals(first.resultHash, again.resultHash)
        assertEquals(1, amount(2))
        assertEquals(SqlOutbox.State.ACKED, transaction { SqlOutbox.query(route.owner, event.eventId)?.state })
    }

    @Test fun callerRollbackAfterSuccessfulConsumerRemovesItsDomainChangeAndAck() = fixture(nested = true) {
        val event = source()
        val lease = requireNotNull(SqlOutbox.claim(route, event.eventId, UUID.randomUUID()))
        assertFailsWith<IllegalStateException> { transaction {
            maxAttempts = 1
            consume(lease)
            error("outer rollback after consumer")
        } }
        assertEquals(0, amount(2))
        assertEquals(SqlOutbox.State.LEASED, transaction { SqlOutbox.query(route.owner, event.eventId)?.state })
        consume(lease)
        assertEquals(1, amount(2))
    }

    @Test fun expiredSqlLeaseCanBeReclaimedAndOldTokenCannotConsumeOrAck() = fixture {
        val event = source()
        val old = requireNotNull(SqlOutbox.claim(route, event.eventId, UUID.randomUUID()))
        assertNull(SqlOutbox.claim(route, event.eventId, UUID.randomUUID()))
        expire(event)
        val next = requireNotNull(SqlOutbox.claim(route, event.eventId, UUID.randomUUID()))
        assertFailsWith<IllegalStateException> { consume(old) { error("old worker ran") } }
        assertEquals(0, amount(2))
        consume(next)
        assertEquals(1, amount(2))
    }

    @Test fun uncertainNativeDeltaIsHeldWhileProjectionRebuildCanBeReclaimed() = fixture {
        for (delivery in listOf(SqlOutbox.DeliveryClass.NATIVE_FENCED, SqlOutbox.DeliveryClass.REBUILD_PROJECTION)) {
            val current = route.copy(eventType = delivery.name, deliveryClass = delivery)
            val event = source(current)
            val old = requireNotNull(SqlOutbox.claim(current, event.eventId, UUID.randomUUID()))
            expire(event)
            val next = SqlOutbox.claim(current, event.eventId, UUID.randomUUID())
            if (delivery == SqlOutbox.DeliveryClass.NATIVE_FENCED) {
                assertNull(next)
                assertEquals(SqlOutbox.State.HELD, transaction { SqlOutbox.query(current.owner, event.eventId)?.state })
            } else {
                assertNotNull(next)
                SqlOutbox.acknowledge(next)
            }
            assertFailsWith<IllegalStateException> { SqlOutbox.acknowledge(old) }
        }
    }

    @Test fun sameInboxIdWithDifferentPayloadIsDurablyHeldWithoutConsumerUpdate() = fixture {
        val id = UUID.randomUUID()
        val first = source(eventId = id)
        consume(requireNotNull(SqlOutbox.claim(route, first.eventId, UUID.randomUUID())))
        val secondRoute = route.copy(owner = "other-producer")
        val second = source(secondRoute, id, value = 2)
        val result = consume(requireNotNull(SqlOutbox.claim(secondRoute, second.eventId, UUID.randomUUID()))) { error("conflicting update") }
        assertEquals(SqlOutbox.SqlDelivery.Held, result)
        assertEquals(1, amount(2))
        assertEquals(SqlOutbox.State.HELD, transaction { SqlOutbox.query(secondRoute.owner, id)?.state })
    }

    @Test fun matchingPayloadFromAnotherSourceIsNotAnAcknowledgedReplay() = fixture {
        val id = UUID.randomUUID()
        val first = source(eventId = id)
        consume(requireNotNull(SqlOutbox.claim(route, first.eventId, UUID.randomUUID())))
        val other = route.copy(owner = "other-producer")
        val second = source(other, id)
        assertEquals(first.payloadHash, second.payloadHash)
        assertNotEquals(first.sourceOperationId, second.sourceOperationId)
        val result = consume(requireNotNull(SqlOutbox.claim(other, id, UUID.randomUUID()))) { error("colliding source must be held") }
        assertEquals(SqlOutbox.SqlDelivery.Held, result)
        assertEquals(1, amount(2))
        assertEquals(SqlOutbox.State.HELD, transaction { SqlOutbox.query(other.owner, id)?.state })
    }

    @Test fun corruptPayloadIsPreservedAndQuarantinedInAFreshUnit() = fixture {
        val event = source()
        transaction { exec("UPDATE persistence_outbox SET payloadBytes=X'00' WHERE eventId='${event.eventId}'") }
        val corrupt = assertFailsWith<SqlOutbox.CorruptPayload> { transaction { SqlOutbox.query(route.owner, event.eventId) } }
        assertTrue(SqlOutbox.quarantine(corrupt)); assertFalse(SqlOutbox.quarantine(corrupt))
        transaction {
            exec("SELECT state,hex(payloadBytes) FROM persistence_outbox WHERE eventId='${event.eventId}'") {
                assertTrue(it.next()); assertEquals("HELD", it.getString(1)); assertEquals("00", it.getString(2))
            }
        }
    }
}
