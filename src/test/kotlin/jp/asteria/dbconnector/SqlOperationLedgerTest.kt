package jp.asteria.dbconnector

import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.transactions.TransactionManager
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import java.nio.file.Files
import java.util.UUID
import kotlin.test.*

class SqlOperationLedgerTest {
    private fun fixture(action: () -> Unit) {
        val path = Files.createTempFile("operation-ledger-", ".db")
        val db = Database.connect("jdbc:sqlite:$path", "org.sqlite.JDBC")
        SqlStoreTransactions.initialize()
        try {
            SqlOperationLedger.initializeSchema()
            transaction { exec("CREATE TABLE debit (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)"); exec("INSERT INTO debit VALUES(1,10)") }
            action()
        } finally { TransactionManager.closeAndUnregister(db); Files.deleteIfExists(path) }
    }
    private fun balance(): Int = transaction { exec("SELECT amount FROM debit WHERE id=1") { it.next(); it.getInt(1) }!! }
    private fun debit() { TransactionManager.current().exec("UPDATE debit SET amount=amount-1 WHERE id=1") }
    private fun invoke(id: UUID, source: String = "test/1", value: Int = 1, participants: List<Int> = listOf(1),
                       action: () -> SqlOperationLedger.Outcome): SqlOperationLedger.Receipt =
        SqlStoreTransactions.write {
            SqlOperationLedger.enlist("test", id, source, CanonicalRequest(mapOf("amount" to value)), participants,
                mapOf("equipment" to "v1"), action)
        }

    @Test fun canonicalIdentityIsStableAndRejectsLossyValues() {
        val first = CanonicalRequest(mapOf("b" to listOf(null, true, 2), "a" to "日本語\n\""))
        val second = CanonicalRequest(mapOf("a" to "日本語\n\"", "b" to listOf(null, true, 2)))
        assertEquals(first.hash, second.hash)
        first.bytes().fill(0)
        assertContentEquals(first.bytes(), second.bytes())
        assertFailsWith<IllegalArgumentException> { CanonicalRequest(mapOf("n" to 1.0)) }
        assertFailsWith<IllegalArgumentException> { CanonicalRequest(mapOf("n" to "\uD800")) }
    }

    @Test fun replayByIdAndSourceNeverDebitsAgainAndReturnsOriginalBytes() = fixture {
        val id = UUID.randomUUID()
        var calls = 0
        val receipt = invoke(id) {
            calls++; debit()
            SqlOperationLedger.Outcome(SqlOperationLedger.State.COMMITTED, byteArrayOf(1, 2, 3))
        }
        receipt.resultBytes().fill(0)
        val again = invoke(id) { error("Replayed callback") }
        val otherId = invoke(UUID.randomUUID()) { error("Replayed source") }
        assertEquals(id, otherId.operationId)
        assertContentEquals(byteArrayOf(1, 2, 3), again.resultBytes())
        assertEquals(9, balance()); assertEquals(1, calls)
        assertFailsWith<SqlOperationLedger.Conflict> { invoke(id, value = 2) { error("Conflict callback") } }
        assertFailsWith<SqlOperationLedger.Conflict> { invoke(id, source = "changed") { error("Changed source") } }
        assertFailsWith<SqlOperationLedger.Conflict> { invoke(id, participants = listOf(2)) { error("Changed actor") } }
        assertEquals(9, balance())
    }

    @Test fun outcomeAndDebitRollbackTogetherAndCanBeRetried() = fixture {
        val id = UUID.randomUUID()
        assertFailsWith<IllegalStateException> {
            invoke(id) { debit(); error("Before outcome") }
        }
        assertEquals(10, balance())
        transaction { assertNull(SqlOperationLedger.query("test", id)) }
        invoke(id) { debit(); SqlOperationLedger.Outcome(SqlOperationLedger.State.COMMITTED, byteArrayOf(9)) }
        assertEquals(9, balance())
    }

    @Test fun outerRollbackAlsoRemovesResultAndRejectedOutcomeIsRetained() = fixture {
        val id = UUID.randomUUID()
        assertFailsWith<IllegalStateException> {
            transaction {
                maxAttempts = 1
                invoke(id) { debit(); SqlOperationLedger.Outcome(SqlOperationLedger.State.COMMITTED, byteArrayOf(1)) }
                error("Outer failure")
            }
        }
        assertEquals(10, balance())
        transaction { assertNull(SqlOperationLedger.query("test", id)) }
        val denied = invoke(id) { SqlOperationLedger.Outcome(SqlOperationLedger.State.REJECTED, byteArrayOf(0)) }
        assertEquals(SqlOperationLedger.State.REJECTED, denied.state)
        assertEquals(SqlOperationLedger.State.REJECTED, invoke(id) { error("Repeated denial") }.state)
        assertEquals(10, balance())
    }
}
