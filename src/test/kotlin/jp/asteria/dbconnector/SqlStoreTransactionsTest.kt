package jp.asteria.dbconnector

import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.transactions.TransactionManager
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import java.nio.file.Files
import kotlin.test.*

class SqlStoreTransactionsTest {
    private object Entries : Table("entries") { val id = integer("id") }
    private object Other : Table("other_entries") { val id = integer("id") }
    private class Failure : Error()

    private fun fixture(nested: Boolean = false, action: (Database) -> Unit) {
        val path = Files.createTempFile("sql-store-", ".db")
        val db = Database.connect("jdbc:sqlite:$path", "org.sqlite.JDBC", databaseConfig = org.jetbrains.exposed.v1.core.DatabaseConfig { useNestedTransactions = nested })
        SqlStoreTransactions.initialize()
        try {
            transaction(db) {
                exec("CREATE TABLE entries (id INTEGER PRIMARY KEY)")
                exec("CREATE TABLE other_entries (id INTEGER PRIMARY KEY)")
            }
            action(db)
        } finally { TransactionManager.closeAndUnregister(db); Files.deleteIfExists(path) }
    }
    private fun count(db: Database, table: String) = transaction(db) {
        exec("SELECT COUNT(*) FROM $table") { it.next(); it.getInt(1) }
    }
    private fun rawInsert(value: Int) {
        val connection = TransactionManager.current().connection.connection as java.sql.Connection
        connection.prepareStatement("INSERT INTO entries VALUES (?)").use { it.setInt(1, value); it.executeUpdate() }
    }
    @Test fun protectedSqlCommitsAndUnrelatedOrmIsAllowed() = fixture { db ->
        transaction(db) {
            SqlStoreTransactions.write(listOf(Entries)) { rawInsert(1) }
            Other.insert { it[id] = 1 }
        }
        assertEquals(1, count(db, "entries")); assertEquals(1, count(db, "other_entries"))
    }
    @Test fun caughtOrmRejectionStillRollsBackEarlierSql() = fixture { db ->
        assertFailsWith<IllegalStateException> { transaction(db) {
            maxAttempts = 1
            SqlStoreTransactions.write(listOf(Entries)) { rawInsert(1) }
            assertFailsWith<IllegalStateException> { Entries.insert { it[id] = 2 } }
        } }
        assertEquals(0, count(db, "entries"))
    }
    @Test fun caughtErrorInSavepointRejectsTheWholeParent() = fixture(nested = true) { db ->
        assertFailsWith<IllegalStateException> { transaction(db) {
            maxAttempts = 1
            rawInsert(1)
            assertFailsWith<Failure> { SqlStoreTransactions.write { rawInsert(2); throw Failure() } }
        } }
        assertEquals(0, count(db, "entries"))
    }
    @Test fun protectionDoesNotLeakIntoANewTransaction() = fixture { db ->
        SqlStoreTransactions.write(listOf(Entries)) { rawInsert(1) }
        transaction(db) { Entries.insert { it[id] = 2 } }
        assertEquals(2, count(db, "entries"))
    }
    @Test fun failureInAnotherDatabaseDoesNotPoisonTheOuterDatabase() = fixture { first ->
        fixture { second ->
            transaction(first) {
                rawInsert(1)
                assertFailsWith<Failure> { transaction(second) {
                    SqlStoreTransactions.write(listOf(Entries)) { rawInsert(2); throw Failure() }
                } }
            }
            assertEquals(1, count(first, "entries")); assertEquals(0, count(second, "entries"))
        }
    }
    @Test fun caughtNestedSqlExceptionCannotClearTheFailureGuardDuringRollback() = fixture { db ->
        transaction(db) {
            exec("CREATE TRIGGER reject_entry BEFORE INSERT ON entries WHEN NEW.id=2 BEGIN SELECT RAISE(ABORT,'entry fault'); END")
        }
        val failure = assertFailsWith<IllegalStateException> { transaction(db) {
            maxAttempts = 1
            rawInsert(1)
            val fault = assertFailsWith<java.sql.SQLException> {
                SqlStoreTransactions.write(listOf(Entries)) { rawInsert(2) }
            }
            assertEquals(19, fault.errorCode)
            assertTrue(fault.message.orEmpty().contains("entry fault"))
            rawInsert(3)
        } }
        assertEquals("A failed SQL write requires the whole transaction to roll back", failure.message)
        assertEquals(0, count(db, "entries"))
    }
    @Test fun explicitRollbackDoesNotRemoveProtectionFromALaterChild() = fixture(nested = true) { db ->
        assertFailsWith<IllegalStateException> { transaction(db) {
            maxAttempts = 1
            SqlStoreTransactions.write(listOf(Entries)) { rawInsert(1) }
            rollback()
            transaction(db) { Entries.insert { it[id] = 2 } }
        } }
        assertEquals(0, count(db, "entries"))
    }

}
