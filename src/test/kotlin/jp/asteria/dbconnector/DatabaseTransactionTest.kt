package jp.asteria.dbconnector

import java.nio.file.Files
import java.sql.Connection
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.*

class DatabaseTransactionTest {
    private lateinit var directory: java.nio.file.Path

    @BeforeTest fun setup() {
        directory = Files.createTempDirectory("asteria-transactions-")
        Database.connect(DBType.SQLITE, "jdbc:sqlite:${directory.resolve("test.db")}", "org.sqlite.JDBC")
        Database.getDataSource().connection.use { connection ->
            connection.createStatement().use { it.execute("CREATE TABLE entries (id INTEGER PRIMARY KEY)") }
        }
    }
    @AfterTest fun cleanup() {
        Database.close()
        Files.walk(directory).use { paths -> paths.sorted(Comparator.reverseOrder()).forEach(Files::delete) }
    }
    private fun insert(id: Int) {
        val connection = assertNotNull(Database.getTransactionConnection())
        connection.prepareStatement("INSERT INTO entries (id) VALUES (?)").use { it.setInt(1,id); it.executeUpdate() }
    }
    private fun ids(): List<Int> = Database.getDataSource().connection.use { connection ->
        connection.createStatement().use { statement ->
            statement.executeQuery("SELECT id FROM entries ORDER BY id").use { result ->
                buildList { while(result.next()) add(result.getInt(1)) }
            }
        }
    }
    @Test fun successfulTransactionCommitsAndClosesItsScope() {
        lateinit var used: Connection
        assertNull(Database.getTransactionConnection())
        assertEquals("result", Database.transaction {
            used=assertNotNull(Database.getTransactionConnection()); insert(1); "result"
        })
        assertTrue(used.isClosed)
        assertNull(Database.getTransactionConnection())
        assertEquals(listOf(1),ids())
    }
    @Test fun exceptionRollsBackAndNextTransactionCanStart() {
        assertFailsWith<IllegalArgumentException> { Database.transaction { insert(1); errorArgument() } }
        assertNull(Database.getTransactionConnection()); assertEquals(emptyList(),ids())
        Database.transaction { insert(2) }; assertEquals(listOf(2),ids())
    }
    private fun errorArgument(): Nothing = throw IllegalArgumentException("fixture")

    @Test fun errorsAlsoRollBackRatherThanCommitDuringCleanup() {
        assertFailsWith<AssertionError> { Database.transaction { insert(1); throw AssertionError("fixture") } }
        assertNull(Database.getTransactionConnection()); assertEquals(emptyList(),ids())
    }
    @Test fun nestedSuccessRetainsTheOuterConnectionUntilCommit() {
        Database.transaction {
            val outer=assertNotNull(Database.getTransactionConnection()); insert(1)
            Database.transaction { assertSame(outer,Database.getTransactionConnection()); insert(2) }
            assertSame(outer,Database.getTransactionConnection()); assertFalse(outer.isClosed); insert(3)
        }
        assertEquals(listOf(1,2,3),ids()); assertNull(Database.getTransactionConnection())
    }
    @Test fun caughtNestedFailureRollsBackOnlyToItsSavepoint() {
        Database.transaction {
            val outer=assertNotNull(Database.getTransactionConnection()); insert(1)
            assertFailsWith<IllegalArgumentException> {
                Database.transaction { insert(2); errorArgument() }
            }
            assertSame(outer,Database.getTransactionConnection()); insert(3)
        }
        assertEquals(listOf(1,3),ids())
    }
    @Test fun outerRollbackAlsoRollsBackSuccessfulNestedWork() {
        assertFailsWith<IllegalArgumentException> {
            Database.transaction { insert(1); Database.transaction { insert(2) }; errorArgument() }
        }
        assertEquals(emptyList(),ids()); assertNull(Database.getTransactionConnection())
    }
    @Test fun concurrentTransactionsKeepSeparateConnections() {
        val executor=Executors.newFixedThreadPool(2)
        val barrier=CyclicBarrier(2)
        try {
            val tasks=(1..2).map {
                executor.submit<Connection> {
                    val used=Database.transaction {
                        val local=assertNotNull(Database.getTransactionConnection())
                        barrier.await(5,TimeUnit.SECONDS)
                        assertSame(local,Database.getTransactionConnection())
                        local.createStatement().use { statement -> statement.executeQuery("SELECT 1").use { assertTrue(it.next()) } }
                        local
                    }
                    assertNull(Database.getTransactionConnection()); used
                }
            }
            val connections=tasks.map { it.get(10,TimeUnit.SECONDS) }
            assertNotSame(connections[0],connections[1]); assertTrue(connections.all { it.isClosed })
        } finally { executor.shutdownNow(); executor.awaitTermination(5,TimeUnit.SECONDS) }
        assertNull(Database.getTransactionConnection())
    }
    @Test fun failedConnectionAcquisitionDoesNotLeaveATransactionScope() {
        Database.close()
        assertFails { Database.transaction { insert(1) } }
        assertNull(Database.getTransactionConnection())
    }

    @Test fun failedSavepointReleasePreventsOuterCommitWhenCaught() {
        interceptConnection { actual, method, args ->
            if (method.name == "releaseSavepoint") throw java.sql.SQLException("release fixture")
            invoke(actual, method, args)
        }
        val failure = assertFailsWith<IllegalStateException> {
            Database.transaction {
                insert(1)
                val inner = assertFailsWith<java.sql.SQLException> {
                    Database.transaction { insert(2) }
                }
                assertEquals("release fixture", inner.message)
                assertEquals("release fixture", inner.suppressed.single().message)
                insert(3)
            }
        }
        assertEquals("release fixture", failure.cause?.message)
        assertEquals(emptyList(), ids())
        assertNull(Database.getTransactionConnection())
    }

    @Test fun rollbackAndCloseFailuresDoNotHideTheOriginalFailure() {
        var injectFailures = true
        interceptConnection { actual, method, args ->
            when {
                injectFailures && method.name == "rollback" && args.isEmpty() -> throw java.sql.SQLException("rollback fixture")
                injectFailures && method.name == "close" -> {
                    actual.close()
                    throw java.sql.SQLException("close fixture")
                }
                else -> invoke(actual, method, args)
            }
        }
        val original = IllegalArgumentException("original fixture")
        val failure = assertFailsWith<IllegalArgumentException> {
            Database.transaction { insert(1); throw original }
        }
        assertSame(original, failure)
        assertEquals(listOf("rollback fixture", "close fixture"), failure.suppressed.map { it.message })
        injectFailures = false
        assertNull(Database.getTransactionConnection())
        assertEquals(emptyList(), ids())
    }

    private fun interceptConnection(action: (Connection, java.lang.reflect.Method, Array<out Any?>) -> Any?) {
        val original = Database.dataSource
        Database.dataSource = object : com.zaxxer.hikari.HikariDataSource() {
            override fun getConnection(): Connection {
                val actual = original.connection
                return java.lang.reflect.Proxy.newProxyInstance(Connection::class.java.classLoader,
                    arrayOf(Connection::class.java)) { _, method, args ->
                    action(actual, method, args ?: emptyArray())
                } as Connection
            }
            override fun close() { original.close(); super.close() }
        }
    }

    private fun invoke(actual: Connection, method: java.lang.reflect.Method, args: Array<out Any?>): Any? =
        try { method.invoke(actual, *args) }
        catch (failure: java.lang.reflect.InvocationTargetException) { throw failure.targetException }

    @Test fun failedSavepointRollbackPreventsOuterCommitEvenWhenInnerErrorIsCaught() {
        val original = Database.dataSource
        Database.dataSource = object : com.zaxxer.hikari.HikariDataSource() {
            override fun getConnection(): Connection {
                val actual = original.connection
                return java.lang.reflect.Proxy.newProxyInstance(Connection::class.java.classLoader,
                    arrayOf(Connection::class.java)) { _, method, args ->
                    if (method.name == "rollback" && args?.size == 1) throw java.sql.SQLException("savepoint rollback fixture")
                    try { method.invoke(actual, *(args ?: emptyArray())) }
                    catch (failure: java.lang.reflect.InvocationTargetException) { throw failure.targetException }
                } as Connection
            }
            override fun close() { original.close(); super.close() }
        }
        val failure = assertFailsWith<IllegalStateException> {
            Database.transaction {
                insert(1)
                val inner = assertFailsWith<IllegalArgumentException> {
                    Database.transaction { insert(2); errorArgument() }
                }
                assertIs<java.sql.SQLException>(inner.suppressed.single())
                insert(3)
            }
        }
        assertIs<java.sql.SQLException>(failure.cause)
        assertEquals(emptyList(),ids())
        assertNull(Database.getTransactionConnection())
    }

}
