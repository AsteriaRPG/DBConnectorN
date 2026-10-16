package jp.asteria.dbconnector

import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.transactions.TransactionManager
import java.sql.Connection
import java.sql.ResultSet
import java.util.UUID

/** 配送は権利の原本ではない。SQL消費はinbox、nativeは各ownerの復旧契約を使う。 */
object SqlOutbox {
    enum class DeliveryClass { SQL_ONCE, REBUILD_PROJECTION, NATIVE_FENCED }
    enum class State { PENDING, LEASED, ACKED, HELD }

    data class Route(val owner: String, val destination: String, val eventType: String, val deliveryClass: DeliveryClass) {
        init {
            for (value in listOf(owner, destination, eventType)) {
                require(value.isNotBlank() && value.length <= 128 && value.all { it.code in 33..126 })
            }
        }
    }
    class Event internal constructor(val route: Route, val eventId: UUID, val sourceOperationId: UUID,
                                     bytes: ByteArray, val payloadHash: String, val state: State,
                                     val leaseEpoch: UUID?, val leaseUntilUtcMs: Long?,
                                     val attempt: Long, val revision: Long) {
        private val payload = bytes.clone()
        fun payloadBytes(): ByteArray = payload.clone()
    }
    class Lease internal constructor(val event: Event, val epoch: UUID)
    sealed interface SqlDelivery {
        data class Committed(val resultHash: String, val replayed: Boolean) : SqlDelivery
        data object Held : SqlDelivery
    }
    class Conflict : IllegalStateException("Outbox identity conflicts with an existing event")
    class CorruptPayload internal constructor(val owner: String, val eventId: UUID, val revision: Long) :
        IllegalStateException("Corrupt outbox payload requires quarantine")

    fun initializeSchema() = SqlStoreTransactions.write { SchemaUtils.create(OutboxEvents, InboxReceipts) }

    /** 必ずSqlOperationLedgerのaction内。結果と別transactionから後付け生成しない。 */
    fun enlist(route: Route, eventId: UUID, sourceOperationId: UUID, payload: CanonicalRequest): Event {
        requireOuter()
        return SqlStoreTransactions.write(listOf(OutboxEvents)) {
            val existing = findIdentity(route, eventId, sourceOperationId)
            if (existing.isNotEmpty()) {
                val stored = existing.singleOrNull() ?: throw Conflict()
                if (stored.route != route || stored.sourceOperationId != sourceOperationId ||
                    stored.payloadHash != payload.hash || !stored.payloadBytes().contentEquals(payload.bytes())) throw Conflict()
                return@write stored
            }
            check(operationState(route.owner, sourceOperationId) == "RECEIVED") { "Enqueue inside the source operation action" }
            connection().prepareStatement("INSERT INTO persistence_outbox (owner,eventId,sourceOperationId,destination,eventType,payloadBytes,payloadHash,deliveryClass,state,attempt,revision) VALUES (?,?,?,?,?,?,?,?,'PENDING',0,0)").use {
                it.setString(1, route.owner)
                it.setString(2, eventId.toString())
                it.setString(3, sourceOperationId.toString())
                it.setString(4, route.destination)
                it.setString(5, route.eventType)
                it.setBytes(6, payload.bytes())
                it.setString(7, payload.hash)
                it.setString(8, route.deliveryClass.name)
                check(it.executeUpdate() == 1)
            }
            SqlStoreTransactions.checkBeforeCommit {
                check(operationState(route.owner, sourceOperationId) == "COMMITTED") { "Outbox source did not commit a successful outcome" }
            }
            requireNotNull(query(route.owner, eventId))
        }
    }

    /** poll候補だけを返す。claimのCASが取得を確定する。 */
    fun candidates(destination: String, limit: Int = 32): List<Event> {
        require(limit in 1..256)
        return connection().prepareStatement("SELECT * FROM persistence_outbox WHERE destination=? AND (state='PENDING' OR (state='LEASED' AND leaseUntilUtcMs<=?)) ORDER BY owner,eventId LIMIT ?").use {
            it.setString(1, destination)
            it.setLong(2, now())
            it.setInt(3, limit)
            it.executeQuery().use { rows -> buildList { while (rows.next()) add(decode(rows)) } }
        }
    }

    fun query(owner: String, eventId: UUID): Event? = connection().prepareStatement(
        "SELECT * FROM persistence_outbox WHERE owner=? AND eventId=?"
    ).use {
        it.setString(1, owner)
        it.setString(2, eventId.toString())
        it.executeQuery().use { rows -> if (rows.next()) decode(rows) else null }
    }

    /** 返ったleaseも外側commit前は未確定。期限切れnative加算は再配送せず保留する。 */
    fun claim(route: Route, eventId: UUID, workerEpoch: UUID, leaseMs: Long = 5000): Lease? {
        require(leaseMs in 1..60000)
        return SqlStoreTransactions.write(listOf(OutboxEvents)) {
            val before = query(route.owner, eventId) ?: return@write null
            check(before.route == route) { "Handler route changed" }
            val clock = now()
            if (before.state !in setOf(State.PENDING, State.LEASED)) return@write null
            if (before.state == State.LEASED) {
                if (requireNotNull(before.leaseUntilUtcMs) > clock) return@write null
                if (route.deliveryClass == DeliveryClass.NATIVE_FENCED) {
                    transition(before, State.HELD)
                    return@write null
                }
            }
            connection().prepareStatement("UPDATE persistence_outbox SET state='LEASED',leaseEpoch=?,leaseUntilUtcMs=?,attempt=?,revision=? WHERE owner=? AND eventId=? AND revision=?").use {
                it.setString(1, workerEpoch.toString())
                it.setLong(2, Math.addExact(clock, leaseMs))
                it.setLong(3, Math.addExact(before.attempt, 1))
                it.setLong(4, Math.addExact(before.revision, 1))
                it.setString(5, route.owner)
                it.setString(6, eventId.toString())
                it.setLong(7, before.revision)
                if (it.executeUpdate() != 1) return@write null
            }
            Lease(requireNotNull(query(route.owner, eventId)), workerEpoch)
        }
    }

    /** 呼出元が参加者を全てlockしたSQL UOW内で消費し、callback・inbox・ackをまとめる。 */
    fun consumeSql(lease: Lease, action: () -> CanonicalRequest): SqlDelivery {
        requireOuter()
        require(lease.event.route.deliveryClass == DeliveryClass.SQL_ONCE)
        return SqlStoreTransactions.write(listOf(OutboxEvents, InboxReceipts)) {
            val event = requireNotNull(query(lease.event.route.owner, lease.event.eventId))
            requireSamePayload(event, lease.event)
            val inbox = inbox(event)
            if (inbox != null && (!inbox.belongsTo(event) || inbox.payloadHash != event.payloadHash || inbox.state == "HELD")) {
                markInboxHeld(event)
                if (event.state != State.HELD) transition(event, State.HELD)
                return@write SqlDelivery.Held
            }
            if (event.state == State.ACKED) {
                check(inbox != null) { "Acknowledged SQL delivery has no inbox receipt" }
                return@write SqlDelivery.Committed(inbox.resultHash, true)
            }
            requireCurrent(lease, event)
            if (inbox != null) {
                transition(event, State.ACKED)
                return@write SqlDelivery.Committed(inbox.resultHash, true)
            }
            val result = action()
            // 長いSQL処理中に失効した場合、domainの更新も取り消して再計画する。
            requireCurrent(lease, requireNotNull(query(event.route.owner, event.eventId)))
            connection().prepareStatement("INSERT INTO persistence_inbox_receipts (destination,eventId,sourceOwner,sourceOperationId,eventType,payloadHash,resultHash,state,createdUtcMs) VALUES (?,?,?,?,?,?,?,'COMMITTED',?)").use {
                it.setString(1, event.route.destination)
                it.setString(2, event.eventId.toString())
                it.setString(3, event.route.owner)
                it.setString(4, event.sourceOperationId.toString())
                it.setString(5, event.route.eventType)
                it.setString(6, event.payloadHash)
                it.setString(7, result.hash)
                it.setLong(8, now())
                check(it.executeUpdate() == 1)
            }
            transition(event, State.ACKED)
            SqlDelivery.Committed(result.hash, false)
        }
    }

    /** nativeの置換観測またはownerの永続結果を確認してから使う。加算callbackを自動再実行しない。 */
    fun acknowledge(lease: Lease) = SqlStoreTransactions.write(listOf(OutboxEvents)) {
        require(lease.event.route.deliveryClass != DeliveryClass.SQL_ONCE)
        val event = requireNotNull(query(lease.event.route.owner, lease.event.eventId))
        requireCurrent(lease, event)
        transition(event, State.ACKED)
    }

    fun hold(lease: Lease) = SqlStoreTransactions.write(listOf(OutboxEvents)) {
        val event = requireNotNull(query(lease.event.route.owner, lease.event.eventId))
        requireCurrent(lease, event)
        transition(event, State.HELD)
    }

    /** 検査失敗した読取UOWを終了してから、正常な別UOWで呼ぶ。原bytesは保持する。 */
    fun quarantine(failure: CorruptPayload): Boolean = SqlStoreTransactions.write(listOf(OutboxEvents)) {
        connection().prepareStatement("UPDATE persistence_outbox SET state='HELD',revision=revision+1 WHERE owner=? AND eventId=? AND revision=?").use {
            it.setString(1, failure.owner)
            it.setString(2, failure.eventId.toString())
            it.setLong(3, failure.revision)
            it.executeUpdate() == 1
        }
    }

    private data class Inbox(val sourceOwner: String, val sourceOperationId: String, val eventType: String,
                             val payloadHash: String, val resultHash: String, val state: String) {
        fun belongsTo(event: Event): Boolean = sourceOwner == event.route.owner &&
            sourceOperationId == event.sourceOperationId.toString() && eventType == event.route.eventType
    }
    private fun inbox(event: Event): Inbox? = connection().prepareStatement("SELECT * FROM persistence_inbox_receipts WHERE destination=? AND eventId=?").use {
        it.setString(1, event.route.destination)
        it.setString(2, event.eventId.toString())
        it.executeQuery().use { rows ->
            if (!rows.next()) null else Inbox(rows.getString("sourceOwner"), rows.getString("sourceOperationId"),
                rows.getString("eventType"), rows.getString("payloadHash"), rows.getString("resultHash"), rows.getString("state"))
        }
    }
    private fun markInboxHeld(event: Event) {
        connection().prepareStatement("UPDATE persistence_inbox_receipts SET state='HELD' WHERE destination=? AND eventId=?").use {
            it.setString(1, event.route.destination)
            it.setString(2, event.eventId.toString())
            check(it.executeUpdate() == 1)
        }
    }
    private fun transition(before: Event, state: State) {
        connection().prepareStatement("UPDATE persistence_outbox SET state=?,revision=? WHERE owner=? AND eventId=? AND revision=?").use {
            it.setString(1, state.name)
            it.setLong(2, Math.addExact(before.revision, 1))
            it.setString(3, before.route.owner)
            it.setString(4, before.eventId.toString())
            it.setLong(5, before.revision)
            check(it.executeUpdate() == 1) { "Outbox lease was replaced" }
        }
    }
    private fun requireCurrent(lease: Lease, event: Event) {
        requireSamePayload(event, lease.event)
        check(event.state == State.LEASED && event.leaseEpoch == lease.epoch && event.revision == lease.event.revision &&
            requireNotNull(event.leaseUntilUtcMs) > now()) { "Outbox lease is not current" }
    }
    private fun requireSamePayload(a: Event, b: Event) {
        check(a.route == b.route && a.sourceOperationId == b.sourceOperationId && a.payloadHash == b.payloadHash &&
            a.payloadBytes().contentEquals(b.payloadBytes())) { "Outbox payload changed" }
    }
    private fun findIdentity(route: Route, eventId: UUID, operation: UUID): List<Event> = connection().prepareStatement(
        "SELECT * FROM persistence_outbox WHERE owner=? AND (eventId=? OR (sourceOperationId=? AND destination=? AND eventType=?))"
    ).use {
        it.setString(1, route.owner)
        it.setString(2, eventId.toString())
        it.setString(3, operation.toString())
        it.setString(4, route.destination)
        it.setString(5, route.eventType)
        it.executeQuery().use { rows -> buildList { while (rows.next()) add(decode(rows)) } }
    }
    private fun decode(rows: ResultSet): Event {
        val payload = rows.getBytes("payloadBytes")
        if (payload.size > CanonicalRequest.MAX_BYTES || CanonicalRequest.sha256(payload) != rows.getString("payloadHash")) {
            throw CorruptPayload(rows.getString("owner"), UUID.fromString(rows.getString("eventId")), rows.getLong("revision"))
        }
        return Event(Route(rows.getString("owner"), rows.getString("destination"), rows.getString("eventType"),
            DeliveryClass.valueOf(rows.getString("deliveryClass"))), UUID.fromString(rows.getString("eventId")),
            UUID.fromString(rows.getString("sourceOperationId")), payload, rows.getString("payloadHash"), State.valueOf(rows.getString("state")),
            rows.getString("leaseEpoch")?.let(UUID::fromString), rows.getObject("leaseUntilUtcMs")?.let { rows.getLong("leaseUntilUtcMs") },
            rows.getLong("attempt"), rows.getLong("revision"))
    }
    private fun operationState(owner: String, id: UUID): String? = connection().prepareStatement("SELECT state FROM persistence_operations WHERE owner=? AND operationId=?").use {
        it.setString(1, owner)
        it.setString(2, id.toString())
        it.executeQuery().use { rows -> if (rows.next()) rows.getString(1) else null }
    }
    private fun now(): Long {
        val query = when (connection().metaData.databaseProductName) {
            "SQLite" -> "SELECT CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)"
            "MySQL", "MariaDB" -> "SELECT CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS SIGNED)"
            else -> error("Unsupported outbox database")
        }
        return connection().createStatement().use { statement ->
            statement.executeQuery(query).use { rows ->
                check(rows.next())
                rows.getLong(1)
            }
        }
    }
    private fun requireOuter() { check(TransactionManager.currentOrNull() != null) { "An outer SQL UOW is required" } }
    private fun connection(): Connection = TransactionManager.current().connection.connection as Connection
}
