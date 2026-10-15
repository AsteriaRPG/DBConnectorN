package jp.asteria.dbconnector

import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.transactions.TransactionManager
import java.sql.Connection
import java.util.UUID

/** SQL結果の再送判定。ゲームルールやnative在庫へは依存しない。 */
object SqlOperationLedger {
    private object Operations : Table("persistence_operations") {
        val owner = varchar("owner", 128)
        val operationId = varchar("operationId", 36)
        val sourceId = varchar("sourceId", 256)
        val requestHash = varchar("requestHash", 64)
        val requestBytes = blob("requestBytes")
        val participants = text("participants")
        val definitionVersions = text("definitionVersions")
        val state = varchar("state", 16)
        val preparedResultBytes = blob("preparedResultBytes").nullable()
        val resultHash = varchar("resultHash", 64).nullable()
        val resultBytes = blob("resultBytes").nullable()
        val revision = long("revision")
        val committedUtcMs = long("committedUtcMs").nullable()
        override val primaryKey = PrimaryKey(owner, operationId)
        init { uniqueIndex(owner, sourceId) }
    }

    fun initializeSchema() = SqlStoreTransactions.write { SchemaUtils.create(Operations) }

    class Receipt internal constructor(val operationId: UUID, val sourceId: String, val requestHash: String,
                                        val state: State, result: ByteArray) {
        private val result = result.clone()
        fun resultBytes(): ByteArray = result.clone()
    }
    enum class State { COMMITTED, REJECTED, HELD }
    class Conflict : IllegalStateException("Operation identity conflicts with an existing request")

    /**
     * PlayerTransactions等の外側SQL UOWから呼ぶ。再送ではactionを呼ばない。
     * 返ったReceiptも外側commit前は未確定。native操作はactionで行わない。
     * 同時の初回要求でunique制約に競合した場合は全体をrollbackし、同じ要求を再照会する。
     */
    fun enlist(owner: String, operationId: UUID, sourceId: String, request: CanonicalRequest,
               participants: List<Int>, definitionVersions: Map<String, String>, action: () -> Outcome): Receipt {
        check(TransactionManager.currentOrNull() != null) { "An outer SQL UOW is required" }
        require(owner.isNotBlank() && owner.length <= 128 && owner.all { it.code in 33..126 })
        require(sourceId.isNotBlank() && sourceId.toByteArray(Charsets.UTF_8).size <= 256)
        require(participants.isNotEmpty() && participants.all { it >= 0 } && participants == participants.distinct().sorted())
        return SqlStoreTransactions.write(listOf(Operations)) {
            // 再送を先に確定する。現在の装備・残高・revisionは過去の結果を変更しない。
            val expectedParticipants = participants.joinToString(",", "[", "]")
            val expectedDefinitions = String(CanonicalRequest(definitionVersions).bytes(), Charsets.UTF_8)
            val matches = find(owner, operationId, sourceId, lock = true)
            if (matches.isNotEmpty()) {
                if (matches.size != 1 || matches.single().requestHash != request.hash ||
                    !matches.single().request.contentEquals(request.bytes()) ||
                    matches.single().participants != expectedParticipants || matches.single().definitions != expectedDefinitions ||
                    (matches.single().receipt.operationId == operationId && matches.single().receipt.sourceId != sourceId)) throw Conflict()
                return@write matches.single().receipt
            }
            val conn = connection()
            conn.prepareStatement("INSERT INTO persistence_operations (owner,operationId,sourceId,requestHash,requestBytes,participants,definitionVersions,state,revision) VALUES (?,?,?,?,?,?,?,'RECEIVED',0)").use { sql ->
                sql.setString(1, owner)
                sql.setString(2, operationId.toString())
                sql.setString(3, sourceId)
                sql.setString(4, request.hash)
                sql.setBytes(5, request.bytes())
                sql.setString(6, expectedParticipants)
                sql.setString(7, expectedDefinitions)
                check(sql.executeUpdate() == 1)
            }
            val outcome = action()
            val result = outcome.bytes()
            conn.prepareStatement("UPDATE persistence_operations SET state=?,resultBytes=?,resultHash=?,revision=1,committedUtcMs=? WHERE owner=? AND operationId=? AND state='RECEIVED'").use { sql ->
                sql.setString(1, outcome.state.name)
                sql.setBytes(2, result)
                sql.setString(3, CanonicalRequest.sha256(result))
                sql.setLong(4, System.currentTimeMillis())
                sql.setString(5, owner)
                sql.setString(6, operationId.toString())
                check(sql.executeUpdate() == 1)
            }
            Receipt(operationId, sourceId, request.hash, outcome.state, result)
        }
    }

    class Outcome(val state: State, bytes: ByteArray) {
        private val value = bytes.clone().also { require(it.size <= CanonicalRequest.MAX_BYTES) }
        fun bytes(): ByteArray = value.clone()
    }

    private class Stored(val receipt: Receipt, val request: ByteArray, val participants: String, val definitions: String) {
        val requestHash: String get() = receipt.requestHash
    }

    /** 独立したreadトランザクションからの結果照会にも使える。DB不明は呼出元がHELDにする。 */
    fun query(owner: String, operationId: UUID): Receipt? = find(owner, operationId, null, false).singleOrNull()?.receipt

    private fun find(owner: String, operationId: UUID, sourceId: String?, lock: Boolean): List<Stored> {
        val suffix = if (lock && connection().metaData.databaseProductName in setOf("MySQL", "MariaDB")) " FOR UPDATE" else ""
        val predicate = if (sourceId == null) "operationId=?" else "(operationId=? OR sourceId=?)"
        return connection().prepareStatement("SELECT * FROM persistence_operations WHERE owner=? AND $predicate$suffix").use { sql ->
            sql.setString(1, owner)
            sql.setString(2, operationId.toString())
            if (sourceId != null) sql.setString(3, sourceId)
            sql.executeQuery().use { rows -> buildList {
                while (rows.next()) {
                    val state = State.valueOf(rows.getString("state"))
                    val bytes = requireNotNull(rows.getBytes("resultBytes")) { "Operation outcome requires reconciliation" }
                    require(CanonicalRequest.sha256(bytes) == rows.getString("resultHash")) { "Corrupt operation result" }
                    val request = rows.getBytes("requestBytes")
                    require(CanonicalRequest.sha256(request) == rows.getString("requestHash")) { "Corrupt operation request" }
                    add(Stored(Receipt(UUID.fromString(rows.getString("operationId")), rows.getString("sourceId"),
                        rows.getString("requestHash"), state, bytes), request, rows.getString("participants"), rows.getString("definitionVersions")))
                }
            } }
        }
    }
    private fun connection(): Connection = TransactionManager.current().connection.connection as Connection
}
