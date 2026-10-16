package jp.asteria.dbconnector

import org.jetbrains.exposed.v1.core.Table

internal object OutboxEvents : Table("persistence_outbox") {
    val owner = varchar("owner", 128)
    val eventId = varchar("eventId", 36)
    val sourceOperationId = varchar("sourceOperationId", 36)
    val destination = varchar("destination", 128)
    val eventType = varchar("eventType", 128)
    val payloadBytes = blob("payloadBytes")
    val payloadHash = varchar("payloadHash", 64)
    // 送信元の固定handler定義から保存する配送分類。再claim時にpayloadから選び直さない。
    val deliveryClass = varchar("deliveryClass", 32)
    val state = varchar("state", 16)
    val leaseEpoch = varchar("leaseEpoch", 36).nullable()
    val leaseUntilUtcMs = long("leaseUntilUtcMs").nullable()
    val attempt = long("attempt")
    val revision = long("revision")
    override val primaryKey = PrimaryKey(owner, eventId)
    init { uniqueIndex(owner, sourceOperationId, destination, eventType); index(false, destination, state) }
}

internal object InboxReceipts : Table("persistence_inbox_receipts") {
    val destination = varchar("destination", 128)
    val eventId = varchar("eventId", 36)
    val sourceOwner = varchar("sourceOwner", 128)
    val sourceOperationId = varchar("sourceOperationId", 36)
    val eventType = varchar("eventType", 128)
    val payloadHash = varchar("payloadHash", 64)
    val resultHash = varchar("resultHash", 64)
    val state = varchar("state", 16)
    val createdUtcMs = long("createdUtcMs")
    override val primaryKey = PrimaryKey(destination, eventId)
}
