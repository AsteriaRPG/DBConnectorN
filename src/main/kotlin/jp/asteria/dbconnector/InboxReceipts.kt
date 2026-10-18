package jp.asteria.dbconnector

import org.jetbrains.exposed.v1.core.Table

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
