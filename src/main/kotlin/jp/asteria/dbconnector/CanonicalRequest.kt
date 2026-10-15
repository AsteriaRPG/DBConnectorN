package jp.asteria.dbconnector

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/** 操作の同一性に使う整数のみの正規JSON。可変入力を作成時にbyteへ固定する。 */
class CanonicalRequest(value: Map<String, Any?>) {
    private val encoded = encode(value).toByteArray(StandardCharsets.UTF_8).also {
        require(it.size <= MAX_BYTES) { "Request too large" }
    }
    val hash: String = sha256(encoded)
    fun bytes(): ByteArray = encoded.clone()

    companion object {
        const val MAX_BYTES = 4 * 1024 * 1024
        fun sha256(bytes: ByteArray): String = MessageDigest.getInstance("SHA-256").digest(bytes)
            .joinToString("") { "%02x".format(it) }

        private fun encode(value: Any?, depth: Int = 0): String {
            require(depth <= 32) { "Request nesting too deep" }
            return when (value) {
                null -> "null"
                is Boolean, is Byte, is Short, is Int, is Long -> value.toString()
                is String -> quote(value)
                is List<*> -> value.joinToString(",", "[", "]") { encode(it, depth + 1) }
                is Map<*, *> -> {
                    require(value.keys.all { it is String })
                    value.keys.map { it as String }.sorted().joinToString(",", "{", "}") {
                        quote(it) + ":" + encode(value[it], depth + 1)
                    }
                }
                else -> throw IllegalArgumentException("Unsupported request value: ${value.javaClass.name}")
            }
        }

        private fun quote(value: String): String {
            require(StandardCharsets.UTF_8.newEncoder().canEncode(value)) { "Malformed Unicode" }
            return buildString {
                append('"')
                for (char in value) when (char) {
                    '"' -> append("\\\"")
                    '\\' -> append("\\\\")
                    '\b' -> append("\\b")
                    '\u000C' -> append("\\f")
                    '\n' -> append("\\n")
                    '\r' -> append("\\r")
                    '\t' -> append("\\t")
                    else -> if (char.code < 32) append("\\u%04x".format(char.code)) else append(char)
                }
                append('"')
            }
        }
    }
}
