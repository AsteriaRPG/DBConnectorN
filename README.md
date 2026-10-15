# DBConnectorN
## 概要
- 各プラグインのDBを1つにまとめるための**プラグイン**です(ライブラリ/フレームワークではありません。)

## 使い方
### build.gradle.kts
```gradle
repositories {
    maven {
        url = uri("https://maven.pkg.github.com/xxpmmperxx/DBConnectorN")
        credentials {
            username = project.findProperty("gpr.user") as String? ?: System.getenv("GITHUB_ACTOR")
            password = project.findProperty("gpr.key") as String? ?: System.getenv("GITHUB_TOKEN")
        }
    }
}

dependencies {
    compileOnly("jp.asteria:db-connector-n:1.3.0")
}
```

### plugin.yml
dependを書いてください
```
name: SamplePlugin
version: 1.0.0
～～～～（略）～～～～
depend:
  - DBConnectorN
```

### ソースコード
`transaction`は成功時にコミットし、例外・Errorの発生時にロールバックする。接続はスレッドごとに管理し、最外のスコープの終了時に閉じる。
```kotlin
import jp.asteria.dbconnector.Database
import jp.asteria.dbconnector.Database.transaction

// 直でSQL書く場合
transaction {
    val type = Database.type // enumを取得できる 今のところは SQLITE,MYSQL の2種類

    val connection = Database.getTransactionConnection() ?: throw SQLException() // java.sql.Connection
    // あとはいつも通りPreparedStatementとか
}

// Exposedとかで使う用でDataSource取得
val ds = Database.getDataSource()
```

## トランザクションの入れ子と互換性

同じスレッドで`Database.transaction`を入れ子にすると、外側と同じ接続でセーブポイントを作る。
内側の成功は外側のコミットまで確定しない。内側の失敗を外側で捕捉した場合、内側の変更だけを戻して処理を続けられる。
セーブポイントへのロールバックに失敗した場合は、外側もコミットせず失敗する。

接続を別スレッドやスコープ外へ持ち出したり、呼び出し側でコミット・クローズしたりしない。
Exposedなどへ`getDataSource()`を渡して作成したトランザクションは、このJDBCスコープとは別管理となる。

1.2.1以前の`transaction`はinline関数だったため、更新時はJDBCの`transaction`を使うプラグインも1.2.2以降で再ビルドする。
1.2.2以降は処理をDBConnectorN側で実行する。非ローカルreturnは使わず、結果をブロックの末尾や`return@transaction`で返す。

## Exposedの保存後通知

Exposed 1.0.0のJDBCトランザクションでは、`CommittedEffects`で保存後の処理を予約できる。ExposedはこのAPIを利用する側が用意し、DBConnectorNには埋め込まない。通常のJDBC接続と`Database.transaction`だけを使う場合はExposed不要。

```kotlin
import jp.asteria.dbconnector.CommittedEffects

val effects = CommittedEffects(
    schedule = { action ->
        server.scheduler.scheduleDelayedTask(plugin, Runnable { action() }, 1)
    },
    onFailure = { failure -> plugin.logger.error("保存後通知に失敗", failure) }
)

org.jetbrains.exposed.v1.jdbc.transactions.transaction {
    // このトランザクションで保存する。
    effects.enqueue(this) { publishSnapshot(playerId, snapshot) }
}
```

`schedule`には処理をその場で実行せず、別の実行機会へ予約する関数を渡す。コミットしたスレッドから呼ばれ得るため、実行先が受け付けるスレッドで安全に予約できる必要がある。処理にはID・不変値を渡し、DAO・接続・トランザクションを保持しない。表示や通知の内容、必要なスレッドは各機能が管理する。

同じDBの子トランザクションの予約は親へ引き継ぎ、最外の保存後だけに実行する。取消・SQL再試行の失敗分は破棄する。通知・予約・エラー報告の失敗で保存済みデータを取り消したり再試行したりしない。致命的なVMエラーは伝播する。

所有するプラグインごとにインスタンスを作り、停止時に`close()`する。停止後は未実行の予約と新規予約を無効にする。開始済みの処理は中断しない。`dispatch { ... }`はDB保存を伴わない処理を同じ実行先へ予約する。別インスタンス間の実行順は保証しない。
