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
    compileOnly("jp.asteria:db-connector-n:1.2.2")
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
