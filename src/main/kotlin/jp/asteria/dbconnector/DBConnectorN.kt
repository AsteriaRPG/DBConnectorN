package jp.asteria.dbconnector

import cn.nukkit.plugin.PluginBase

class DBConnectorN : PluginBase() {
    override fun onEnable() {
        saveDefaultConfig()

        try {
            val type = DBType.entries.first { x -> x.str == config.getString("type") }
            var url = ""
            var driver = ""
            var user = ""
            var password = ""
            when (type) {
                DBType.SQLITE -> {
                    url = "jdbc:sqlite:${config.getString("sqlite.file")}"
                    driver = org.sqlite.JDBC::class.java.name
                }

                DBType.MYSQL -> {
                    url = "jdbc:mysql://${config.getString("mysql.url")}/${config.getString("mysql.name")}"
                    driver = com.mysql.jdbc.Driver::class.java.name
                    user = config.getString("mysql.user", "")!!
                    password = config.getString("mysql.password", "")!!
                }
            }
            Database.connect(type, url, driver, user, password)
        } catch (e: Exception) {
            logger.warning(e.stackTraceToString())
            logger.warning("データベースに接続できないため、プラグインを無効化します。")
            server.pluginManager.disablePlugin(this)
        }
    }

    override fun onDisable() {
        Database.close()
    }
}
