include("core", "mock", "docs", "examples:postgres", "examples:oracle")

dependencyResolutionManagement {
    versionCatalogs {
        create("libs") {
            val assertj = version("assertj", "3.23.1")
            val codec = version("codec", "1.15")
            val junit5 = version("junit5", "5.8.2")
            val kotest = version("kotest", "5.3.0")
            val logback = version("logback", "1.2.11")
            val logging = version("logging", "2.1.23")
            val mockito = version("mockito", "4.6.1")
            val mockk = version("mockk", "1.12.4")
            val ojdbc8 = version("ojdbc8", "21.5.0.0")
            val opencsv = version("opencsv", "5.6")
            val postgresql = version("postgresql", "42.3.6")
            val slf4j = version("slf4j", "1.7.36")

            fun lib(name: String, group: String) = library(name, group, name)
            infix fun VersionCatalogBuilder.LibraryAliasBuilder.ver(version: String) = versionRef(version)

            lib("assertj-core", "org.assertj") ver assertj
            lib("commons-codec", "commons-codec") ver codec
            lib("junit-jupiter-api", "org.junit.jupiter") ver junit5
            lib("junit-jupiter-engine", "org.junit.jupiter") ver junit5
            lib("kotest-assertions-core", "io.kotest") ver kotest
            lib("kotlin-logging-jvm", "io.github.microutils") ver logging
            lib("logback-classic", "ch.qos.logback") ver logback
            lib("mockito-core", "org.mockito") ver mockito
            lib("mockito-junit-jupiter", "org.mockito") ver mockito
            lib("mockk", "io.mockk") ver mockk
            lib("ojdbc11", "com.oracle.database.jdbc") ver ojdbc8
            lib("opencsv", "com.opencsv") ver opencsv
            lib("postgresql", "org.postgresql") ver postgresql
            lib("slf4j-api", "org.slf4j") ver slf4j

            val ktlint = version("ktlint", "10.3.0")
            val detekt = version("detekt", "1.20.0")

            infix fun VersionCatalogBuilder.PluginAliasBuilder.ver(version: String) = versionRef(version)
            plugin("ktlint", "org.jlleitschuh.gradle.ktlint") ver ktlint
            plugin("detekt", "io.gitlab.arturbosch.detekt") ver detekt
        }
    }
}
