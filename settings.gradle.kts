include("core", "mock", "docs", "examples:postgres", "examples:oracle")

dependencyResolutionManagement {
    versionCatalogs {
        create("libs") {
            val assert4j = version("assert4j", "3.22.0")
            val codec = version("codec", "1.15")
            val junit5 = version("junit5", "5.8.2")
            val kotest = version("kotest", "5.2.3")
            val logback = version("logback", "1.2.11")
            val logging = version("logging", "2.1.21")
            val mockito = version("mockito", "4.5.1")
            val mockk = version("mockk", "1.12.3")
            val ojdbc8 = version("ojdbc8", "21.5.0.0")
            val opencsv = version("opencsv", "5.6")
            val postgresql = version("postgresql", "42.3.3")
            val slf4j = version("slf4j", "1.7.36")

            library("assertj-core", "org.assertj", "assertj-core").versionRef(assert4j)
            library("commons-codec", "commons-codec", "commons-codec").versionRef(codec)
            library("junit-jupiter-api", "org.junit.jupiter", "junit-jupiter-api").versionRef(junit5)
            library("junit-jupiter-engine", "org.junit.jupiter", "junit-jupiter-engine").versionRef(junit5)
            library("kotest-assertions-core", "io.kotest", "kotest-assertions-core").versionRef(kotest)
            library("kotlin-logging", "io.github.microutils", "kotlin-logging-jvm").versionRef(logging)
            library("logback-classic", "ch.qos.logback", "logback-classic").versionRef(logback)
            library("mockito-core", "org.mockito", "mockito-core").versionRef(mockito)
            library("mockito-junit-jupiter", "org.mockito", "mockito-junit-jupiter").versionRef(mockito)
            library("mockk", "io.mockk", "mockk").versionRef(mockk)
            library("ojdbc8", "com.oracle.database.jdbc", "ojdbc8").versionRef(ojdbc8)
            library("opencsv", "com.opencsv", "opencsv").versionRef(opencsv)
            library("postgresql", "org.postgresql", "postgresql").versionRef(postgresql)
            library("slf4j-api", "org.slf4j", "slf4j-api").versionRef(slf4j)

            val ktlint = version("ktlint", "10.2.1")
            val detekt = version("detekt", "1.19.0")

            plugin("ktlint", "org.jlleitschuh.gradle.ktlint").versionRef(ktlint)
            plugin("detekt", "io.gitlab.arturbosch.detekt").versionRef(detekt)
        }
    }
}
