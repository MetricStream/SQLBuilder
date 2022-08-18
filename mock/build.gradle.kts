plugins {
    antlr
    idea
}

dependencies {
    implementation(project(":core"))
    implementation(libs.slf4j.api)
    implementation(libs.logback.classic)
    implementation(libs.kotlin.logging.jvm)
    implementation(libs.opencsv)
    antlr(libs.antlr)
    testImplementation(libs.mockk)
    testImplementation(libs.mockito.core)
    testImplementation(libs.kotest.assertions.core)
    testImplementation(libs.assertj.core)
    implementation(libs.junit.jupiter.api)
    testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.generateGrammarSource {
    arguments.addAll(listOf("-package", "com.metricstream.jdbc.antlr"))
}


kotlin {
    sourceSets.main.get().kotlin.srcDir(tasks.generateGrammarSource)
}

tasks.compileKotlin {
    dependsOn(tasks.generateGrammarSource)
}
tasks.compileTestKotlin {
    dependsOn(tasks.generateTestGrammarSource)
}
tasks.compileJava {
    dependsOn(tasks.generateGrammarSource)
}

tasks.test {
    testLogging.showStandardStreams = true
}
