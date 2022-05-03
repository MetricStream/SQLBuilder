dependencies {
    implementation(project(":core"))
    implementation(libs.slf4j.api)
    implementation(libs.logback.classic)
    implementation(libs.kotlin.logging.jvm)
    implementation(libs.opencsv)
    testImplementation(libs.mockk)
    testImplementation(libs.mockito.core)
    testImplementation(libs.kotest.assertions.core)
    testImplementation(libs.assertj.core)
    implementation(libs.junit.jupiter.api)
    testRuntimeOnly(libs.junit.jupiter.engine)
}
