dependencies {
    testImplementation(platform(libs.junit.bom))
    implementation(libs.slf4j.api)
    implementation(libs.logback.classic)
    implementation(libs.commons.codec)
    implementation(libs.kotlin.logging.jvm)

    testImplementation(libs.junit.jupiter.api)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testRuntimeOnly(libs.junit.platform.launcher)
}
