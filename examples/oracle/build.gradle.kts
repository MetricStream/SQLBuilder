plugins {
    java
    application
}

dependencies {
    implementation(project(":core"))
    implementation(libs.slf4j.api)
    implementation(libs.logback.classic)
    implementation(libs.ojdbc11)
    testImplementation(project(":mock"))
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit.jupiter)
    testImplementation(libs.junit.jupiter.api)
    testRuntimeOnly(libs.junit.jupiter.engine)
}

application {
    mainClass.set("oracle.App")
}
