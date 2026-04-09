plugins {
    java
    application
}

dependencies {
    testImplementation(platform(libs.junit.bom))
    implementation(project(":core"))
    implementation(libs.postgresql)

    testImplementation(project(":mock"))
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit.jupiter)
    testImplementation(libs.junit.jupiter.api)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testRuntimeOnly(libs.junit.platform.launcher)
}

application {
    mainClass.set("postgres.App")
}
