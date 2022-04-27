plugins {
    java
    application
}

dependencies {
    implementation(project(":core"))
    implementation(libs.postgresql)

    testImplementation(project(":mock"))
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit.jupiter)
    testImplementation(libs.junit.jupiter.api)
    testRuntimeOnly(libs.junit.jupiter.engine)
}

application {
    mainClass.set("postgres.App")
}
