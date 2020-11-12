plugins {
    java
    application
}

dependencies {
    implementation(project(":core"))
    implementation("org.postgresql:postgresql:42.2.18")

    testImplementation(project(":mock"))
    testImplementation("org.mockito:mockito-core:3.6.0")
    testImplementation("org.mockito:mockito-junit-jupiter:3.6.0")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.7.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.7.0")
}

application {
    mainClassName = "postgres.App"
}
