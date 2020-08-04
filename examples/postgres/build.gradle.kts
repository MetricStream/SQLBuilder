plugins {
    java
    application
}

dependencies {
    implementation(project(":core"))
    implementation("org.postgresql:postgresql:42.2.14")

    testImplementation(project(":mock"))
    testImplementation("org.mockito:mockito-core:3.4.6")
    testImplementation("org.mockito:mockito-junit-jupiter:3.4.6")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.6.2")
}

application {
    mainClassName = "postgres.App"
}
