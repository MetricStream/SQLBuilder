plugins {
    java
    application
}

dependencies {
    implementation(project(":core"))
    implementation("org.postgresql:postgresql:42.2.12")

    testImplementation(project(":mock"))
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.2")
    testImplementation("org.mockito:mockito-all:1.10.19")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.6.2")
}

application {
    mainClassName = "postgres.App"
}
