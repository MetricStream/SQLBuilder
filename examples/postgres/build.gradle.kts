plugins {
    java
    application
}

dependencies {
    implementation(project(":core"))
    implementation("org.postgresql:postgresql:42.2.19")

    testImplementation(project(":mock"))
    testImplementation("org.mockito:mockito-core:3.11.2")
    testImplementation("org.mockito:mockito-junit-jupiter:3.11.2")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.7.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.7.2")
}

application {
    mainClass.set("postgres.App")
}
