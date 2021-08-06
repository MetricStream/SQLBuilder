plugins {
    java
    application
}

dependencies {
    implementation(project(":core"))
    implementation("org.postgresql:postgresql:42.2.23")

    testImplementation(project(":mock"))
    testImplementation("org.mockito:mockito-all:1.10.19")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.7.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.7.2")
}

application {
    mainClass.set("postgres.App")
}
