plugins {
    java
    application
}

dependencies {
    implementation(project(":core"))
    implementation("org.slf4j:slf4j-api:1.7.36")
    implementation("ch.qos.logback:logback-classic:1.2.11")
    implementation("com.oracle.database.jdbc:ojdbc8:21.4.0.0.1")
    testImplementation(project(":mock"))
    testImplementation("org.mockito:mockito-core:4.3.1")
    testImplementation("org.mockito:mockito-junit-jupiter:4.3.1")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.8.2")
}

application {
    mainClass.set("oracle.App")
}
