dependencies {
    implementation(project(":core"))
    implementation("org.slf4j:slf4j-api:1.7.32")
    implementation("ch.qos.logback:logback-classic:1.2.8")
    implementation("com.opencsv:opencsv:5.5.2")
    testImplementation("io.mockk:mockk:1.12.1")
    testImplementation("org.mockito:mockito-core:4.0.0")
    testImplementation("io.kotest:kotest-assertions-core:5.0.1")
    testImplementation("org.assertj:assertj-core:3.21.0")
    implementation("org.junit.jupiter:junit-jupiter-api:5.8.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.8.2")
}
