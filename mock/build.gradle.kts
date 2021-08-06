dependencies {
    implementation(project(":core"))
    implementation("org.slf4j:slf4j-api:1.7.32")
    implementation("ch.qos.logback:logback-classic:1.2.5")
    implementation("com.opencsv:opencsv:5.5.1")
    implementation("org.mockito:mockito-core:3.11.2")
    implementation("org.mockito.kotlin:mockito-kotlin:3.2.0")
    testImplementation("io.kotest:kotest-assertions-core:4.6.1")
    testImplementation("org.assertj:assertj-core:3.20.2")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.7.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.7.2")
}
