dependencies {
    implementation(project(":core"))
    implementation("org.slf4j:slf4j-api:1.7.32")
    implementation("ch.qos.logback:logback-classic:1.2.6")
    implementation("com.opencsv:opencsv:5.5.2")
    implementation("org.mockito:mockito-core:4.0.0")
    implementation("org.mockito.kotlin:mockito-kotlin:4.0.0")
    testImplementation("io.kotest:kotest-assertions-core:4.6.3")
    testImplementation("org.assertj:assertj-core:3.21.0")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.8.1")
}
