dependencies {
    implementation(project(":core"))
    implementation("org.slf4j:slf4j-api:1.7.36")
    implementation("ch.qos.logback:logback-classic:1.2.11")
    implementation("io.github.microutils:kotlin-logging-jvm:2.1.21")
    implementation("com.opencsv:opencsv:5.6")
    testImplementation("io.mockk:mockk:1.12.3")
    testImplementation("org.mockito:mockito-core:4.4.0")
    testImplementation("io.kotest:kotest-assertions-core:5.2.2")
    testImplementation("org.assertj:assertj-core:3.22.0")
    implementation("org.junit.jupiter:junit-jupiter-api:5.8.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.8.2")
}
