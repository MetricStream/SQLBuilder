dependencies {
    implementation(project(":core"))
    implementation("org.slf4j:slf4j-api:1.7.31")
    implementation("ch.qos.logback:logback-classic:1.2.3")
    implementation("com.opencsv:opencsv:5.4")
    implementation("org.mockito:mockito-core:3.11.2")
    testImplementation("org.hamcrest:hamcrest:2.2")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.7.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.7.2")
}
