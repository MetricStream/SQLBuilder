plugins {
    base
    `java-library`
    `maven-publish`
    signing
}

subprojects {
    apply(plugin = "maven-publish")
    apply(plugin = "java-library")
    apply(plugin = "signing")

    group = "com.metricstream.jdbc"
    version = "1.0.0"

    repositories {
        mavenCentral()
    }

    tasks.withType<AbstractArchiveTask> {
        setProperty("archiveBaseName", "sqlbuilder")
        setProperty("archiveAppendix", project.name)

    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }

    publishing {
        publications {
            create<MavenPublication>("maven${project.name}") {
                artifactId = "sqlbuilder-${project.name}"

                from(components["java"])

                pom {
                    name.set("SQLBuilder")
                    description.set("SQLBuilder is a Java library which aims to be a better PreparedStatement")
                    url.set("https://github.com/MetricStream/SQLBuilder")
                    licenses {
                        license {
                            name.set("The Apache License, Version 2.0")
                            url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                        }
                    }
                    developers {
                        developer {
                            id.set("nkiesel")
                            name.set("Norbert Kiesel")
                            email.set("nkiesel@metricstream.com")
                        }
                    }
                    scm {
                        connection.set("scm:git:git@github.com:MetricStream/SQLBuilder.git")
                        developerConnection.set("scm:git:git@github.com:MetricStream/SQLBuilder.git")
                        url.set("https://github.com/MetricStream/SQLBuilder")
                    }
                }
            }
        }
    }

    signing {
        useGpgCmd()
        sign(publishing.publications["maven${project.name}"])
    }

}
