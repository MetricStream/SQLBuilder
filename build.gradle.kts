plugins {
    base
    `java-library`
    `maven-publish`
    signing
    kotlin("jvm") version "2.3.0"
    alias(libs.plugins.ktlint)
    alias(libs.plugins.detekt)
    alias(libs.plugins.versions)
    alias(libs.plugins.versions.filter)
    alias(libs.plugins.versions.update)
}

// These are credentials required for publishing. They are read either from gradle.properties or from the command
// line using -PsonatypeUsername=abc
val sonatypeUsername: String by project
val sonatypePassword: String by project

repositories {
    mavenCentral()
}

subprojects {
    apply(plugin = "maven-publish")
    apply(plugin = "java-library")
    apply(plugin = "kotlin")
    apply(plugin = "signing")
    apply(plugin = "org.jlleitschuh.gradle.ktlint")
    apply(plugin = "dev.detekt")
    group = "com.metricstream.jdbc"
    version = "4.0.0"

    repositories {
        mavenCentral()
    }

    kotlin {
        jvmToolchain {
            languageVersion.set(JavaLanguageVersion.of(21))
        }
    }

    tasks.withType<AbstractArchiveTask> {
        setProperty("archiveBaseName", "sqlbuilder")
        setProperty("archiveAppendix", project.name)
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }

    tasks.withType<JavaCompile> {
        options.compilerArgs.add("-Xlint:deprecation")
        options.compilerArgs.add("-Xlint:unchecked")
    }

    java {
        withJavadocJar()
        withSourcesJar()
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
                            name.set("Prasadu Babu Dandu")
                            email.set("prasadbabu@metricstream.com")
                            organization.set("MetricStream, Inc.")
                            organizationUrl.set("https://metricstream.com")
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

        repositories { }
    }

    signing {
        val signingKeyFile: String? by project
        val signingPassword: String? by project
        if (signingKeyFile != null) {
            val keyContent = file(signingKeyFile!!).readText()
            useInMemoryPgpKeys(keyContent, signingPassword)
        }
        sign(publishing.publications["maven${project.name}"])
    }

    detekt {
        config = files("detekt-config.yml")
        buildUponDefaultConfig = true
    }
}
