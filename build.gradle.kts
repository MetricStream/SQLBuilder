plugins {
    base
}

allprojects {
    group = "com.metricstream.jdbc"
    version = "1.0.0"

    repositories {
       mavenCentral()
    }

    tasks.withType<AbstractArchiveTask> {
        setProperty("archiveBaseName", "sqlbuilder")
        setProperty("archiveAppendix", project.name)
    }

}
