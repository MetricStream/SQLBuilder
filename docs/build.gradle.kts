fun pandoc(format: String) = "pandoc --from=markdown+yaml_metadata_block Rationale.md metadata.yaml" +
        " --number-sections --highlight-style=pygments --standalone --output Rationale.$format"

val formats = listOf("pdf", "html", "docx")

// I cannot figure out how to run 2 commandLine in a single Exec task, so instead this dynamically creates
// a task per format
formats.forEach { fmt -> tasks.register<Exec>(fmt) { commandLine(pandoc(fmt).split(" ")) } }

tasks.register("pandoc") {
    formats.forEach { fmt -> dependsOn(fmt) }
}

tasks.named<Delete>("clean") {
    delete = formats.map { fmt -> "Rationale.$fmt" }.toSet()
}
