plugins {
    id("com.gradleup.nmcp.settings") version "1.4.4"
}

nmcpSettings {
    centralPortal {
        username = providers.gradleProperty("sonatypeUsername").getOrElse("")
        password = providers.gradleProperty("sonatypePassword").getOrElse("")
        publishingType = "USER_MANAGED"
    }
}

include("core", "mock", "docs", "examples:postgres", "examples:oracle")
