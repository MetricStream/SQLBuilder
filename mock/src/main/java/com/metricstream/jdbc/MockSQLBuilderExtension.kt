/*
 * Copyright © 2021-2022, MetricStream, Inc. All rights reserved.
 */

package com.metricstream.jdbc

import org.junit.jupiter.api.extension.AfterAllCallback
import org.junit.jupiter.api.extension.AfterEachCallback
import org.junit.jupiter.api.extension.BeforeAllCallback
import org.junit.jupiter.api.extension.ExtensionContext

class MockSQLBuilderExtension : BeforeAllCallback, AfterAllCallback, AfterEachCallback {

    override fun beforeAll(extensionContext: ExtensionContext) {
        MockSQLBuilderProvider.enable()
    }

    override fun afterAll(extensionContext: ExtensionContext) {
        MockSQLBuilderProvider.disable()
    }

    override fun afterEach(extensionContext: ExtensionContext) {
        MockSQLBuilderProvider.reset()
    }
}
