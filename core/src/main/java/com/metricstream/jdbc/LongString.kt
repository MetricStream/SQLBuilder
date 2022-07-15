/*
 * Copyright © 2020-2022, MetricStream, Inc. All rights reserved.
 */
package com.metricstream.jdbc

import java.io.Reader
import java.io.StringReader

class LongString(val data: String) {
    val reader: Reader
        get() = StringReader(data)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as LongString

        if (data != other.data) return false

        return true
    }

    override fun hashCode(): Int {
        return data.hashCode()
    }
}
