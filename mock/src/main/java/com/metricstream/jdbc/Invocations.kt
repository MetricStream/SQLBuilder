package com.metricstream.jdbc

import java.sql.ResultSet
import io.mockk.MockKGateway

class Invocations {
    @get:JvmName("getResultSet") var getResultSet: Int = 0

    @get:JvmName("getInt") var getInt: Int = 0

    @get:JvmName("getLong") var getLong: Int = 0

    @get:JvmName("getDouble") var getDouble: Int = 0

    @get:JvmName("getString") var getString: Int = 0

    @get:JvmName("getBigDecimal") var getBigDecimal: Int = 0

    @get:JvmName("getObject") var getObject: Int = 0

    @get:JvmName("getDateTime") var getDateTime: Int = 0

    @get:JvmName("getInstant") var getInstant: Int = 0

    @get:JvmName("getTimestamp") var getTimestamp: Int = 0

    @get:JvmName("getDate") var getDate: Int = 0

    @get:JvmName("execute") var execute: Int = 0

    @get:JvmName("getList") var getList: Int = 0

    @get:JvmName("getMap") var getMap: Int = 0

    @get:JvmName("getSingle") var getSingle: Int = 0

    @get:JvmName("getRs") var getRs: Int = 0

    val getNext: Int
        @JvmName("getNext") get() = invocationCount(rsNext)

    val getRsInt: Int
        @JvmName("getRsInt") get() = invocationCount(rsGetInt)

    val getRsLong: Int
        @JvmName("getRsLong") get() = invocationCount(rsGetLong)

    val getRsDouble: Int
        @JvmName("getRsDouble") get() = invocationCount(rsGetDouble)

    val getRsString: Int
        @JvmName("getRsString") get() = invocationCount(rsGetString)

    val getRsBigDecimal: Int
        @JvmName("getRsBigDecimal") get() = invocationCount(rsGetBigDecimal)

    val getRsObject: Int
        @JvmName("getRsObject") get() = invocationCount(rsGetObject)

    val getRsTimestamp: Int
        @JvmName("getRsTimestamp") get() = invocationCount(rsGetTimestamp)

    val getRsDate: Int
        @JvmName("getRsDate") get() = invocationCount(rsGetDate)

    private fun invocationCount(unused: String): Int {
//        return MockKGateway.implementation().callRecorder.calls.count { it.matcher.method.name == name }
        return 0
    }

    val returnedResultSets: MutableList<ResultSet> = mutableListOf()

    val getAnyColumn: Int
        @JvmName("getAnyColumn") get() = getInt +
                getLong +
                getDouble +
                getString +
                getBigDecimal +
                getObject +
                getDateTime +
                getInstant +
                getTimestamp +
                getDate

    companion object {
        private val rsNext = "next"
        private val rsGetInt = rsGet("Int")
        private val rsGetLong = rsGet("Long")
        private val rsGetDouble = rsGet("Double")
        private val rsGetString = rsGet("String")
        private val rsGetBigDecimal = rsGet("BigDecimal")
        private val rsGetObject = rsGet("Object")
        private val rsGetTimestamp = rsGet("Timestamp")
        private val rsGetDate = rsGet("Date")

        private fun rsGet(name: String) = "get$name"
    }
}
