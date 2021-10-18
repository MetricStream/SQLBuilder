package com.metricstream.jdbc

import java.lang.reflect.Method
import java.sql.ResultSet
import org.mockito.kotlin.mockingDetails

class Invocations {
    @get:JvmName("getResultSet") var getResultSet: Int = 0

    @get:JvmName("getInt") var getInt: Int = 0

    @get:JvmName("getLong") var getLong: Int = 0

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

    val getRsString: Int
        @JvmName("getRsString") get() = invocationCount(rsGetString)

    val getRsBigDecimal: Int
        @JvmName("getRsBigDecimal") get() = invocationCount(rsGetBigDecimal)

    val getRsObject: Int
        @JvmName("getRsObject") get() = invocationCount(rsGetObject)

    val getRsDateTime: Int
        @JvmName("getRsDateTime") get() = invocationCount(rsGetDateTime)

    val getRsInstant: Int
        @JvmName("getRsInstant") get() = invocationCount(rsGetInstant)

    val getRsTimestamp: Int
        @JvmName("getRsTimestamp") get() = invocationCount(rsGetTimestamp)

    val getRsDate: Int
        @JvmName("getRsDate") get() = invocationCount(rsGetDate)

    private fun invocationCount(methods: Set<Method>): Int {
        return returnedResultSets.flatMap { mockingDetails(it).invocations }.count { it.method in methods }
    }

    val returnedResultSets: MutableList<ResultSet> = mutableListOf()

    val getAnyColumn: Int
        @JvmName("getAnyColumn") get() = getInt +
                getLong +
                getString +
                getBigDecimal +
                getObject +
                getDateTime +
                getInstant +
                getTimestamp +
                getDate

    companion object {
        private val rsNext = setOf(ResultSet::class.java.getDeclaredMethod("next"))
        private val rsGetInt = rsGet("Int")
        private val rsGetLong = rsGet("Long")
        private val rsGetString = rsGet("String")
        private val rsGetBigDecimal = rsGet("BigDecimal")
        private val rsGetObject = rsGet("Object")
        private val rsGetDateTime = rsGet("DateTime")
        private val rsGetInstant = rsGet("Instant")
        private val rsGetTimestamp = rsGet("Timestamp")
        private val rsGetDate = rsGet("Date")

        private fun rsGet(name: String) = listOf(Integer.TYPE, String::class.java)
            .map { ResultSet::class.java.getDeclaredMethod("get$name", it) }
            .toSet()
    }
}
