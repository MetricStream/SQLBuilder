package com.metricstream.jdbc

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

    @get:JvmName("next") var next: Int = 0

    @get:JvmName("getRsInt") var getRsInt: Int = 0

    @get:JvmName("getRsLong") var getRsLong: Int = 0

    @get:JvmName("getRsBoolean") var getRsBoolean: Int = 0

    @get:JvmName("getRsDouble") var getRsDouble: Int = 0

    @get:JvmName("getRsString") var getRsString: Int = 0

    @get:JvmName("getRsBigDecimal") var getRsBigDecimal: Int = 0

    @get:JvmName("getRsObject") var getRsObject: Int = 0

    @get:JvmName("getRsTimestamp") var getRsTimestamp: Int = 0

    @get:JvmName("getRsDate") var getRsDate: Int = 0

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
}
