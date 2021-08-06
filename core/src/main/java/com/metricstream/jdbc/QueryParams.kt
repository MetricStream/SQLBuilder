/*
 * Copyright © 2020-2021, MetricStream, Inc. All rights reserved.
 */
package com.metricstream.jdbc

/**
 * Query parameter values holder. It will give parameter value by name
 */
interface QueryParams {
    /**
     * @param name name of the parameter
     * @return parameter value by name
     */
    fun getParameterValue(name: String): Any?

    /**
     * @param name name of the parameter
     * @param isMulti can have multiple values
     * @return parameter values collection if it is has multiple values by name
     */
    fun getParameterValue(name: String, isMulti: Boolean): Any?
    fun getParameterValue(name: String, isMulti: Boolean, dateAsString: Boolean): Any?

    /**
     * To check if parameter expects date as string. TO_DATE function can be misused in queries.
     * E.g. select col1 from tab1 where date1 <= to_date(:1). In this case :1 should be ideally configured as
     * VARCHAR type. However it can be configured as Date type to support upstream components render(reports filters, form elements etc) as Date field.
     * Actually it should not be used to_date in this context as query already expects date type object, why to_date is used again.
     * To support backward compatibility (previously parameters hard replacement works well in this case), if parameter is bound with TO_DATE,
     * the value must be string. matching for `TO_DATE(` or select `TO_DATE(NVL(`
     * @param subStr The string that is searched
     * @return true if `getDateParameterAsString` should be used for this parameter value
     */
    fun dateAsStringNeeded(subStr: String): Boolean

    /**
     * Date type parameter may be considered as VARCHAR.
     * @return DATE format to convert Date object
     */
    val dateParameterAsString: String
    val paramNames: List<String>
}
