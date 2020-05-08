/*
 * Copyright © 2020, MetricStream, Inc. All rights reserved.
 */
package com.metricstream.util;

import java.util.BitSet;
import java.util.Collection;
import java.util.Map;

/**
 * Check is a class of commonly use utility methods for checking conditions like
 * "this collection in non-null and not empty".  All its public methods are
 * static and should always be statically imported by name (i.e. no wildcard
 * imports!).  Static imports are easily abused, so please be thoughtful when
 * extending this class.
 */
public class Check {

    /**
     * Checks if the provided parameter is null or empty
     *
     * @param arg a string
     * @return true if the parameter is null or empty, false otherwise
     */
    public static boolean noContent(String arg) {
        return arg == null || arg.isEmpty();
    }

    /**
     * Checks if the provided parameter is null or empty
     *
     * @param arg an int array
     * @return true if the parameter is null or has length 0, false otherwise
     */
    public static boolean noContent(int[] arg) {
        return arg == null || arg.length == 0;
    }

    /**
     * Checks if the provided parameter is null or empty
     *
     * @param arg a long array
     * @return true if the parameter is null or has length 0, false otherwise
     */
    public static boolean noContent(long[] arg) {
        return arg == null || arg.length == 0;
    }

    /**
     * Checks if the provided parameter is null or empty
     *
     * @param arg a character array
     * @return true if the parameter is null or has length 0, false otherwise
     */
    public static boolean noContent(char[] arg) {
        return arg == null || arg.length == 0;
    }

    /**
     * Checks if the provided parameter is null or empty
     *
     * @param arg a boolean array
     * @return true if the parameter is null or has length 0, false otherwise
     */
    public static boolean noContent(boolean[] arg) {
        return arg == null || arg.length == 0;
    }

    /**
     * Checks if the provided parameter is null or empty
     *
     * @param arg a byte array
     * @return true if the parameter is null or has length 0, false otherwise
     */
    public static boolean noContent(byte[] arg) {
        return arg == null || arg.length == 0;
    }

    /**
     * Checks if the provided parameter is null or empty
     *
     * @param arg an object array
     * @return true if the parameter is null or has length 0, false otherwise
     */
    public static boolean noContent(Object[] arg) {
        return arg == null || arg.length == 0;
    }

    /**
     * Checks if the provided parameter is null or empty
     *
     * @param arg a collection
     * @return true if the parameter is null or empty, false otherwise
     */
    public static boolean noContent(Collection<?> arg) {
        return arg == null || arg.isEmpty();
    }

    /**
     * Checks if the provided parameter is null or empty
     *
     * @param arg a map
     * @return true if the parameter is null or empty, false otherwise
     */
    public static boolean noContent(Map<?, ?> arg) {
        return arg == null || arg.isEmpty();
    }

    /**
     * Checks if the provided parameter is null or empty
     *
     * @param arg a stringBuilder
     * @return true if the parameter is null or empty, false otherwise
     */
    public static boolean noContent(StringBuilder arg) {
        return arg == null || arg.length() == 0;
    }

    /**
     * Checks if the provided parameter is null or empty
     *
     * @param arg a stringBuffer
     * @return true if the parameter is null or empty, false otherwise
     */
    public static boolean noContent(StringBuffer arg) {
        return arg == null || arg.length() == 0;
    }

    /**
     * Checks if the provided parameter is null or empty
     *
     * @param arg a bitset
     * @return true if the parameter is null or empty, false otherwise
     */
    public static boolean noContent(BitSet arg) {
        return arg == null || arg.isEmpty();
    }

    /**
     * Checks if the provided parameter is not null and not empty
     *
     * @param arg a string
     * @return true if the parameter is not null and not empty, false otherwise
     */
    public static boolean hasContent(String arg) {
        return !noContent(arg);
    }

    /**
     * Checks if the provided parameter is not null and not empty
     *
     * @param arg an int array
     * @return true if the parameter is not null and contains at least one item, false otherwise
     */
    public static boolean hasContent(int[] arg) {
        return !noContent(arg);
    }

    /**
     * Checks if the provided parameter is not null and not empty
     *
     * @param arg a long array
     * @return true if the parameter is not null and contains at least one item, false otherwise
     */
    public static boolean hasContent(long[] arg) {
        return !noContent(arg);
    }

    /**
     * Checks if the provided parameter is not null and not empty
     *
     * @param arg a character array
     * @return true if the parameter is not null and contains at least one item, false otherwise
     */
    public static boolean hasContent(char[] arg) {
        return !noContent(arg);
    }

    /**
     * Checks if the provided parameter is not null and not empty
     *
     * @param arg a boolean array
     * @return true if the parameter is not null and contains at least one item, false otherwise
     */
    public static boolean hasContent(boolean[] arg) {
        return !noContent(arg);
    }

    /**
     * Checks if the provided parameter is not null and not empty
     *
     * @param arg a byte array
     * @return true if the parameter is not null and contains at least one item, false otherwise
     */
    public static boolean hasContent(byte[] arg) {
        return !noContent(arg);
    }

    /**
     * Checks if the provided parameter is not null and not empty
     *
     * @param arg an object array
     * @return true if the parameter is not null and contains at least one item, false otherwise
     */
    public static boolean hasContent(Object[] arg) {
        return !noContent(arg);
    }

    /**
     * Checks if the provided parameter is not null and not empty
     *
     * @param arg a collection
     * @return true if the parameter is not null and contains at least one item, false otherwise
     */
    public static boolean hasContent(Collection<?> arg) {
        return !noContent(arg);
    }

    /**
     * Checks if the provided parameter is not null and not empty
     *
     * @param arg a map
     * @return true if the parameter is not null and contains at least one item, false otherwise
     */
    public static boolean hasContent(Map<?, ?> arg) {
        return !noContent(arg);
    }

    /**
     * Checks if the provided parameter is not null and not empty
     *
     * @param arg a StringBuilder
     * @return true if the parameter is not null and contains at least one item, false otherwise
     */
    public static boolean hasContent(StringBuilder arg) {
        return !noContent(arg);
    }

    /**
     * Checks if the provided parameter is not null and not empty
     *
     * @param arg a StringBuffer
     * @return true if the parameter is not null and contains at least one item, false otherwise
     */
    public static boolean hasContent(StringBuffer arg) {
        return !noContent(arg);
    }

    /**
     * Checks if the provided parameter is not null and not empty
     *
     * @param arg a bitset
     * @return true if the parameter is not null and contains at least one item, false otherwise
     */
    public static boolean hasContent(BitSet arg) {
        return !noContent(arg);
    }

    /**
     * Checks if all provided parameters are null
     *
     * @param arg an object
     * @return true if the object is null, false otherwise
     */
    public static boolean isNull(Object arg) {
        return arg == null;
    }

    /**
     * Checks if all provided parameters are null
     *
     * @param arg an object
     * @return true if the object is null, false otherwise
     */
    public static boolean allNull(Object arg) {
        return arg == null;
    }

    /**
     * Checks if all provided parameters are null
     *
     * @param arg1 an object
     * @param arg2 another object
     * @return true if all objects are null, false otherwise
     */
    public static boolean allNull(Object arg1, Object arg2) {
        return arg1 == null && arg2 == null;
    }


    /**
     * Checks if all provided parameters are null
     *
     * @param arg1 an object
     * @param arg2 another object
     * @param args optionally more objects
     * @return true if all objects are null, false otherwise
     */
    public static boolean allNull(Object arg1, Object arg2, Object... args)
    {
        if (arg1 != null || arg2 != null) {
            return false;
        }
        for (Object o : args) {
            if (o != null) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks if all provided parameters are not null
     *
     * @param arg an object
     * @return true if the object is not null, false otherwise
     */
    public static boolean notNull(Object arg) {
        return arg != null;
    }

    /**
     * Checks if all provided parameters are not null
     *
     * @param arg an object
     * @return true if the object is not null, false otherwise
     */
    public static boolean noneNull(Object arg) {
        return arg != null;
    }

    /**
     * Checks if all provided parameters are not null
     *
     * @param arg1 an object
     * @param arg2 another object
     * @return true if all objects are not null, false otherwise
     */
    public static boolean noneNull(Object arg1, Object arg2) {
        return arg1 != null && arg2 != null;
    }

    /**
     * Checks if all provided parameters are not null
     *
     * @param arg1 an object
     * @param arg2 another object
     * @param args optionally more objects
     * @return true if all objects are not null, false otherwise
     */
    public static boolean noneNull(Object arg1, Object arg2, Object... args)
    {
        if (arg1 == null || arg2 == null || args == null) {
            return false;
        }
        for (Object o : args) {
            if (o == null) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks if any provided parameters are null
     *
     * @param arg an object
     * @return true if the object is null, false otherwise
     */
    public static boolean anyNull(Object arg)
    {
        return arg == null;
    }

    /**
     * Checks if any provided parameters are null
     *
     * @param arg1 an object
     * @param arg2 another object
     * @return true if any of the objects is null, false otherwise
     */
    public static boolean anyNull(Object arg1, Object arg2)
    {
        return  arg1 == null || arg2 == null;
    }

    /**
     * Checks if any provided parameters are null
     *
     * @param arg1 an object
     * @param arg2 another object
     * @param args optionally more objects
     * @return true if any of the objects is null, false otherwise
     */
    public static boolean anyNull(Object arg1, Object arg2, Object... args)
    {
        if (arg1 == null || arg2 == null || args == null) {
            return true;
        }
        for (Object o : args) {
            if (o == null) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the first non-null parameter
     *
     * @param arg1 an object
     * @param arg2 another object of the same type
     * @return <code>arg</code> if it is not null, <code>arg2</code> otherwise
     */
    public static <T> T nvl(T arg1, T arg2)
    {
        return arg1 != null ? arg1 : arg2;
    }

    /**
     * Returns the first non-null parameter
     *
     * @param args one or more objects of the same type
     * @return the first object from <code>args</code> that is not null, or null otherwise
     */
    @SafeVarargs
    public static <T> T nvl(T... args)
    {
        for (T t : args) {
            if (t != null) {
                return t;
            }
        }
        return null;
    }

    /**
     * Returns the first parameter that has content (using hasContent()).
     *
     * @param args
     *            one or more objects of String type
     * @return the first object from <code>args</code> that has content, or null otherwise
     */
    public static String firstContent(String... args) {
        for (String val : args) {
            if (hasContent(val)) {
                return val;
            }
        }
        return null;
    }

    /**
     * Returns the first parameter that has content (using hasContent()).
     *
     * @param args
     *            one or more objects of int[] type
     * @return the first object from <code>args</code> that has content, or null otherwise
     */
    public static int[] firstContent(int[]... args) {
        for (int[] val : args) {
            if (hasContent(val)) {
                return val;
            }
        }
        return null;
    }

    /**
     * Returns the first parameter that has content (using hasContent()).
     *
     * @param args
     *            one or more objects of long[] type
     * @return the first object from <code>args</code> that has content, or null otherwise
     */
    public static long[] firstContent(long[]... args) {
        for (long[] val : args) {
            if (hasContent(val)) {
                return val;
            }
        }
        return null;
    }

    /**
     * Returns the first parameter that has content (using hasContent()).
     *
     * @param args
     *            one or more objects of char[] type
     * @return the first object from <code>args</code> that has content, or null otherwise
     */
    public static char[] firstContent(char[]... args) {
        for (char[] val : args) {
            if (hasContent(val)) {
                return val;
            }
        }
        return null;
    }

    /**
     * Returns the first parameter that has content (using hasContent()).
     *
     * @param args
     *            one or more objects of boolean[] type
     * @return the first object from <code>args</code> that has content, or null otherwise
     */
    public static boolean[] firstContent(boolean[]... args) {
        for (boolean[] val : args) {
            if (hasContent(val)) {
                return val;
            }
        }
        return null;
    }

    /**
     * Returns the first parameter that has content (using hasContent()).
     *
     * @param args
     *            one or more objects of byte[] type
     * @return the first object from <code>args</code> that has content, or null otherwise
     */
    public static byte[] firstContent(byte[]... args) {
        for (byte[] val : args) {
            if (hasContent(val)) {
                return val;
            }
        }
        return null;
    }

    /**
     * Returns the first parameter that has content (using hasContent()).
     *
     * @param args
     *            one or more objects of Object[] type
     * @return the first object from <code>args</code> that has content, or null otherwise
     */
    public static Object[] firstContent(Object[]... args) {
        for (Object[] val : args) {
            if (hasContent(val)) {
                return val;
            }
        }
        return null;
    }

    /**
     * Returns the first parameter that has content (using hasContent()).
     *
     * @param args
     *            one or more objects of Collection type
     * @return the first object from <code>args</code> that has content, or null otherwise
     */
    public static Collection<?> firstContent(Collection<?>... args) {
        for (Collection<?> val : args) {
            if (hasContent(val)) {
                return val;
            }
        }
        return null;
    }

    /**
     * Returns the first parameter that has content (using hasContent()).
     *
     * @param args
     *            one or more objects of Map type
     * @return the first object from <code>args</code> that has content, or null otherwise
     */
    public static Map<?,?> firstContent(Map<?, ?>... args) {
        for (Map<?, ?> val : args) {
            if (hasContent(val)) {
                return val;
            }
        }
        return null;
    }

    /**
     * Returns the first parameter that has content (using hasContent()).
     *
     * @param args
     *            one or more objects of StringBuilder type
     * @return the first object from <code>args</code> that has content, or null otherwise
     */
    public static StringBuilder firstContent(StringBuilder... args) {
        for (StringBuilder val : args) {
            if (hasContent(val)) {
                return val;
            }
        }
        return null;
    }

    /**
     * Returns the first parameter that has content (using hasContent()).
     *
     * @param args
     *            one or more objects of StringBuffer type
     * @return the first object from <code>args</code> that has content, or null otherwise
     */
    public static StringBuffer firstContent(StringBuffer... args) {
        for (StringBuffer val : args) {
            if (hasContent(val)) {
                return val;
            }
        }
        return null;
    }

    /**
     * Checks recursively whether all the elements are <code>null</code>.
     *
     * @param args
     *            one or more objects of any type
     * @return true, returns <code>true</code> if all the elements are <code>null</code>, otherwise <code>false</code>
     */
    @SuppressWarnings("rawtypes")
    public static boolean allNullRecursive(Object... args) {
        if (args != null) {
            if (args.length == 0) {
                return false;
            }
            for (Object a : args) {
                if (a != null) {
                    if (a instanceof Object[]) {
                        if (((Object[]) a).length == 0) {
                            return false;
                        }
                        for (Object x : (Object[]) a) {
                            if (!allNullRecursive(x)) {
                                return false;
                            }
                        }
                    } else if (a instanceof Collection) {
                        if (((Collection) a).isEmpty()) {
                            return false;
                        }
                        for (Object x : (Collection) a) {
                            if (!allNullRecursive(x)) {
                                return false;
                            }
                        }
                    } else {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * Checks recursively whether any element is <code>null</code>.
     *
     * @param args
     *            one or more objects of any type
     * @return true, returns <code>true</code> if any of the element is <code>null</code>, otherwise <code>false</code>
     */
    @SuppressWarnings({ "rawtypes" })
    public static boolean anyNullRecursive(Object... args) {
        if (isNull(args)) {
            return true;
        }
        for (Object a : args) {
            if (isNull(a)) {
                return true;
            }
            if (a instanceof Object[]) {
                for (Object x : (Object[]) a) {
                    if (anyNullRecursive(x)) {
                        return true;
                    }
                }
            } else if (a instanceof Collection) {
                for (Object x : (Collection) a) {
                    if (anyNullRecursive(x)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Checks recursively whether all the elements are not <code>null</code>.
     *
     * @param args
     *            one or more objects of any type
     * @return true, returns <code>true</code> if all the elements are not <code>null</code>, otherwise <code>false</code>
     */
    public static boolean noneNullRecursive(Object... args) {
        return !anyNullRecursive(args);
    }

}
