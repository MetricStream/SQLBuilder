/*
 * Copyright © 2020, MetricStream, Inc. All rights reserved.
 */
package com.metricstream.util;

import static com.metricstream.util.Check.allNull;
import static com.metricstream.util.Check.allNullRecursive;
import static com.metricstream.util.Check.anyNull;
import static com.metricstream.util.Check.anyNullRecursive;
import static com.metricstream.util.Check.firstContent;
import static com.metricstream.util.Check.hasContent;
import static com.metricstream.util.Check.noContent;
import static com.metricstream.util.Check.noneNull;
import static com.metricstream.util.Check.noneNullRecursive;
import static com.metricstream.util.Check.notNull;
import static com.metricstream.util.Check.nvl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;

import org.junit.jupiter.api.Test;


/**
 * Unit test for Check class
 */
public class CheckTest
{
    @Test
    public void testContent()
    {
        String s = "hello";
        String t = "there";
        String e = "";
        String p = " ";
        String n = null;
        assertTrue(noContent(n));
        assertFalse(hasContent(n));

        assertTrue(noContent(e));
        assertFalse(hasContent(e));

        assertTrue(hasContent(s));
        assertFalse(noContent(s));

        assertTrue(hasContent(p));
        assertFalse(noContent(p));

        int[] ia = null;
        assertTrue(noContent(ia));
        assertFalse(hasContent(ia));

        ia = new int[] {};
        assertTrue(noContent(ia));
        assertFalse(hasContent(ia));

        ia = new int[] { 4, 5 };
        assertTrue(hasContent(ia));
        assertFalse(noContent(ia));

        String[] sa = null;
        assertTrue(noContent(sa));
        assertFalse(hasContent(sa));

        sa = new String[] {};
        assertTrue(noContent(sa));
        assertFalse(hasContent(sa));

        sa = new String[] { "4", "5" };
        assertTrue(hasContent(sa));
        assertFalse(noContent(sa));

        ArrayList<Integer> al = null;
        assertTrue(noContent(al));
        assertFalse(hasContent(al));

        al = new ArrayList<Integer>();
        assertTrue(noContent(al));
        assertFalse(hasContent(al));

        al.add(3);
        assertTrue(hasContent(al));
        assertFalse(noContent(al));

        HashMap<String, String> hm = null;
        assertTrue(noContent(hm));
        assertFalse(hasContent(hm));

        hm = new HashMap<String, String>();
        assertTrue(noContent(hm));
        assertFalse(hasContent(hm));

        hm.put("foo", "bar");
        assertTrue(hasContent(hm));
        assertFalse(noContent(hm));

        assertFalse(allNull(s));
        assertTrue(notNull(s));
        assertTrue(noneNull(s));
        assertTrue(noneNull(s, s));
        assertFalse(noneNull(s, n));
        assertTrue(noneNull(s, s, s));
        assertFalse(noneNull(s, s, n));
        assertTrue(noneNull(s, s, s, s));
        assertFalse(noneNull(s, s, n, n));
        assertTrue(allNull(n, n));
        assertTrue(anyNull(n));
        assertTrue(anyNull(n, s));
        assertTrue(anyNull(n, s, s));
        assertTrue(anyNull(s, s, n));
        assertTrue(anyNull(s, s, n, s));
        assertTrue(anyNull(s, s, s, n));

        assertEquals(s, nvl(s, t));
        assertEquals(t, nvl(n, t));

        int i1 = 1;
        int i2 = 2;
        Integer I0 = null;
        Integer I1 = 1;
        Integer I2 = 2;
        assertEquals(I1, nvl(I1, I2));
        assertEquals(I2, nvl(I0, I2));
        assertEquals(i1, (int) nvl(I1, I2));
        assertEquals(s, nvl(n, n,n, s, t));
    }

    @Test
    public void firstContentTest()
    {
        assertEquals("hello", firstContent("hello", "world"));
        assertEquals("hello", firstContent(null, "hello", "world"));
        assertEquals(null, firstContent((String) null));

        HashMap<String, String> hm1 = new HashMap<>();
        HashMap<String, String> hm2 = new HashMap<>();
        hm2.put("hello", "world");
        assertEquals("world", firstContent(hm1, hm2).get("hello"));
        HashMap<Integer, Integer> hm3 = new HashMap<>();
        hm3.put(1,2);
        assertEquals(2, firstContent(hm1, hm3, hm2).get(1));
        
    }

    @SuppressWarnings({ "rawtypes", "serial", "unchecked" })
    @Test
    public void testAllNullRecursive() {
        assertEquals(false, allNullRecursive(1, null));
        assertEquals(true, allNullRecursive(null, null));

        assertEquals(false, allNullRecursive());
        assertEquals(true, allNullRecursive(null, null));
        assertEquals(false, allNullRecursive(1, null));
        assertEquals(false, allNullRecursive(new Long[] { new Long(1), null }, null));
        assertEquals(true, allNullRecursive(new Long[] { null }, null));
        assertEquals(false, allNullRecursive(new Long[] {}, null));

        assertEquals(false, allNullRecursive(new ArrayList<>()));
        assertEquals(true, allNullRecursive(new ArrayList() {

            {
                add(null);
            }
        }));
        assertEquals(false, allNullRecursive(new ArrayList() {

            {
                add(null);
                add(1);
            }
        }));
    }

    @SuppressWarnings({ "unchecked", "rawtypes", "serial" })
    @Test
    public void testAnyNullRecursive() {
        assertEquals(true, anyNullRecursive(1, null));
        assertEquals(false, anyNullRecursive(1, 2, 3));

        assertEquals(false, anyNullRecursive());
        assertEquals(true, anyNullRecursive(null, null));
        assertEquals(true, anyNullRecursive(new Object[] { 1, null }, null));
        assertEquals(false, anyNullRecursive(new Long[] { new Long(1), new Long(2) }, 3L));
        assertEquals(true, anyNullRecursive(new Long[] { null }, null));
        assertEquals(true, anyNullRecursive(new Long[] {}, null));

        assertEquals(false, anyNullRecursive(new ArrayList<>()));
        assertEquals(true, anyNullRecursive(new ArrayList() {

            {
                add(null);
            }
        }));
        assertEquals(true, anyNullRecursive(new ArrayList() {

            {
                add(null);
                add(1);
            }
        }));
    }

    @SuppressWarnings({ "unchecked", "rawtypes", "serial" })
    @Test
    public void testNoneNullRecursive() {
        assertEquals(false, noneNullRecursive(1, null));
        assertEquals(true, noneNullRecursive(1, 2, 3));

        assertEquals(true, noneNullRecursive());
        assertEquals(false, noneNullRecursive(null, null));
        assertEquals(false, noneNullRecursive(new Object[] { 1, null }, null));
        assertEquals(true, noneNullRecursive(new Long[] { new Long(1), new Long(2) }, 3L));
        assertEquals(false, noneNullRecursive(new Long[] { null }, null));
        assertEquals(false, noneNullRecursive(new Long[] {}, null));

        assertEquals(true, noneNullRecursive(new ArrayList<>()));
        assertEquals(false, noneNullRecursive(new ArrayList() {

            {
                add(null);
            }
        }));
        assertEquals(false, noneNullRecursive(new ArrayList() {

            {
                add(null);
                add(1);
            }
        }));
        assertEquals(true, noneNullRecursive(new ArrayList() {

            {
                add(1);
                add(2);
            }
        }));
    }
}
