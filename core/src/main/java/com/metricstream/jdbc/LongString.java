/*
 * Copyright © 2020-2021, MetricStream, Inc. All rights reserved.
 */
package com.metricstream.jdbc;

import java.io.Reader;
import java.io.StringReader;


public class LongString {
    
    private final String data;
    
    public LongString(String data) {
        this.data = data;
    }
        
    public Reader getReader() {
        return new StringReader(data);
    }
    
    public String getData() {
        return data;
    }
    
    @Override
    public boolean equals(Object object) {
        if (object instanceof LongString) {
            return data.equals(((LongString) object).getData());
        }
        return false;
    }
}
