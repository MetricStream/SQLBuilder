/*
 * Copyright © 2000-2020, MetricStream, Inc. All rights reserved.
 */
package com.metricstream.jdbc;

import java.io.Reader;
import java.io.StringReader;

public class LongString {
    
    private final String data;
    
    public LongString(String _data) {
        data = _data;
    }
        
    public Reader getReader() {
        return new StringReader(data);
    }
    
    public String getData() {
        return data;
    }
    
    @Override
    public boolean equals(Object _obj) {
        if (_obj instanceof LongString) {
            return data.equals(((LongString) _obj).getData());
        }
        return false;
    }
}
