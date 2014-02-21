package com.nflabs.peloton2.kafka.elasticsearch;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class Context {
	Properties prop;

	Context( String file) throws Exception{
		prop = new Properties();
	    InputStream is = new FileInputStream(file);
	    prop.load(is);
	}

	public String getString(String key){
		return prop.getProperty(key);
	}

	public int getInt(String key){
		String value =  prop.getProperty(key);
		return Integer.getInteger(value, 1);
	}

}