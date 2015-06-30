package com.geospatial.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SystemConfiguration {

    private ClassLoader loader;
    private InputStream inputStream;
    private Properties  properties;

    public ClassLoader getLoader() {
        return loader;
    }

    public void setLoader(ClassLoader loader) {
        this.loader = loader;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public void setInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }
    
    public Properties getProperties() {
        return properties;
    }

    public Properties loadSystemProperties() {
        ClassLoader loader = SystemConfiguration.class.getClassLoader();
        InputStream resourceProperties = loader.getResourceAsStream("app.properties");
        Properties p = new Properties();
        try {
            p.load(resourceProperties);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Problem in loading properties file");
        }
        return p;
    }

}
