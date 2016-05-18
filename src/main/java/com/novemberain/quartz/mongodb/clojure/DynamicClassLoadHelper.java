package com.novemberain.quartz.mongodb.clojure;

import clojure.lang.DynamicClassLoader;
import org.quartz.spi.ClassLoadHelper;

import java.io.InputStream;
import java.net.URL;

/**
 * Makes it possible for Quartz to load and instantiate jobs that are defined
 * using Clojure defrecord without AOT compilation.
 */
public class DynamicClassLoadHelper implements ClassLoadHelper {

    @Override
    public ClassLoader getClassLoader() {
        return new DynamicClassLoader();
    }

    @Override
    public URL getResource(String name) {
        return null;
    }

    @Override
    public InputStream getResourceAsStream(String name) {
        return null;
    }

    @Override
    public void initialize() {
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        return null;
    }

    @Override
    public <T> Class<? extends T> loadClass(String name, Class<T> clazz)
            throws ClassNotFoundException {
        return null;
    }
}
