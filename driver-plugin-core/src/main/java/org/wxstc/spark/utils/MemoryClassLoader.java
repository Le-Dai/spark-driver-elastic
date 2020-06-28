package org.wxstc.spark.utils;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;

public class MemoryClassLoader extends URLClassLoader {
    Map<String, byte[]> classes = new HashMap<>();
    Map<String, Map<String, byte[]>> deps = new HashMap<>();

    public MemoryClassLoader(Map<String, byte[]> classes){
        super(new URL[0], Thread.currentThread().getContextClassLoader());
        this.classes = classes;
    }

    public MemoryClassLoader(URL[] urls) {
        super(urls);
    }

    public void addOrUpdateClass(String name, byte[] classBytes){
        this.classes.put(name, classBytes);
    }

    public void addOrUpdateAllClass(Map<String, byte[]> classBytes){
        classBytes.forEach((k,v) -> {
            this.classes.put(k, v);
        });
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        try {
            Class<?> superClass = super.loadClass(name);
            return superClass;
        } catch (ClassNotFoundException e) {
            if(classes.get(name) == null) throw e;
            byte[] bytes = classes.get(name);
            return findClass(name, bytes);
        } catch (NoClassDefFoundError e2){
            if(classes.get(name) == null) throw new ClassNotFoundException(e2.getMessage().replaceAll("/", "."));
            byte[] bytes = classes.get(name);
            return findClass(name, bytes);
        }
    }

    public Class<?> findClass(String name, byte[] bytes){
        return defineClass(name, bytes, 0, bytes.length);
    }

    public Map<String, byte[]> getAllDepClass(String name){
        try {
            loadClass(name).newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return classes;
    }
}
