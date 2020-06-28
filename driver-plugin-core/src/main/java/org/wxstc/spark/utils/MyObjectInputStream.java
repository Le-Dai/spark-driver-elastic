package org.wxstc.spark.utils;

import org.wxstc.spark.network.server.ServerHandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

public class MyObjectInputStream extends ObjectInputStream {
    private MemoryClassLoader loader = ServerHandler.loader;
    public MyObjectInputStream(InputStream in) throws IOException {
        super(in);
    }

    protected MyObjectInputStream() throws IOException, SecurityException {
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
        Class<?> MemoryClass = loader.loadClass(desc.getName());
        if(MemoryClass == null)
            return super.resolveClass(desc);
        else return MemoryClass;
    }
}
