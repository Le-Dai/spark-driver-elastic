package org.wxstc.spark.utils;

import java.io.*;
import java.util.Optional;

public class ByteArrayUtils {
    public static<T> Optional<byte[]> objectToBytes(T obj){
        byte[] bytes = null;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream sOut;
        try {
            sOut = new ObjectOutputStream(out);
            sOut.writeObject(obj);
            sOut.flush();
            bytes= out.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Optional.ofNullable(bytes);
    }

    public static<T> Optional<T> bytesToObjectMemory(byte[] bytes) {
        T t = null;
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        ObjectInputStream sIn = null;
        try {
            //use memory classloader load class
            sIn = new MyObjectInputStream(in);
            t = (T)sIn.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                sIn.close();
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
                sIn = null;
                in =  null;
            }
        }
        return Optional.ofNullable(t);
    }

    public static<T> Optional<T> bytesToObject(byte[] bytes) {
        T t = null;
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        ObjectInputStream sIn = null;
        try {
            sIn = new ObjectInputStream(in);
            t = (T)sIn.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                sIn.close();
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
                sIn = null;
                in =  null;
            }
        }
        return Optional.ofNullable(t);
    }

    public static<T> Optional<T> inputStreamToObject(BufferedInputStream inputStream) {
        T t = null;
        ObjectInputStream sIn;
        try {
            byte[] bytes = new byte[inputStream.available()];
            inputStream.read(bytes, 0, bytes.length);
//            byte buf[] = (byte[]) new ObjectInputStream(inputStream).readObject();
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            sIn = new ObjectInputStream(in);
            t = (T)sIn.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Optional.ofNullable(t);
    }

    public static byte[] readBytes(InputStream inputStream) {
        try {
            byte[] bytes = new byte[inputStream.available()];
            inputStream.read(bytes, 0, bytes.length);
            return bytes;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static byte[] getClassBytes(Class<?> clazz){
        String filename = clazz.getName().replaceAll("\\.","/") + ".class";
        InputStream input = null;
        try {
            input = clazz.getClassLoader().getResource(filename).openStream();//.getContent();
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] res = ByteArrayUtils.readBytes(input);
        return res;
    }
}
