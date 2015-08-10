package com.novemberain.quartz.mongodb;

import org.apache.commons.codec.binary.Base64;
import org.quartz.JobDataMap;

import java.io.*;
import java.util.Map;

public class SerialUtils {

    public static String jobDataToString(JobDataMap jobDataMap) throws IOException {
        try {
            byte[] bytes = stringMapToBytes(jobDataMap.getWrappedMap());
            return Base64.encodeBase64String(bytes);
        } catch (NotSerializableException e) {
            throw new NotSerializableException(
                    "Unable to serialize JobDataMap for insertion into " +
                    "database because the value of property '" +
                    getKeyOfNonSerializableStringMapEntry(jobDataMap.getWrappedMap()) +
                    "' is not serializable: " + e.getMessage());
        }
    }

    private static byte[] stringMapToBytes(Object object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(baos);
        out.writeObject(object);
        out.flush();
        return baos.toByteArray();
    }

    public static String getKeyOfNonSerializableStringMapEntry(Map<String, ?> data) {
        for (Map.Entry<String, ?> entry : data.entrySet()) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                ObjectOutputStream out = new ObjectOutputStream(baos);
                out.writeObject(entry.getValue());
                out.flush();
            } catch (IOException e) {
                return entry.getKey();
            }
        }
        return null;
    }

    public static void jobDataMapFromString(JobDataMap jobDataMap, String clob) throws IOException {
        try {
            byte[] bytes = Base64.decodeBase64(clob);
            Map<String, ?> map = stringMapFromBytes(bytes);
            jobDataMap.putAll(map);
            jobDataMap.clearDirtyFlag();
        } catch (NotSerializableException e) {
            throw new NotSerializableException(
                    "Unable to serialize JobDataMap for insertion into " +
                    "database because the value of property '" +
                    getKeyOfNonSerializableStringMapEntry(jobDataMap.getWrappedMap()) +
                    "' is not serializable: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static Map<String, ?> stringMapFromBytes(byte[] bytes)
            throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais);
        @SuppressWarnings("unchecked")
        Map<String, ?> map = (Map<String, ?>) ois.readObject();
        ois.close();
        return map;
    }
}
