package com.novemberain.quartz.mongodb;

import org.apache.commons.codec.binary.Base64;
import org.quartz.JobDataMap;

import java.io.*;
import java.util.Collections;
import java.util.Map;

public class SerialUtils {

    private static final String SERIALIZE_MESSAGE_FORMAT =
            "Unable to serialize JobDataMap for insertion into " +
            "database because the value of property '%s' " +
            "is not serializable: %s";

    public static String serialize(JobDataMap jobDataMap) throws IOException {
        try {
            byte[] bytes = stringMapToBytes(jobDataMap.getWrappedMap());
            return Base64.encodeBase64String(bytes);
        } catch (NotSerializableException e) {
            return rethrowEnhanced(jobDataMap, e);
        }
    }

    public static Map<String, ?> deserialize(JobDataMap jobDataMap, String clob) throws IOException {
        try {
            byte[] bytes = Base64.decodeBase64(clob);
            return stringMapFromBytes(bytes);
        } catch (NotSerializableException e) {
            rethrowEnhanced(jobDataMap, e);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return Collections.emptyMap();
    }

    private static byte[] stringMapToBytes(Object object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(baos);
        out.writeObject(object);
        out.flush();
        return baos.toByteArray();
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

    private static String rethrowEnhanced(JobDataMap jobDataMap, NotSerializableException e)
            throws NotSerializableException {
        final String key = getKeyOfNonSerializableStringMapEntry(jobDataMap.getWrappedMap());
        throw new NotSerializableException(
                String.format(SERIALIZE_MESSAGE_FORMAT, key, e.getMessage()));
    }

    private static String getKeyOfNonSerializableStringMapEntry(Map<String, ?> data) {
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
}
