package com.novemberain.quartz.mongodb.util;

import org.apache.commons.codec.binary.Base64;
import org.bson.types.Binary;
import org.quartz.Calendar;
import org.quartz.JobDataMap;
import org.quartz.JobPersistenceException;

import java.io.*;
import java.util.Collections;
import java.util.Map;

public class SerialUtils {

    private static final String SERIALIZE_MESSAGE_FORMAT =
            "Unable to serialize JobDataMap for insertion into " +
            "database because the value of property '%s' " +
            "is not serializable: %s";

    public static Object serialize(Calendar calendar) throws JobPersistenceException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try {
            ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
            objectStream.writeObject(calendar);
            objectStream.close();
            return byteStream.toByteArray();
        } catch (IOException e) {
            throw new JobPersistenceException("Could not serialize Calendar.", e);
        }
    }
    
    public static <T> T deserialize(Binary serialized, Class<T> clazz) throws JobPersistenceException {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(serialized.getData());
        try {
            ObjectInputStream objectStream = new ObjectInputStream(byteStream);
            Object deserialized = objectStream.readObject();
            objectStream.close();
            if(clazz.isInstance(deserialized)) {
                @SuppressWarnings("unchecked")
                T obj = (T)deserialized;
                return obj;
            }
        
            throw new JobPersistenceException("Deserialized object is not of the desired type");
        } catch (IOException | ClassNotFoundException e) {
            throw new JobPersistenceException("Could not deserialize.", e);
        }
    }

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
