package utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.Serializable;
import java.util.Map;

public class CustomDeserializer<T extends Serializable> implements Deserializer<T> {

    private ObjectMapper objectMapper = new ObjectMapper();
    public static final String VALUE_CLASS_NAME_CONFIG = "value.class.name";

    public CustomDeserializer(){}
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(String topic, byte[] objectData) {
        System.out.println("objectData to be deserialized "+objectData);
        return (objectData == null) ? null : (T) SerializationUtils.deserialize(objectData);
    }

    @Override
    public void close() {
    }
}
