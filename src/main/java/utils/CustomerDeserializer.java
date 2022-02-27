package utils;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.IntNode;
import com.github.kafkastreams.applications.Customer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;

public class CustomerDeserializer extends StdDeserializer<Customer> {

    public CustomerDeserializer() {
        this(null);
    }

    public CustomerDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public Customer deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JacksonException {
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);
        int id = (Integer) ((IntNode) node.get("id")).numberValue();
        String customerName = node.get("customerName").asText();
        String depositedAtTime = node.get("depositedAtTime").asText();
        int amountDeposited = (Integer) ((IntNode) node.get("amountDeposited")).numberValue();

        return new Customer(id, customerName, depositedAtTime,amountDeposited);
    }
}
