package utils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.github.kafkastreams.applications.Customer;

import java.io.IOException;

public class CustomerSerializer extends StdSerializer<Customer> {

    public CustomerSerializer() {
        this(null);
    }
    protected CustomerSerializer(Class<Customer> t) {
        super(t);
    }

    @Override
    public void serialize(Customer customer, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeNumberField("id", customer.getId());
        jsonGenerator.writeStringField("customerName", customer.getCustomerName());
        jsonGenerator.writeStringField("depositedAtTime", customer.getDepositedAt());
        jsonGenerator.writeNumberField("amountDeposited", customer.getAmountDeposited());
        jsonGenerator.writeEndObject();
    }
}
