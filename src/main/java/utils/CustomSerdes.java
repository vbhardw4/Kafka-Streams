package utils;

import com.github.kafkastreams.applications.Customer;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;


public class CustomSerdes extends Serdes.WrapperSerde<Customer> {

    static CustomSerializer<Customer> serializer = new CustomSerializer<Customer>();
    static CustomDeserializer<Customer> deserializer = new CustomDeserializer<Customer>();

    public CustomSerdes() {
        super(new CustomSerializer<Customer>(), new CustomDeserializer<Customer>());
    }

    public static Serde<Customer> getCustomerSerdes() {
        return Serdes.serdeFrom(serializer,deserializer);
    }
}
