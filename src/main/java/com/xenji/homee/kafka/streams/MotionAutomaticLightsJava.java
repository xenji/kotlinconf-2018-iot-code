package com.xenji.homee.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;

import java.util.Objects;
import java.util.Properties;

public class MotionAutomaticLightsJava {

    public static void main(String[] args) {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "iot-light-on-motion");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        var builder = new StreamsBuilder();
        var attributeSerde = new AttributeSerde();
        builder.stream("attribute_events", Consumed.with(Serdes.Long(), attributeSerde))
                    // Only motion alerts
                    .filter((key, value) -> Objects.equals(
                            AttributeType.MotionAlarm.getValue(),
                            value.getType())
                    )
                    // From the same node ID
                    .groupBy((key, value) -> value.getNode_id())
                    // Within a two second window
                    .windowedBy(SessionWindows.with(2000))
                    // Count the amount of single motion events
                    .count()
                    // Proceed if we have "a lot of motion"
                    .filter((nodeId, amount) -> amount > 3)
                    // Get back the node id as value
                    .mapValues((readOnlyKey, value) -> readOnlyKey.key())
                    // Turn windowed stream back into normal stream
                    .toStream()
                // Output into another topic that trigger lights in the same group as the node id
                .to("notify_group_of_node", Produced.valueSerde(Serdes.Integer()));

        new KafkaStreams(builder.build(), props).start();
    }
}
