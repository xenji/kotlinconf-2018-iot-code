package com.xenji.homee.kafka.streams

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.SessionWindows
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.processor.TopicNameExtractor
import java.time.Duration
import java.util.*

fun main() {
    val props = Properties().apply {
        this[StreamsConfig.APPLICATION_ID_CONFIG] = "iot-light-on-motion"
        this[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    }
    val attributeSerde = AttributeSerde()

    KafkaStreams(StreamsBuilder().run {
        stream("attribute_events", Consumed.with(Serdes.Long(), attributeSerde))
            // Only motion alerts
            .filter { _, value -> value.type == AttributeType.MotionAlarm.value }
                // From the same node ID. Attribute is a data class, so we can use components here.
                .groupBy { _, (_, _, node_id) -> node_id }
                // Within a two second window
                .windowedBy(SessionWindows.with(Duration.ofSeconds(2)))
                // Count the amount of single motion events
                .count()
                // Proceed if we have "a lot of motion"
                .filter { _, amount -> amount > 3 }
                // Get back the node id as value
                .mapValues { readOnlyKey, _ -> readOnlyKey.key() }
                // Turn windowed stream back into normal stream
                .toStream()
            // Output into another topic that trigger lights in the same group as the node id
            .to({ _, _, _->  "notify_group_of_node"}, Produced.valueSerde<Windowed<Int>, Int>(Serdes.Integer()))
        build()
    }, props)
}
