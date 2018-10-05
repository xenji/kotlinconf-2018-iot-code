package com.xenji.homee.kafka.streams

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG
import org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.streams.kstream.*
import java.time.Duration

private val configuration = mapOf(
    APPLICATION_ID_CONFIG to "iot-light-on-motion",
    BOOTSTRAP_SERVERS_CONFIG to "localhost:9092"
)

private val eventStreamConsumedWith = Consumed.with(Serdes.Long(), AttributeSerde())

typealias StreamProcBuilder = (StreamsBuilder.() -> Unit) -> KafkaStreams
typealias ConfigMap = Map<String, String>

fun main(args: Array<String>) {
    val streamProc = configuredStreamContext(config = configuration)
    streamProc {
        with(streamOfMotionEvents()) {
            whenNodeIdOccursMoreThan(times = 3, per = Duration.ofSeconds(2))
                .notifyLightsInNodeGroup()
        }
    }
}

fun configuredStreamContext(
    config: ConfigMap,
    autoStart: Boolean = true
): StreamProcBuilder = {
    val ks = KafkaStreams(
        StreamsBuilder().apply(it).build(),
        config.toProperties()
    )
    if (autoStart) ks.start()
    ks
}

fun StreamsBuilder.streamOfMotionEvents(
    topic: String = "attribute_events"
): KStream<Long, Attribute> = stream(topic, eventStreamConsumedWith)
    .filter { _, value -> value.type == AttributeType.MotionAlarm.value }

fun KStream<Long, Attribute>.whenNodeIdOccursMoreThan(
    times: Int,
    per: Duration
): KTable<Windowed<Int>, Long> = groupBy { _, (_, _, node_id) -> node_id }
    .windowedBy(SessionWindows.with(per.toMillis()))
    .count()
    .filter { _, amount -> amount > times }

fun KTable<Windowed<Int>, Long>.notifyLightsInNodeGroup(
    topic: String = "notify_lights_in_group_of_node"
) = mapValues { readOnlyKey, _ -> readOnlyKey.key() }
    .toStream()
    .to(topic, Produced.valueSerde<Windowed<Int>, Int>(Serdes.Integer()))
