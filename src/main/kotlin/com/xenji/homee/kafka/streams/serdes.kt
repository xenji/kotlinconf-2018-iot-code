package com.xenji.homee.kafka.streams

import com.beust.klaxon.Klaxon
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import java.io.ByteArrayInputStream

private val klaxon = Klaxon()

class AttributeSerializer : Serializer<Attribute> {
    override fun configure(configs: MutableMap<String, *>, isKey: Boolean) = Unit
    override fun close() = Unit
    override fun serialize(topic: String, data: Attribute?): ByteArray? =
        if (data == null) null else klaxon.toJsonString(data).toByteArray()
}

class AttributeDeserializer : Deserializer<Attribute> {
    override fun configure(configs: MutableMap<String, *>, isKey: Boolean) = Unit
    override fun close() = Unit
    override fun deserialize(topic: String?, data: ByteArray?): Attribute? =
        if (data == null) null else klaxon.parse<Attribute>(ByteArrayInputStream(data))
}

class AttributeSerde : Serde<Attribute> {
    override fun configure(configs: MutableMap<String, *>, isKey: Boolean) = Unit
    override fun close() = Unit
    override fun deserializer() = AttributeDeserializer()
    override fun serializer() = AttributeSerializer()
}
