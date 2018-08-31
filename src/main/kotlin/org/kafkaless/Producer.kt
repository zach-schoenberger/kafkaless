package org.kafkaless

import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.sendBlocking
import org.apache.commons.io.IOUtils
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.io.BufferedReader
import java.io.File
import java.util.*

suspend fun produceRecords(properties: Properties,
                           topic: String,
                           channel: Channel<String>
) {
    val kafkaProducer = KafkaProducer<String, String>(properties)
    while(!channel.isClosedForSend) {
        val record = channel.receive()
        if(record == IOUtils.EOF.toByte().toString()) {
            break
        }
        kafkaProducer.send(ProducerRecord(topic, record))
    }
}

suspend fun readRecordsFromStream(inputStream: BufferedReader,
                                  channel: Channel<String>
) {
    inputStream.use {
        it.lineSequence().forEach {
            channel.sendBlocking(it)
        }
    }
    channel.sendBlocking(IOUtils.EOF.toByte().toString())
}
