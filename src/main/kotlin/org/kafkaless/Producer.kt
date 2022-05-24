package org.kafkaless

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.runBlocking
import org.apache.commons.cli.CommandLine
import org.apache.commons.io.IOUtils
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.io.BufferedReader
import java.io.File
import java.util.*

fun startProducer(defaultProps: Properties, cmd: CommandLine) {
    val inputStream = if(cmd.hasOption("l")) {
        File(cmd.getOptionValue("l")).bufferedReader()
    } else {
        System.`in`.bufferedReader()
    }

    val channel = Channel<String>(1000)
    val writeJob = GlobalScope.async {
        produceRecords(properties = defaultProps, topic = cmd.getOptionValue('t'), channel = channel)
    }

    readRecordsFromStream(inputStream = inputStream, channel = channel)

    runBlocking {
        writeJob.await()
    }
}

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

fun readRecordsFromStream(inputStream: BufferedReader,
                                  channel: Channel<String>
) {
    inputStream.use {
        it.lineSequence().forEach {
            channel.trySendBlocking(it)
        }
    }
    channel.trySendBlocking(IOUtils.EOF.toByte().toString())
}
