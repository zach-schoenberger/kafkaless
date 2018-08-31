package org.kafkaless

import kotlinx.coroutines.experimental.channels.Channel
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.util.*

suspend fun consumeRecords(properties: Properties,
                           topic: String,
                           offset: KafkaOffsets,
                           useGroup: Boolean,
                           channel: Channel<ConsumerRecord<String, String>>
) {
    val kafkaConsumer = KafkaConsumer<String,String>(properties)
    if(useGroup) {
        kafkaConsumer.subscribe(listOf(topic))
    } else {
        val partitions = kafkaConsumer.partitionsFor(topic).map { TopicPartition(it.topic(), it.partition()) }
        kafkaConsumer.assign(partitions)
    }

    var records = kafkaConsumer.poll(100)

    when(offset){
        KafkaOffsets.Earliest -> {
            kafkaConsumer.seekToBeginning(kafkaConsumer.assignment())
        }
        KafkaOffsets.Latest -> {
            kafkaConsumer.seekToEnd(kafkaConsumer.assignment())
        }
        else -> {}
    }

    while(true) {
        records = kafkaConsumer.poll(100)
        when {
            records.isEmpty -> {
                Thread.sleep(100)
            }
            else -> {
                kafkaConsumer.pause(kafkaConsumer.assignment())
                records.forEach {
                    channel.send(it)
                }
                kafkaConsumer.resume(kafkaConsumer.assignment())
            }
        }
    }
}

suspend fun processRecords(channel: Channel<ConsumerRecord<String, String>>,
                           fullRecord: Boolean,
                           filterRegex: String?,
                           follow: Boolean,
                           count: Long
) {
    val regex = filterRegex?.let {
        filterRegex.removeSurrounding("'")
        Regex(filterRegex)
    }

    val inputChannel = when(follow) {
        true -> null
        false -> System.`in`.bufferedReader()
    }

    val buf = CharArray(1)
    var curCount = 0L
    while (curCount < count) {
        val consumerRecord = channel.receive()
        val record = if (fullRecord) {
            consumerRecord.toString()
        } else {
            consumerRecord.value()
        }

        if (regex?.containsMatchIn(record) != false) {
            curCount++
            println(record)
        }

        inputChannel?.read(buf)
    }
}
