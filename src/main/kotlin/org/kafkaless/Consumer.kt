package org.kafkaless

//import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.commons.cli.CommandLine
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.util.*

fun startConsumer(defaultProps: Properties, cmd: CommandLine) {
    val useGroup = cmd.hasOption('g')
    if (useGroup) {
        defaultProps["group.id"] = cmd.getOptionValue('g')
        defaultProps["enable.auto.commit"] = "true"
    } else {
//        defaultProps["group.id"] = UUID.randomUUID().toString()
        defaultProps["enable.auto.commit"] = "false"
    }

    val (offset, index, ts) = if (cmd.hasOption("o")) {
        val value = cmd.getOptionValue("o")
        when {
            value == "beginning" -> Triple(KafkaOffsets.Earliest, null, null)
            value == "end" -> Triple(KafkaOffsets.Latest, null, null)
            value == "stored" -> Triple(KafkaOffsets.None, null, null)
            value.startsWith("@") -> Triple(KafkaOffsets.OffsetTs, null, value.trimStart('@').toLongOrNull())
            else -> Triple(KafkaOffsets.Offset, value.toLongOrNull(), null)
        }
    } else {
        Triple(KafkaOffsets.None, null, null)
    }

    val autoFollow = !cmd.hasOption("pause")
    val channel: Channel<ConsumerRecord<String, String>> = when (autoFollow) {
        true -> Channel(1000)
        false -> Channel()
    }

    val shutdownChan = Channel<Any>(0)

    GlobalScope.launch {
        try {
            consumeRecords(
                properties = defaultProps,
                topic = cmd.getOptionValue('t'),
                useGroup = useGroup,
                offset = offset,
                channel = channel,
                offsetIndex = index,
                offsetTs = ts
            )
        } finally {
            shutdownChan.send(Any())
        }
    }
    GlobalScope.launch {
        onEvent(
            channel = channel,
            fullRecord = cmd.hasOption("F"),
            filterRegex = cmd.getOptionValue("r"),
            follow = autoFollow,
            count = cmd.getOptionValue("c")?.toLong() ?: Long.MAX_VALUE
        )
    }

    runBlocking {
        shutdownChan.receive()
    }
}

suspend fun consumeRecords(
    properties: Properties,
    topic: String,
    offset: KafkaOffsets,
    offsetIndex: Long?,
    offsetTs: Long?,
    useGroup: Boolean,
    channel: Channel<ConsumerRecord<String, String>>
) {
    val kafkaConsumer = KafkaConsumer<String, String>(properties)
    if (useGroup) {
        kafkaConsumer.subscribe(listOf(topic))
    } else {
        val partitions = kafkaConsumer.partitionsFor(topic).map { TopicPartition(it.topic(), it.partition()) }
        kafkaConsumer.assign(partitions)
    }

//    This is done to initialize the consumer with the above assignments. It never appears to returns records
//    It is required for group consuming to allow the seek to work properly
    kafkaConsumer.poll(100)

    when (offset) {
        KafkaOffsets.Earliest -> {
            kafkaConsumer.seekToBeginning(kafkaConsumer.assignment())
        }
        KafkaOffsets.Latest -> {
            kafkaConsumer.seekToEnd(kafkaConsumer.assignment())
        }
        KafkaOffsets.Offset -> {
            val partitionOffsets = when {
                (offsetIndex!! < 0) -> {
                    kafkaConsumer.endOffsets(kafkaConsumer.assignment(), Duration.ofMillis(50000)).map {
                        Pair(it.key, it.value - offsetIndex)
                    }.toMap()
                }
                else -> {
                    kafkaConsumer.assignment().associateWith { offsetIndex.toLong() }
                }
            }

//            setting the ts based ts for each partition
            for (partitionTime in partitionOffsets) {
                kafkaConsumer.seek(partitionTime.key, partitionTime.value)
            }

//             needed to initialize the offsets we just set in the cluster
            kafkaConsumer.poll(Duration.ofMillis(100))
//            committing the offsets we just initialized
            kafkaConsumer.commitSync()
        }
        KafkaOffsets.OffsetTs -> {
            val assignment = kafkaConsumer.assignment()
            val seekTimes = assignment.associateWith { offsetTs!! }
            val partitionTimes = kafkaConsumer.offsetsForTimes(seekTimes, Duration.ofMillis(50000))

//            setting the ts based ts for each partition
            for (partitionTime in partitionTimes) {
                kafkaConsumer.seek(partitionTime.key, partitionTime.value.offset())
            }

//             needed to initialize the offsets we just set in the cluster
            kafkaConsumer.poll(100)
//            committing the offsets we just initialized
            kafkaConsumer.commitSync()
        }
        else -> {

        }
    }
    try {
        while (true) {
            val records = kafkaConsumer.poll(Duration.ofMillis(100))

            when {
                records.isEmpty -> {
                    delay(100)
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
    } catch (_: Exception) {
    }
}

suspend fun onEvent(
    channel: Channel<ConsumerRecord<String, String>>,
    fullRecord: Boolean,
    filterRegex: String?,
    follow: Boolean,
    count: Long
) {
    val regex = filterRegex?.let {
        filterRegex.removeSurrounding("'")
        Regex(filterRegex)
    }

    val inputChannel = when (follow) {
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

    channel.close()
}
