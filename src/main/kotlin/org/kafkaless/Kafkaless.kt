package org.kafkaless

import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.text.StringSubstitutor
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.io.File
import java.util.*
import kotlin.system.exitProcess

enum class KafkaOffsets {
    Earliest,
    Latest,
    None
}

fun main(args: Array<String>) {

    val options = Options()
    options.addRequiredOption("b", "broker", true, "broker list [address:port]")
    options.addRequiredOption("t", "topic", true, "topic")
    options.addOption("s", "ssl", false, "enable ssl")
    options.addOption("o", "offset", true, "offset to start at [beginning|end|stored]")
    options.addOption("P", "publish", false, "publish to topic. requires either -l or -i")
    options.addOption("l", "file", true, "file of lines to publish to kafka topic")
    options.addOption("i", false, "read stdin to publish to kafka topic")
    options.addOption("X", true, "additional kafka properties")
//    options.addOption("g", "group", true, "consumer group name to use")
    options.addOption("r", "regex", true, "regex to filter by")
    options.addOption("F", "full", false, "print full consumer record")
    options.addOption("c", "count", true, "number of records to consume")
    options.addOption("p", "password", true, "ssl password")
    options.addOption("u", "user", true, "ssl username")
    options.addOption("h", "help", false, "help")

    val propertiesOption = Option.builder()
            .argName("properties")
            .hasArg()
            .longOpt("properties")
            .desc("kafka properties")
            .build()
    options.addOption(propertiesOption)

    options.addOption(Option.builder()
            .longOpt("pause")
            .desc("puases stream after displaying record. must press enter for new record")
            .build())

    val parser = DefaultParser()
    val cmd = try {
        parser.parse(options, args)!!
    } catch (ignore: Exception){
        printHelp(options)
        exitProcess(1)
    }

    when {
        cmd.hasOption('h') -> {
            printHelp(options)
            exitProcess(0)
        }
        cmd.hasOption('P') && !(cmd.hasOption('i') || cmd.hasOption('l')) -> {
            printHelp(options)
            exitProcess(1)
        }
        !(cmd.hasOption('b') && cmd.hasOption('t')) -> {
            printHelp(options)
            exitProcess(1)
        }
        (cmd.hasOption('s') && !(cmd.hasOption('u') && cmd.hasOption('p'))) -> {
            printHelp(options)
            exitProcess(1)
        }
    }

    val defaultProps = Properties()
    defaultProps.loadPropertiesFile("default.properties")

    if(cmd.hasOption('s')) {
        defaultProps.loadPropertiesFile("default-ssl.properties")

        try {
            val valuesMap = HashMap<String, String>()
            valuesMap["username"] = cmd.getOptionValue('u')
            valuesMap["password"] = cmd.getOptionValue('p')

            val sub = StringSubstitutor(valuesMap)
            defaultProps["client.id"] = sub.replace(defaultProps["client.id"])
            defaultProps["sasl.jaas.config"] = sub.replace(defaultProps["sasl.jaas.config"])
        }
        catch (ignore: Exception) {
        }
    }

    if(cmd.hasOption("properties")) {
        defaultProps.loadPropertiesFile(cmd.getOptionValue("properties"))
    }

    if(cmd.hasOption('X')) {
        cmd.getOptionValues('X').map { it.split("=") }.forEach { defaultProps[it[0]] = it[1] }
    }

    defaultProps["bootstrap.servers"] = cmd.getOptionValue('b')
    val useGroup = cmd.hasOption('g')
    if(useGroup) {
        defaultProps["group.id"] = cmd.getOptionValue('g')
        defaultProps["enable.auto.commit"] = "true"
    } else {
//        defaultProps["group.id"] = UUID.randomUUID().toString()
        defaultProps["enable.auto.commit"] = "false"
    }

    val offset = if(cmd.hasOption("o")) {
        when(cmd.getOptionValue("o")) {
            "beginning" -> KafkaOffsets.Earliest
            "end" -> KafkaOffsets.Latest
            "stored" -> KafkaOffsets.None
            else -> KafkaOffsets.None
        }
    } else {
        KafkaOffsets.None
    }

    if(cmd.hasOption('P')) {
        val inputStream = if(cmd.hasOption("l")) {
            File(cmd.getOptionValue("l")).bufferedReader()
        } else {
            System.`in`.bufferedReader()
        }

        val channel = Channel<String>(1000)
        val readJob = async {
            readRecordsFromStream(inputStream = inputStream, channel = channel)
        }
        val writeJob = async {
            produceRecords(properties = defaultProps, topic = cmd.getOptionValue('t'), channel = channel)
        }

        runBlocking {
            readJob.await()
            writeJob.await()
        }
    } else {
        val autoFollow = !cmd.hasOption("pause")
        val channel : Channel<ConsumerRecord<String,String>> = when(autoFollow) {
            true -> Channel(1000)
            false -> Channel()
        }

        launch {
            consumeRecords(properties = defaultProps,
                    topic = cmd.getOptionValue('t'),
                    useGroup = useGroup,
                    offset = offset,
                    channel = channel
            )
        }
        launch {
            processRecords(channel = channel,
                    fullRecord = cmd.hasOption("F"),
                    filterRegex = cmd.getOptionValue("r"),
                    follow = autoFollow,
                    count = cmd.getOptionValue("c")?.toLong() ?: Long.MAX_VALUE
            )
        }
    }
    while (true) {
        Thread.sleep(100)
    }
}

fun Properties.loadPropertiesFile(file: String) {
    try{
        this.load(ClassLoader.getSystemResourceAsStream(file))
    }
    catch (e: Exception) {
        System.err.println(e.message)
        exitProcess(1)
    }
}

fun printHelp(options: Options){
    val formatter = HelpFormatter()
    formatter.printHelp("kafkaless", "kafkaless", options, "", true)
}

