package org.kafkaless

import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.text.StringSubstitutor
import java.util.*
import kotlin.system.exitProcess

enum class KafkaOffsets {
    Earliest,
    Latest,
    Offset,
    OffsetTs,
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
    options.addOption("g", "group", true, "consumer group name to use")
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

    options.addOption(
        Option.builder()
            .longOpt("pause")
            .desc("puases stream after displaying record. must press enter for new record")
            .build()
    )

    val parser = DefaultParser()
    val cmd = try {
        parser.parse(options, args)!!
    } catch (ignore: Exception) {
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

    if (cmd.hasOption('s')) {
        defaultProps.loadPropertiesFile("default-ssl.properties")

        try {
            val valuesMap = HashMap<String, String>()
            valuesMap["username"] = cmd.getOptionValue('u')
            valuesMap["password"] = cmd.getOptionValue('p')

            val sub = StringSubstitutor(valuesMap)
            defaultProps["client.id"] = sub.replace(defaultProps["client.id"])
            defaultProps["sasl.jaas.config"] = sub.replace(defaultProps["sasl.jaas.config"])
        } catch (ignore: Exception) {
        }
    }

    cmd.getOptionValue("properties")?.let { defaultProps.loadPropertiesFile(it) }
    cmd.getOptionValues('X')?.map { it.split("=") }?.forEach { defaultProps[it[0]] = it[1] }
    defaultProps["bootstrap.servers"] = cmd.getOptionValue('b')

    when {
        cmd.hasOption('P') -> {
            startProducer(defaultProps, cmd)
        }
        else -> {
            startConsumer(defaultProps, cmd)
        }
    }
}

fun Properties.loadPropertiesFile(file: String) {
    try {
        this.load(ClassLoader.getSystemResourceAsStream(file))
    } catch (e: Exception) {
        System.err.println(e.message)
        exitProcess(1)
    }
}

fun printHelp(options: Options) {
    val formatter = HelpFormatter()
    formatter.printHelp("kafkaless", "kafkaless", options, "", true)
}

