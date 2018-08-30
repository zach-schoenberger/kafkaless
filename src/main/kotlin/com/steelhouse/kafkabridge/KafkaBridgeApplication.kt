package com.steelhouse.kafkabridge

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaBridgeApplication

fun main(args: Array<String>) {
    runApplication<KafkaBridgeApplication>(*args)
}
