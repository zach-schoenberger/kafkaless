package com.steelhouse.kafkabridge

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class KafkaService {
    @KafkaListener(topics = ["page-views"])
    fun process(event: ConsumerRecord<String, String>){
        if (event.value().contains("e039ef40-95e6-11e8-9283-9be6e0db2f54")) {
            println(event)
        }
    }
}
