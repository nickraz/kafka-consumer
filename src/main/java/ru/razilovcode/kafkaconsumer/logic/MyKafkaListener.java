package ru.razilovcode.kafkaconsumer.logic;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.razilovcode.kafkaconsumer.models.Person;

@Component
@Slf4j
public class MyKafkaListener {

    private ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "${mykafka.topic}", groupId = "${mykafka.groupId}")
    @SneakyThrows
    public void listen(String value) {

        Person person;
        System.out.println(objectMapper.writeValueAsString(new Person("nick", "razilov")));
        try {
            person = objectMapper.readValue(value, Person.class);
            log.info("Received Message in group foo: {}", person);
        } catch (RuntimeException e) {
            log.error(e.getMessage(), e);
        }

    }

}
