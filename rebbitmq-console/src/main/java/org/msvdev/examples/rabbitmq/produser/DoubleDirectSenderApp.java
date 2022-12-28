package org.msvdev.examples.rabbitmq.produser;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class DoubleDirectSenderApp {

    private final static String EXCHANGER_NAME = "DoubleDirectExchanger";


    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (   // Создание соединения и открытие канала связи
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel()
        ) {
            // Определение обменника
            channel.exchangeDeclare(EXCHANGER_NAME, BuiltinExchangeType.DIRECT);

            // Отправка сообщения в обменник с ключём "java"
            channel.basicPublish(EXCHANGER_NAME, "java", null, "java msg".getBytes(StandardCharsets.UTF_8));

            // Отправка сообщения в обменник с ключём "с++"
            channel.basicPublish(EXCHANGER_NAME, "c++", null, "c++ msg".getBytes(StandardCharsets.UTF_8));

            // Отправка сообщения в обменник с ключём "php"
            channel.basicPublish(EXCHANGER_NAME, "php", null, "php msg".getBytes(StandardCharsets.UTF_8));

            System.out.println("OK");
        }
    }
}
