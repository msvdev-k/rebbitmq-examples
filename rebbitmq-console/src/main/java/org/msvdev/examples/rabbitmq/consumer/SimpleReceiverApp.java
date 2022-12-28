package org.msvdev.examples.rabbitmq.consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class SimpleReceiverApp {

    private final static String QUEUE_NAME = "SimpleReceiverQueue";


    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        // Создание соединения и открытие канала связи
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // Создание очереди
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println("[*] Waiting for message...");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.printf("[x] Received '%s'\n", message);
            System.out.printf("[.] Thread: %s\n", Thread.currentThread().getName());
        };

        // Прослушивание сообщений, поступающих в очередь
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {});
    }
}
