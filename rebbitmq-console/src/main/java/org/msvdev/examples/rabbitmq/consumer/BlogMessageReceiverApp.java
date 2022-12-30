package org.msvdev.examples.rabbitmq.consumer;

import com.rabbitmq.client.*;
import org.msvdev.examples.rabbitmq.produser.BlogMessageSenderApp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;


public class BlogMessageReceiverApp {

    /**
     * Подписчик, которого интересуют статьи по определённым языкам.
     * Подписчик должен при запуске ввести команду 'set_topic php',
     * после чего начнёт получать сообщения из очереди с соответствующей
     * темой 'php'
     */
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        // Создание соединения и открытие канала связи
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();


        // Создание и получение имени временной очереди, связанной с соединением
        String queueName = channel.queueDeclare().getQueue();
        System.out.printf("My queue name: %s\n", queueName);


        new Thread(() -> {
            try (
                    BufferedReader input = new BufferedReader(new InputStreamReader(System.in))
            ) {
                while (true) {
                    System.out.print("[*] Введите команду:");
                    String[] command = input.readLine().split("\\s+", 2);

                    if (command[0].equalsIgnoreCase("set_topic")) {
                        channel.queueBind(queueName, BlogMessageSenderApp.EXCHANGER_NAME, command[1]);
                        System.out.printf("[*] Вы подписались на новости о %s\n", command[1]);
                    }

                    if (command[0].equalsIgnoreCase("del_topic")) {
                        channel.queueUnbind(queueName, BlogMessageSenderApp.EXCHANGER_NAME, command[1]);
                        System.out.printf("[*] Подписка на %s удалена\n", command[1]);
                    }

                    if (command[0].equalsIgnoreCase("q")) {
                        break;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();


        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

            System.out.print("\r                        \r");
            System.out.printf("[%s] %s\n", delivery.getEnvelope().getRoutingKey(), message);
            System.out.print("[*] Введите команду:");
        };

        // Прослушивание сообщений, поступающих в очередь
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }
}