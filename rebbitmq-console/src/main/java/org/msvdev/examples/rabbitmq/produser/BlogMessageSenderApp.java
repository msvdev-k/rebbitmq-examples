package org.msvdev.examples.rabbitmq.produser;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;


public class BlogMessageSenderApp {

    public final static String EXCHANGER_NAME = "BlogMessageSenderExchanger";


    /**
     * IT-блог, который публикует статьи по языкам программирования.
     * Если IT- блог в консоли пишет 'php some message', то 'some message'
     * отправляется в RabbitMQ с темой 'php', и это сообщение получают
     * подписчики этой темы
     */
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (   // Создание соединения и открытие канала связи
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();

                // Поток данных из консоли
                BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
        ) {

            // Определение обменника
            channel.exchangeDeclare(EXCHANGER_NAME, BuiltinExchangeType.DIRECT);

            System.out.print("Введите сообщение (q/Q - выход):");
            String message = input.readLine();
            while (!message.equalsIgnoreCase("q")) {

                String[] inputs = message.split("\\s+", 2);

                channel.basicPublish(EXCHANGER_NAME, inputs[0], null, inputs[1].getBytes(StandardCharsets.UTF_8));
                System.out.printf("[%s] Sent message: %s\n", inputs[0], inputs[1]);

                System.out.print("Введите сообщение (q/Q - выход):");
                message = input.readLine();
            }
        }
    }
}