package org.msvdev.examples.rabbitmq.produser;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class TaskSenderApp {

    private final static String TASK_EXCHANGER_NAME = "TaskExchanger";


    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (   // Создание соединения и открытие канала связи
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel()
        ) {
            // Определение обменника
            channel.exchangeDeclare(TASK_EXCHANGER_NAME, BuiltinExchangeType.FANOUT);

            for (int i = 0; i < 100; i++) {
                // Сообщение задачи
                String message = String.format("Task number %d", i);

                // Отправка сообщения в обменник
                channel.basicPublish(TASK_EXCHANGER_NAME, "",
                        MessageProperties.PERSISTENT_TEXT_PLAIN,
                        message.getBytes(StandardCharsets.UTF_8));

                System.out.printf("[x] Sent message: %s\n", message);
            }
        }
    }
}
