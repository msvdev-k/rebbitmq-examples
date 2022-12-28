package org.msvdev.examples.rabbitmq.consumer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class TaskReceiverApp {

    private final static String TASK_QUEUE_NAME = "TaskReceiverQueue";
    private final static String TASK_EXCHANGER_NAME = "TaskExchanger";


    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        // Создание соединения и открытие канала связи
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();


        // Определение обменника
        channel.exchangeDeclare(TASK_EXCHANGER_NAME, BuiltinExchangeType.FANOUT);

        // Создание очереди
        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

        // Привязка очереди к обменнику без ключа
        channel.queueBind(TASK_QUEUE_NAME, TASK_EXCHANGER_NAME, "");
        System.out.println("[*] Waiting for message...");

        // Максимальное кол-во задач выбираемых из очереди за один раз
        channel.basicQos(3);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.printf("[x] Received '%s'\n", message);
//            System.out.printf("[.] Thread: %s\n", Thread.currentThread().getName());

//            if (true) {
//                throw new RuntimeException("Oops");
//            }

            doWork(message);
            System.out.println("[x] Done");

            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        // Прослушивание сообщений, поступающих в очередь
        channel.basicConsume(TASK_QUEUE_NAME, false, deliverCallback, consumerTag -> {});
    }


    private static void doWork(String message) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
