package rabbitmq.consumer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class DoubleDirectReceiverApp {

    private final static String EXCHANGER_NAME = "DoubleDirectExchanger";


    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        // Создание соединения и открытие канала связи
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // Определение обменника
        channel.exchangeDeclare(EXCHANGER_NAME, BuiltinExchangeType.DIRECT);

        // Создание и получение имени временной очереди, связанной с соединением
        String queueName = channel.queueDeclare().getQueue();
        System.out.printf("My queue name: %s\n", queueName);

        // Привязка временной очереди к обменнику. Ключи: "php" и "java"
        channel.queueBind(queueName, EXCHANGER_NAME, "php");
        channel.queueBind(queueName, EXCHANGER_NAME, "java");
        System.out.println("[*] Waiting for message...");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.printf("[x] Received '%s'\n", message);
            System.out.printf("[.] Thread: %s\n", Thread.currentThread().getName());
        };

        // Прослушивание сообщений, поступающих в очередь
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
    }
}
