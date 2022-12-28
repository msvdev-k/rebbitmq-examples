package rabbitmq.produser;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class SimpleSenderApp {

    private final static String QUEUE_NAME = "SimpleReceiverQueue";
    private final static String EXCHANGER_NAME = "SimpleSenderExchanger";


    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (   // Создание соединения и открытие канала связи
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel()
        ) {

            // Определение обменника
            channel.exchangeDeclare(EXCHANGER_NAME, BuiltinExchangeType.DIRECT);

            // Создание очереди
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            // Привязка очереди к обменнику по ключу "java"
            channel.queueBind(QUEUE_NAME, EXCHANGER_NAME, "java");

            String message = "Hello MSV!";

            // Отправка сообщения в обменник с ключём "java"
            channel.basicPublish(EXCHANGER_NAME, "java", null, message.getBytes(StandardCharsets.UTF_8));
            System.out.printf("[x] Sent message: %s\n", message);

        }
    }
}
