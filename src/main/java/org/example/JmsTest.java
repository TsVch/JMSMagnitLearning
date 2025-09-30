package org.example;

import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class JmsTest {

    private static final String BROKER_URL = "tcp://localhost:61616";
    private static final String BASE_QUEUE_NAME = "TEST.QUEUE";

    public static void main(String[] args) throws Exception {
        List<Path> testFiles = List.of(
                Path.of("e:\\users\\tcoy_v_i\\Desktop\\JAVA\\ESBGW\\Проекты\\Обучение\\Задачи_1_2\\TestJson\\SmallJson.json"),
                Path.of("e:\\users\\tcoy_v_i\\Desktop\\JAVA\\ESBGW\\Проекты\\Обучение\\Задачи_1_2\\TestJson\\MiddleJson.json"),
                Path.of("e:\\users\\tcoy_v_i\\Desktop\\JAVA\\ESBGW\\Проекты\\Обучение\\Задачи_1_2\\TestJson\\BigJson.json")
        );

        testSendReceive(Session.AUTO_ACKNOWLEDGE, "AUTO_ACK", testFiles);
        testSendReceive(Session.CLIENT_ACKNOWLEDGE, "CLIENT_ACK", testFiles);
        testSendReceive(Session.DUPS_OK_ACKNOWLEDGE, "DUPS_OK_ACK", testFiles);
        testSendReceive(Session.SESSION_TRANSACTED, "TRANSACTED", testFiles);
    }

    private static void testSendReceive(int ackMode, String modeName, List<Path> files)
            throws JMSException, IOException {

        System.out.println("\n=== Тестовый запуск: " + modeName + " ===");

        ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        try (Connection connection = factory.createConnection()) {
            connection.start();

            boolean transacted = (ackMode == Session.SESSION_TRANSACTED);
            Session session = connection.createSession(transacted, ackMode);

            String queueName = BASE_QUEUE_NAME + "." + modeName;
            Destination queue = session.createQueue(queueName);
            // Очистка очереди перед тестом
            //clearQueue(session, queue);
            MessageProducer producer = session.createProducer(queue);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            MessageConsumer consumer = session.createConsumer(queue);

            long startTime = System.currentTimeMillis();

            // Отправляем файлы
            for (Path file : files) {
                String content = Files.readString(file);
                TextMessage message = session.createTextMessage(content);
                producer.send(message);
                System.out.println("Sent file: " + file.getFileName() + " (" + content.length() + " chars)");
            }

            // Получаем обратно
            for (int i = 0; i < files.size(); i++) {
                Message received = consumer.receive(5000);
                if (received instanceof TextMessage txt) {
                    System.out.println("Received message (" + txt.getText().length() + " chars)");
                    if (ackMode == Session.CLIENT_ACKNOWLEDGE) {
                        received.acknowledge();
                    }
                }
            }

            if (transacted) {
                session.commit();
            }

            long endTime = System.currentTimeMillis();
            System.out.println("Время выполнения (" + modeName + "): " + (endTime - startTime) + " ms");

            session.close();
        }
    }

    private static void clearQueue(Session session, Destination queue) throws JMSException {
        MessageConsumer consumer = session.createConsumer(queue);
        int cleared = 0;
        while (true) {
            Message msg = consumer.receiveNoWait(); // non-blocking
            if (msg == null) break;
            cleared++;
        }
        consumer.close();
        if (cleared >= 0) {
            System.out.println("Очередь очищена, удалено сообщений: " + cleared);
        }
    }
}
