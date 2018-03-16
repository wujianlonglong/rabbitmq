package my.test.rabbitmq.controller;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
@RequestMapping("/OneToOne")
public class OneToOne {

    private final static String QUEUE_NAME = "hello1";// 队列名不能重复 之前已有就会失败

    @RequestMapping(value="/produceMessage")
    public void produceMessage() throws IOException {
   /* 使用工厂类建立Connection和Channel，并且设置参数 */
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");// MQ的IP
        factory.setPort(5672);// MQ端口
        factory.setUsername("guest");// MQ用户名
        factory.setPassword("guest");// MQ密码
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        /* 创建消息队列，并且发送消息 */
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        for(int i=0;i<10;i++){
            String message = "消息"+i;
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            System.out.println("生产了个'" + message + "'");
        }
        /* 关闭连接 */
        channel.close();
        connection.close();
    }



    @RequestMapping(value="/consumerMessage")
    public void consumerMessage() throws IOException, InterruptedException {
  /* 建立连接 */
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");// MQ的IP
        factory.setPort(5672);// MQ端口
        factory.setUsername("guest");// MQ用户名
        factory.setPassword("guest");// MQ密码
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        /* 声明要连接的队列 */
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println("等待消息产生：");

        /* 创建消费者对象，用于读取消息 */
        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(QUEUE_NAME, true, consumer);

        /* 读取队列，并且阻塞，即在读到消息之前在这里阻塞，直到等到消息，完成消息的阅读后，继续阻塞循环 */
        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());
            System.out.println("收到消息'" + message + "'");
        }
    }



    @RequestMapping(value="/consumerMessageTwo")
    public void consumerMessagetwo() throws IOException, InterruptedException {
  /* 建立连接 */
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");// MQ的IP
        factory.setPort(5672);// MQ端口
        factory.setUsername("guest");// MQ用户名
        factory.setPassword("guest");// MQ密码
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        /* 声明要连接的队列 */
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println("等待消息产生：");

        /* 创建消费者对象，用于读取消息 */
        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(QUEUE_NAME, true, consumer);

        /* 读取队列，并且阻塞，即在读到消息之前在这里阻塞，直到等到消息，完成消息的阅读后，继续阻塞循环 */
        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());
            System.out.println("收到消息'" + message + "'");
        }
    }

}
