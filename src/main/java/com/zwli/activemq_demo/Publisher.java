package com.zwli.activemq_demo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQMapMessage;

public class Publisher {

    private final int MAX_DELTA_PERCENT = 1;
    private final Map<String, Double> LAST_PRICES = new ConcurrentHashMap<String, Double>(); // new Hashtable<String,
                                                                                             // Double>();
    private static int count = 10;
    private static int total;

    private static String brokerURL = "tcp://localhost:61616";
    private transient ConnectionFactory factory;
    private transient Connection connection;
    private transient Session session;
    private transient MessageProducer producer;

    public Publisher() throws JMSException {
        factory = new ActiveMQConnectionFactory(brokerURL);
        connection = factory.createConnection();

        try {
            connection.start();
        } catch (final JMSException e) {
            connection.close();
            throw e;
        }

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(null);
    }

    public void close() throws JMSException {
        if (connection != null) {
            connection.close();
        }
    }

    public void sendMessage(final String[] stocks) throws JMSException {
        int index = 0;
        while (true) {
            index = (int) Math.round(stocks.length * Math.random());
            if (index < stocks.length) {
                break;
            }
        }
        final String stock = stocks[index];
        final Destination destination = session.createTopic("STOCKS." + stock);
        final Message message = createStockMessage(stock, session);
        System.out.println("Sending: " + ((ActiveMQMapMessage) message).getContentMap() + " on destination: "
                + destination);
        producer.send(destination, message);
    }

    public Message createStockMessage(final String stock, final Session session) throws JMSException {
        Double value = LAST_PRICES.get(stock);
        if (value == null) {
            value = new Double(Math.random() * 100);
        }

        // lets mutate the value by some percentage
        final double oldPrice = value.doubleValue();
        value = new Double(mutatePrice(oldPrice));
        LAST_PRICES.put(stock, value);

        final double price = value.doubleValue();
        final double offer = price * 1.001;
        final boolean up = (price > oldPrice);

        final MapMessage message = session.createMapMessage();
        message.setString("stock", stock);
        message.setDouble("price", price);
        message.setDouble("offer", offer);
        message.setBoolean("up", up);
        return message;
    }

    public double mutatePrice(final double price) {
        final double percentChange = (2 * Math.random() * MAX_DELTA_PERCENT) - MAX_DELTA_PERCENT;
        return price * (100 + percentChange) / 100;
    }

    public static void main(final String[] args) throws JMSException {
        final Publisher publisher = new Publisher();
        while (total < 1000) {
            for (int i = 0; i < count; i++) {
                publisher.sendMessage(args);
            }
            total += count;
            System.out.println("Published '" + count + "' of '" + total + "' price messages");
            try {
                Thread.sleep(1000);
            } catch (final InterruptedException x) {
            }
        }
        publisher.close();
    }
}
