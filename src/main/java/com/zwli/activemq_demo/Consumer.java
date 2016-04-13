package com.zwli.activemq_demo;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;

public class Consumer {

    private static String brokerURL = "tcp://localhost:61616";
    private transient ConnectionFactory factory;
    private transient Connection connection;
    private transient Session session;

    public Consumer() throws JMSException {
        factory = new ActiveMQConnectionFactory(brokerURL);
        connection = factory.createConnection();
        try {
            connection.start();
        } catch (final JMSException e) {
            connection.close();
            throw e;
        }
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    public void close() throws JMSException {
        if (connection != null) {
            connection.close();
        }
    }

    public Session getSession() {
        return session;
    }

    public static void main(final String[] args) throws JMSException {
        final Consumer consumer = new Consumer();
        for (final String stock : args) {
            final Destination destination = consumer.getSession().createTopic("STOCKS." + stock);
            final MessageConsumer messageConsumer = consumer.getSession().createConsumer(destination);
            messageConsumer.setMessageListener(new Listener());
        }
    }
}
