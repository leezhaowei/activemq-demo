package com.zwli.activemq_demo;

import java.text.DecimalFormat;

import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;

public class Listener implements MessageListener {

    @Override
    public void onMessage(final Message message) {
        try {
            final MapMessage map = (MapMessage) message;
            final String stock = map.getString("stock");
            final double price = map.getDouble("price");
            final double offer = map.getDouble("offer");
            final boolean up = map.getBoolean("up");
            final DecimalFormat df = new DecimalFormat("#,###,###,##0.00");
            System.out.println(stock + "\t" + df.format(price) + "\t" + df.format(offer) + "\t" + (up ? "up" : "down"));
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

}
