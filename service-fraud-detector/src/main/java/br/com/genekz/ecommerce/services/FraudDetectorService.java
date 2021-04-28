package br.com.genekz.ecommerce.services;


import br.com.genekz.ecommerce.database.LocalDatabase;
import br.com.genekz.ecommerce.model.Message;
import br.com.genekz.ecommerce.model.Order;
import br.com.genekz.ecommerce.services.consumer.ConsumerService;
import br.com.genekz.ecommerce.services.consumer.ServiceRunner;
import br.com.genekz.ecommerce.services.dispatcher.KafkaDispatcher;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

@Slf4j
public class FraudDetectorService implements ConsumerService<Order> {

    private static final int THREADS = 1;
    private final LocalDatabase database;

    FraudDetectorService() throws SQLException {
        this.database = new LocalDatabase("fraud_database");
        this.database.createIfNotExists("create table Orders (" +
                "uuid varchar(200) primary key, " +
                "is_fraud boolean)");
    }

    public static void main(String[] args) {
        new ServiceRunner<>(FraudDetectorService::new).start(THREADS);
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return FraudDetectorService.class.getSimpleName();
    }

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    public void parse(ConsumerRecord<String, Message<Order>> record) throws InterruptedException, ExecutionException, SQLException {
        log.info("-----------------------------------------");
        log.info("Processing new order, checking for fraud");
        log.info(record.key());
        log.info(String.valueOf(record.value()));
        log.info(String.valueOf(record.partition()));
        log.info(String.valueOf(record.offset()));

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }

        var message = record.value();
        var order = message.getPayload();
        if(wasProcessed(order)) {
            log.info("Order" + order.getOrderId() + "was already processed");
        }

        if (isFraud(order)) {
            database.update("insert into Orders (uuid,is_fraud) values (?, true)", order.getOrderId());
            //pretending that the fraud happens when amount is greater than or equal 4500
            log.info("Order is a fraud");
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
        } else {
            database.update("insert into Orders (uuid,is_fraud) values (?, false)", order.getOrderId());
            log.info("Approved: " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
        }
    }

    private boolean wasProcessed(Order order) throws SQLException {
        var results = database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());
        return results.next();
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
