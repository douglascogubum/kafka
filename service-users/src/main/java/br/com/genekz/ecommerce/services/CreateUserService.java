package br.com.genekz.ecommerce.services;

import br.com.genekz.ecommerce.database.LocalDatabase;
import br.com.genekz.ecommerce.model.Message;
import br.com.genekz.ecommerce.model.Order;
import br.com.genekz.ecommerce.services.consumer.ConsumerService;
import br.com.genekz.ecommerce.services.consumer.ServiceRunner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.SQLException;
import java.util.UUID;

@Slf4j
public class CreateUserService implements ConsumerService<Order> {

    private static final int THREADS = 1;
    private final LocalDatabase database;


    CreateUserService() throws SQLException {
        this.database = new LocalDatabase("users_database");
        this.database.createIfNotExists("create table Users (" +
                "uuid varchar(200) primary key, " +
                "email varchar(200))");
    }

    public static void main(String[] args) {
        new ServiceRunner<>(CreateUserService::new).start(THREADS);
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return CreateUserService.class.getSimpleName();
    }

    @Override
    public void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {
        var message = record.value();
        var order = message.getPayload();

        log.info("-----------------------------------------");
        log.info("Processing new order, checking for new user");
        log.info(order.toString());

        if(isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail());
        }
    }

    private void insertNewUser(String email) throws SQLException {
        var uuid = UUID.randomUUID().toString();
        database.update("insert into Users (uuid,email) " +
                "values (?,?)", uuid, email);
        log.info("Usuário " + uuid + " e " + email + " adicionado");
    }


    private boolean isNewUser(String email) throws SQLException {
        var results = database.query("select uuid from Users " +
                "where email =? limit 1", email);
        return !results.next();
    }
}
