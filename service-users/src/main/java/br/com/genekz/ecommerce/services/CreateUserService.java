package br.com.genekz.ecommerce.services;

import br.com.genekz.ecommerce.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.UUID;

@Slf4j
public class CreateUserService {

    private final Connection connection;

    CreateUserService() throws SQLException {
        String url = "jdbc:sqlite:target/users_database.db";
        connection = DriverManager.getConnection(url);
        try {
            connection.createStatement().execute("create table Users (" +
                    "uuid varchar(200) primary key, " +
                    "email varchar(200))");
        } catch (SQLException ex) {
            log.info(ex.getMessage());
        }
    }

    public static void main(String[] args) throws SQLException {
        var createUserService = new CreateUserService();
        try (var service = new KafkaService(CreateUserService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER", createUserService::parse, Order.class, new HashMap<String, String>())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> record) throws SQLException {
        log.info("-----------------------------------------");
        log.info("Processing new order, checking for new user");
        log.info(String.valueOf(record.value()));
        var order = record.value();
        if(isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail());
        }
    }

    private void insertNewUser(String email) throws SQLException {
        var uuid = UUID.randomUUID().toString();
        var insert = connection.prepareStatement("insert into Users (uuid,email) " +
                "values (?,?)");
            insert.setString(1, uuid);
            insert.setString(2, email);
            insert.execute();
            log.info("Usuário " + uuid + " e " + email + " adicionado");
    }


    private boolean isNewUser(String email) throws SQLException {
        var exists = connection.prepareStatement("select uuid from Users " +
                "where email =? limit 1");

        exists.setString(1, email);
        var results = exists.executeQuery();
        return !results.next();
    }
}
