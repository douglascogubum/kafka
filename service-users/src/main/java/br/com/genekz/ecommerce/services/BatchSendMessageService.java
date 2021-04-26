package br.com.genekz.ecommerce.services;

import br.com.genekz.ecommerce.model.Message;
import br.com.genekz.ecommerce.model.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Slf4j
public class BatchSendMessageService {

    private final Connection connection;

    BatchSendMessageService() throws SQLException {
        String url = "jdbc:sqlite:target/users_database.db";
        connection = DriverManager.getConnection(url);
        try {
            connection.createStatement().execute("create table Users (" +
                    "uuid varchar(200) primary key, " +
                    "email varchar(200))");
        } catch (SQLException ex) {
            log.info(ex.getMessage());
            connection.close();
        }
    }

    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {
        var batchService = new BatchSendMessageService();
        try (var service = new KafkaService(BatchSendMessageService.class.getSimpleName(), "ECOMMERCE_SEND_MESSAGES_TO_ALL_USERS", batchService::parse, new HashMap<String, String>())) {
            service.run();
        }
    }

    private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Message<String>> record) throws ExecutionException, InterruptedException, SQLException {
        log.info("-----------------------------------------");
        log.info("Processing new batch");
        var message = record.value();
        log.info("Topic: " + message.getPayload());

        for(User user : getAllUsers()) {
            userDispatcher.send(message.getPayload(), user.getUuid(), message.getId().continueWith(BatchSendMessageService.class.getSimpleName()), user);
            log.info("Enviei para " + user);
        }
    }

    private List<User> getAllUsers() throws SQLException {
        var results = connection.prepareStatement("select uuid from Users").executeQuery();
        List<User> users = new ArrayList<>(10);
        while(results.next()) {
            users.add(new User(results.getString(1)));
        }
        return users;
    }
}
