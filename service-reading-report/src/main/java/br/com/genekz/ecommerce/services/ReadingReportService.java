package br.com.genekz.ecommerce.services;


import br.com.genekz.ecommerce.model.Message;
import br.com.genekz.ecommerce.model.User;
import br.com.genekz.ecommerce.utils.IO;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

@Slf4j
public class ReadingReportService {

    private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        var reportService = new ReadingReportService();
        try (var service = new KafkaService(ReadingReportService.class.getSimpleName(), "ECOMMERCE_USER_GENERATE_READING_REPORT", reportService::parse, new HashMap<String, String>())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
        log.info("-----------------------------------------");
        log.info("Processing report for " + record.value());

        var message = record.value();
        var user = message.getPayload();
        var target = new File(user.getReportPath());
        IO.copyTo(SOURCE, target);
        IO.append(target, "Created for " + user.getUuid());

        log.info("File created: " + target.getAbsolutePath());
    }
}
