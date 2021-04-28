package br.com.genekz.ecommerce.services;

import br.com.genekz.ecommerce.model.Message;
import br.com.genekz.ecommerce.model.User;
import br.com.genekz.ecommerce.services.consumer.ConsumerService;
import br.com.genekz.ecommerce.services.consumer.ServiceRunner;
import br.com.genekz.ecommerce.utils.IO;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

@Slf4j
public class ReadingReportService implements ConsumerService<User> {

    private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();

    private static final int THREADS = 5;

    public static void main(String[] args) {
        new ServiceRunner(ReadingReportService::new).start(THREADS);
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_USER_GENERATE_READING_REPORT";
    }

    @Override
    public String getConsumerGroup() {
        return ReadingReportService.class.getSimpleName();
    }

    @Override
    public void parse(ConsumerRecord<String, Message<User>> record) throws IOException{
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
