package br.com.genekz.ecommerce.services;

import br.com.genekz.ecommerce.model.CorrelationId;
import br.com.genekz.ecommerce.services.dispatcher.KafkaDispatcher;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

@Slf4j
public class GenerateAllReportsServlet extends HttpServlet {

    private final KafkaDispatcher<String> batchDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        batchDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            batchDispatcher.send("ECOMMERCE_SEND_MESSAGES_TO_ALL_USERS", "ECOMMERCE_USER_GENERATE_READING_REPORT", new CorrelationId(GenerateAllReportsServlet.class.getSimpleName()), "ECOMMERCE_USER_GENERATE_READING_REPORT");

            log.info("Sent generte report to all users.");

            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("Report requests generated");
        } catch (ExecutionException|InterruptedException e) {
            throw new ServletException(e);
        }
    }
}
