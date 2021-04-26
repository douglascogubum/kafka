package br.com.genekz.ecommerce.services;

import br.com.genekz.ecommerce.model.CorrelationId;
import br.com.genekz.ecommerce.model.Order;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Slf4j
public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
    private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
        emailDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            var email = req.getParameter("email");
            var amount = new BigDecimal(req.getParameter("amount"));

            var orderId = UUID.randomUUID().toString();
            var order = new Order(orderId, amount, email);
            orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, new CorrelationId(NewOrderServlet.class.getSimpleName()), order);

            var emailCode = "Thank you for your order! We are processing your order";
            emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, new CorrelationId(NewOrderServlet.class.getSimpleName()), emailCode);

            log.info("New order sent successfully.");

            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("New order sent");

        } catch (ExecutionException|InterruptedException e) {
            throw new ServletException(e);
        }
    }
}
