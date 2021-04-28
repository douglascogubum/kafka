package br.com.genekz.ecommerce.services;

import br.com.genekz.ecommerce.database.OrderDatabase;
import br.com.genekz.ecommerce.model.CorrelationId;
import br.com.genekz.ecommerce.model.Order;
import br.com.genekz.ecommerce.services.dispatcher.KafkaDispatcher;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

@Slf4j
public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            var email = req.getParameter("email");
            var amount = new BigDecimal(req.getParameter("amount"));
            var orderId = req.getParameter("uuid");
            var order = new Order(orderId, amount, email);

            try (var database = new OrderDatabase()) {
                if (database.saveNew(order)) {
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, new CorrelationId(NewOrderServlet.class.getSimpleName()), order);

                    log.info("New order sent successfully.");

                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("New order sent");
                } else {
                    log.info("Old order received.");

                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("Old order received");
                }
            }
        } catch (ExecutionException|InterruptedException|SQLException e) {
            throw new ServletException(e);
        }
    }
}
