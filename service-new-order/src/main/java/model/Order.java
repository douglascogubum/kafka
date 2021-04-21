package model;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.math.BigDecimal;

@AllArgsConstructor
@Getter
public class Order {

    private final String userId;
    private final String orderId;
    private final BigDecimal amount;
}
