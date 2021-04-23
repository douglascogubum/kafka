package br.com.genekz.ecommerce.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.math.BigDecimal;

@AllArgsConstructor
@Getter
@ToString
public class Order {

    private final String orderId;
    private final BigDecimal amount;
    private final String email;
}