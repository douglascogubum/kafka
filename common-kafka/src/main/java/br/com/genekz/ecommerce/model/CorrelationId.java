package br.com.genekz.ecommerce.model;

import lombok.Getter;
import lombok.ToString;

import java.util.UUID;

@Getter
@ToString
public class CorrelationId {

    private final String id;

    public CorrelationId(String title){
        id = title + "(" + UUID.randomUUID().toString() + ")";
    }

    public CorrelationId continueWith(String title) {
        return new CorrelationId(id + "-"  + title);
    }
}
