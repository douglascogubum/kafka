package br.com.genekz.ecommerce.model;

import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
public class Message<T> {

    private final CorrelationId id;
    private final T payload;

    public Message(CorrelationId id, T payload){

        this.id = id;
        this.payload = payload;
    }
}
