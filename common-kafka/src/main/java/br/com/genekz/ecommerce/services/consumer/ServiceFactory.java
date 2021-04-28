package br.com.genekz.ecommerce.services.consumer;

public interface ServiceFactory<T> {

    ConsumerService<T> create() throws Exception;
}
