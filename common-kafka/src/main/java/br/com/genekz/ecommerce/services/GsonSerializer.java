package br.com.genekz.ecommerce.services;

import br.com.genekz.ecommerce.model.Message;
import br.com.genekz.ecommerce.model.MessageAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class GsonSerializer<T> implements Serializer<T> {

    private final Gson gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageAdapter()).create();

    @Override
    public byte[] serialize(String s, T t) {
        return gson.toJson(t).getBytes(StandardCharsets.UTF_8);
    }
}
