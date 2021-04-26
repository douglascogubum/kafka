package br.com.genekz.ecommerce.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class User {

    private final String uuid;

    public String getReportPath() {
        return "target/" + uuid + "-report.txt";
    }
}
