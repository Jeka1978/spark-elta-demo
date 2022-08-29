package com.elta.taxi;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Driver {
    private int id;
    private String name;

    public static Driver convertFromLine(String line) {
        String[] data = line.split(", ");

        return Driver.builder().id(Integer.parseInt(data[0])).name(data[1]).build();
    }
}
