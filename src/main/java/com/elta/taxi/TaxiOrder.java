package com.elta.taxi;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TaxiOrder {
    private int driverId;
    private String city;
    private int distance;

    public static TaxiOrder convertFromLine(String line) {
        String[] data = line.split(" ");

        return TaxiOrder.builder()
                .driverId(Integer.parseInt(data[0]))
                .city(data[1])
                .distance(Integer.parseInt(data[2]))
                .build();
    }
}
