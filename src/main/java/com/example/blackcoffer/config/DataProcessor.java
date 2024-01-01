package com.example.blackcoffer.config;

import com.example.blackcoffer.model.Data;
import org.springframework.batch.item.ItemProcessor;

public class DataProcessor implements ItemProcessor<Data, Data> {
    @Override
    public Data process(Data data) {
        String city = data.getCity();
        if (city != null) {
            return data;
        }
        return null;
    }
}
