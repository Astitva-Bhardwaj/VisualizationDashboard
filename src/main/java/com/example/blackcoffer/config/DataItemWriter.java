package com.example.blackcoffer.config;

import com.example.blackcoffer.model.Data;
import com.example.blackcoffer.repository.DataRepository;
import org.springframework.batch.core.configuration.annotation.StepScope;
//import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class DataItemWriter implements ItemWriter<Data> {

    @Autowired
    private DataRepository repository;

    @Override
    public void write(List<? extends Data> list) throws Exception {
        System.out.println("Writer Thread "+Thread.currentThread().getName());
        repository.saveAll(list);
    }

}