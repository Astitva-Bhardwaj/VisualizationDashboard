package com.example.blackcoffer.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import javax.persistence.Id;

import javax.persistence.*;
import javax.persistence.Table;
import java.time.LocalDate;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Entity
@Table(name="data")
public class Data {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Integer end_year;
    private Integer citylng;
    private Integer citylat;
    private Integer intensity;
    private String sector;
    private String topic;
    private String insight;
    private String swot;
    private String url;
    private String region;
    private Integer start_year;
    private Integer impact;
    private LocalDate added;
    private LocalDate published;
    private String city;
    private String country;
    private Integer relevance;
    private String pestle;
    private String source;
    private String title;
    private Integer likelihood;

}
