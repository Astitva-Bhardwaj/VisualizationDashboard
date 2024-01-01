package com.example.blackcoffer.controller;

import com.example.blackcoffer.model.Data;
import com.example.blackcoffer.repository.DataRepository;
//import com.opencsv.CSVReader;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.batch.core.*;

import java.io.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.repository.JobRepository;


@RestController
@RequestMapping("/api/data")
@Component
public class DataController {

    private final JobBuilderFactory jobBuilderFactory;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job job;

    @Autowired
    private DataRepository dataRepository;

    @Autowired
    private JobRepository jobRepository;

    private final String TEMP_STORAGE = "C:/Users/astit/OneDrive/Desktop/batch-files/";

    @Autowired
    public DataController(JobBuilderFactory jobBuilderFactory, Job job) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.job = job;
    }

    @GetMapping
    public List<Data> getAllData() {
        return dataRepository.findAll();
    }


    @PostMapping(path = "/importData")
    public void startBatch(@RequestParam("file") MultipartFile multipartFile) {

        try {
            System.out.println("##");
            String originalFileName = multipartFile.getOriginalFilename();
            File fileToImport = new File(TEMP_STORAGE + originalFileName);
            multipartFile.transferTo(fileToImport);

            List<Data> dataList = parseCSVFile(fileToImport);
            dataRepository.saveAll(dataList);


            JobParameters jobParameters = new JobParametersBuilder()
                    .addString("fullPathFileName", TEMP_STORAGE + originalFileName)
                    .addLong("startAt", System.currentTimeMillis()).toJobParameters();

            JobExecution execution = jobLauncher.run(job, jobParameters);


        } catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException |
                 JobParametersInvalidException |
                 IOException e) {
            e.printStackTrace();
        }
    }

    private List<Data> parseCSVFile(File file) throws IOException {
        // Replace this with the actual maximum length allowed for the 'title' column in your database
        int MAX_TITLE_LENGTH = 255;
        int MAX_INSIGHT_LENGTH = 255;

        List<Data> dataList = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
            CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1).build(); // Assuming the first line is a header

            String[] line;
            while ((line = csvReader.readNext()) != null) {
                // Create a Data object from each line of the CSV file
                Data data = new Data();
                try {
                    data.setEnd_year(parseInteger(line[0]));
                    data.setCitylng(parseInteger(line[1]));
                    data.setCitylat(parseInteger(line[2]));
                    data.setIntensity(parseInteger(line[3]));
                    data.setSector(line[4]);
                    data.setTopic(line[5]);
                    data.setInsight(line[6]);
                    data.setSwot(line[7]);
                    data.setUrl(line[8]);
                    data.setRegion(line[9]);
                    data.setStart_year(parseInteger(line[10]));
                    data.setImpact(parseInteger(line[11]));
                    data.setAdded(parseLocalDate(line[12]));
                    data.setPublished(parseLocalDate(line[13]));
                    data.setCity(line[14]);
                    data.setCountry(line[15]);
                    data.setRelevance(parseInteger(line[16]));
                    data.setPestle(line[17]);
                    data.setSource(line[18]);
                    data.setTitle(line[19]);
                    data.setLikelihood(parseInteger(line[20]));

                    int maxTitleLength = MAX_TITLE_LENGTH;
                    int maxInsightLength = MAX_INSIGHT_LENGTH;
                    data.setInsight(line[6].substring(0, Math.min(line[6].length(), maxInsightLength)));
                    data.setTitle(line[19].substring(0, Math.min(line[19].length(), maxTitleLength)));


                    dataList.add(data);
                } catch (NumberFormatException | DateTimeParseException e) {
                    // Handle the exception, e.g., log it and skip the current line
                    System.err.println("Error parsing CSV line. Skipping the line: " + Arrays.toString(line));
                }
            }
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }

        return dataList;
    }

    private Integer parseInteger(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            // Log the specific line causing the issue
            System.err.println("Error parsing integer from value: " + value);
            throw e;
        }
    }



    private LocalDate parseLocalDate(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MMMM, d yyyy HH:mm:ss");
            return LocalDate.parse(value, formatter);
        } catch (DateTimeParseException e) {
            // Log the specific line causing the issue
            System.err.println("Error parsing LocalDate from value: " + value);
            throw e; // Rethrow the exception if needed
        }
    }

}