package com.example.blackcoffer.config;

import com.example.blackcoffer.listener.StepSkipListener;
import com.example.blackcoffer.model.Data;
import com.example.blackcoffer.repository.DataRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.retry.interceptor.RetryOperationsInterceptor;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.io.File;

@Configuration
@EnableBatchProcessing
//@AllArgsConstructor
public class SpringBatchConfig {


    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;


    @Autowired
    private JobRepository jobRepository;
    // Your existing configuration...

    @Autowired
    private DataRepository customerRepository;
    @Autowired
    private DataItemWriter customerItemWriter;


    @Bean
    @StepScope
    public FlatFileItemReader<Data> itemReader(@Value("#{jobParameters[fullPathFileName]}") String pathToFIle) {
        FlatFileItemReader<Data> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setResource(new FileSystemResource(new File(pathToFIle)));
        flatFileItemReader.setName("CSV-Reader");
        flatFileItemReader.setLinesToSkip(1);
        flatFileItemReader.setLineMapper(lineMapper());
        return flatFileItemReader;
    }

    private LineMapper<Data> lineMapper() {
        DefaultLineMapper<Data> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        //lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob", "age");
        //lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");
        lineTokenizer.setNames(new String[]{"end_year","citylng","citylat","intensity","sector","topic","insight",
                "swot","url","region","start_year","impact","added","published","city","country","relevance","pestle","source",
                "title","likelihood"});
        BeanWrapperFieldSetMapper<Data> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Data.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }

    @Bean
    public DataProcessor processor() {
        return new DataProcessor();
    }

    @Bean
    public RepositoryItemWriter<Data> writer() {
        RepositoryItemWriter<Data> writer = new RepositoryItemWriter<>();
        writer.setRepository(customerRepository);
        writer.setMethodName("save");
        return writer;
    }


    @Bean
    public Step step1(FlatFileItemReader<Data> itemReader) {
        return stepBuilderFactory.get("slaveStep").<Data, Data>chunk(10)
                .reader(itemReader)
                .processor(processor())
                .writer(customerItemWriter)
                .faultTolerant()
                .listener(skipListener())
                .skipPolicy(skipPolicy())
                .taskExecutor(taskExecutor())
                .build();
    }


    @Bean
    public Job runJob(FlatFileItemReader<Data> itemReader) {
        return jobBuilderFactory.get("importCustomer").flow(step1(itemReader)).end().build();
    }


    @Bean
    public SkipPolicy skipPolicy() {
        //return new ExceptionSkipPolicy();
        return new CustomSkipPolicy();
    }


    @Bean
    public StepSkipListener skipListener() {
        return new StepSkipListener();
    }


    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
        taskExecutor.setConcurrencyLimit(10);
        return taskExecutor;
    }

    private static class CustomSkipPolicy implements SkipPolicy {
        @Override
        public boolean shouldSkip(Throwable t, int skipCount) throws SkipLimitExceededException {
            // You can implement custom logic to decide whether to skip the exception
            return true; // or false based on your condition
        }
    }

}