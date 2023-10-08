package com.example.externalfilesort;

import java.io.*;

import com.example.externalfilesort.service.*;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;

@SpringBootApplication
public class ExternalFileSortApplication {

    private static final String TEMP_DIR = "src/main/resources/files/temp";

    private static final String INPUT_FILE = "src/main/resources/files/input.txt";

    private static final String OUTPUT_FILE = "src/main/resources/files/output.txt";

    public static void main(String[] args) throws IOException {
        var ctx = SpringApplication.run(ExternalFileSortApplication.class, args);
        var fileSortService = ctx.getBean(ExternalSortService.class);

        fileSortService.executeExternalSort(INPUT_FILE, OUTPUT_FILE, TEMP_DIR);
    }

}
