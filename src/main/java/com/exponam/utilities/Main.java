package com.exponam.utilities;

import java.util.Arrays;

/**
 * This utility application dumps basic information about Parquet files.
 * It accepts a list of file names, dumping the schema and the first few rows of each.
 */
public class Main {
    public static void main(String[] args) {
        Arrays.stream(args).forEach(arg -> {
            try {
                new ParquetInspector(arg).inspect(System.out, System.err);
            } catch (Exception e) {
                System.err.printf("Uncaught exception while processing '%s', message is '%s'\n", arg, e.getMessage());
            }
        });
    }
}