package com.exponam.utilities;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class ParquetInspector {
    private static final org.apache.hadoop.conf.Configuration hadoopConfiguration =
            new org.apache.hadoop.conf.Configuration();
    static {
        // Explicitly set the fs.file.impl because without this failures can occur related to
        // LocalFileSystem on some environments.
        hadoopConfiguration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }

    private static final int MAX_ROWS_TO_DUMP = 10;

    private final String fileName;

    ParquetInspector(String fileName) {
        this.fileName = Objects.requireNonNull(fileName);
    }

    void inspect(PrintStream outputStream, PrintStream errorStream) {
        outputStream.printf("Inspecting file '%s'\n", fileName);

        var file = new File(fileName);
        if (!file.exists()) {
            errorStream.printf("Unable to find file '%s'\n", fileName);
            return;
        }

        var hadoopPath = new org.apache.hadoop.fs.Path("file:///" + file.getAbsolutePath());
        final HadoopInputFile hadoopInputFile;
        try {
            hadoopInputFile = org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(hadoopPath, hadoopConfiguration);
        } catch(IOException e) {
            errorStream.printf("Unable to open file '%s' at Hadoop path '%s'\n", fileName, hadoopPath);
            return;
        }

        try (var reader = ParquetFileReader.open(hadoopInputFile)) {
            dumpSchema(reader, outputStream);
            dumpData(reader, outputStream, errorStream);
        } catch(IOException e) {
            errorStream.printf("Exception encountered inspecting file '%s', message = '%s'\n", fileName, e.getMessage());
        }
    }

    private static void dumpSchema(ParquetFileReader reader, PrintStream outputStream) {
        new ParquetSchemaHelper(reader).dumpSchema(outputStream);
    }

    private void dumpData(ParquetFileReader parquetFileReader, PrintStream outputStream, PrintStream errorStream) {
        try {
            var rows = gatherRows(parquetFileReader);

            if (rows.isEmpty()) {
                outputStream.println("No row-level data found.");
                return;
            }

            var columnNames = new ParquetSchemaHelper(parquetFileReader).getColumnNames();
            columnNames.add(0, RowNumberField.ROW_FIELD_NAME);

            var maxColumnWidths = getMaxColumnWidths(columnNames, rows);

            dumpRow(columnNames, maxColumnWidths, outputStream);
            rows.forEach(row -> {
                var rowValues = columnNames.stream()
                        .map(columnName -> row.get(columnName).toString())
                        .collect(Collectors.toList());
                dumpRow(rowValues, maxColumnWidths, outputStream);
            });
        } catch (IOException e) {
            errorStream.printf("Exception encountered inspecting file '%s', message = '%s'\n", fileName, e.getMessage());
        }
    }

    private static void dumpRow(List<String> values, List<Integer> maxColumnWidths, PrintStream outputStream) {
        IntStream.range(0, values.size()).forEach(valueIndex -> {
            var value = values.get(valueIndex);
            outputStream.printf(value);
            IntStream.range(0, 1 + maxColumnWidths.get(valueIndex) - value.length())
                    .forEach(paddingIndex -> outputStream.print(" "));
        });
        outputStream.println();
    }

    private static List<Map<String, OutputField>> gatherRows(ParquetFileReader reader) throws IOException {
        var parquetFileSchema = new ParquetSchemaHelper(reader).getSchema();
        var rows = new ArrayList<Map<String, OutputField>>(MAX_ROWS_TO_DUMP);
        var pages = (PageReadStore)null;
        while (null != (pages = reader.readNextRowGroup())) {
            var rowCount = pages.getRowCount();
            var columnIO = new ColumnIOFactory().getColumnIO(parquetFileSchema);
            var recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(parquetFileSchema));
            IntStream.range(0, (int)Math.min(rowCount, MAX_ROWS_TO_DUMP)).forEach(i -> {
                var record = recordReader.read();
                var fieldCount = record.getType().getFieldCount();
                Map<String, OutputField> rowFields = IntStream.range(0, fieldCount)
                        .mapToObj(fieldIndex -> new ParquetOutputField(record, fieldIndex))
                        .collect(Collectors.toMap(ParquetOutputField::getFieldName, oneField -> oneField));
                rowFields.put("Row", new RowNumberField(i + 1));
                rows.add(rowFields);
            });
        }
        return rows;
    }

    private static List<Integer> getMaxColumnWidths(List<String> columnNames, List<Map<String, OutputField>> rows) {
        return columnNames.stream().map(columnName ->
                Math.max(rows.stream().map(row ->
                                row.get(columnName).toString().length())
                                .max(Comparator.comparingInt(x -> x)).orElse(0),
                columnName.length())).collect(Collectors.toList());
    }
}
