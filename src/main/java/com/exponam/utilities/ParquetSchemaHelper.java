package com.exponam.utilities;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;

import java.io.PrintStream;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

class ParquetSchemaHelper {
    private final ParquetFileReader reader;

    ParquetSchemaHelper(ParquetFileReader reader) {
        this.reader = Objects.requireNonNull(reader);
    }

    MessageType getSchema() {
        return reader.getFooter().getFileMetaData().getSchema();
    }

    List<String> getColumnNames() {
        return getSchema().getColumns().stream().map(columnDescriptor -> {
            var pathElements = columnDescriptor.getPath();
            return pathElements[pathElements.length - 1];
        }).collect(Collectors.toList());
    }

    void dumpSchema(PrintStream outputStream) {
        var sb = new StringBuilder();
        getSchema().writeToStringBuilder(sb, "  ");
        outputStream.printf(sb.toString());
    }
}
