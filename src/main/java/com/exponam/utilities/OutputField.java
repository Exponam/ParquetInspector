package com.exponam.utilities;

import java.util.Objects;

abstract class OutputField {
    private final String fieldName;
    protected final String value;

    public OutputField(String fieldName, String value) {
        this.fieldName = Objects.requireNonNull(fieldName);
        this.value = Objects.requireNonNull(value);
    }

    String getFieldName() {
        return fieldName;
    }

    @Override
    public String toString() {
        return value;
    }
}