package com.exponam.utilities;

class RowNumberField extends OutputField {
    static final String ROW_FIELD_NAME = "Row";

    public RowNumberField(int rowNumber) {
        super(ROW_FIELD_NAME, Integer.toString(rowNumber));
    }
}