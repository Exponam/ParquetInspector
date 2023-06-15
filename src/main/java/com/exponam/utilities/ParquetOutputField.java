package com.exponam.utilities;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.schema.Type;

class ParquetOutputField extends OutputField {

    public ParquetOutputField(Group record, int fieldIndex) {
        super(record.getType().getType(fieldIndex).getName(),
                fieldValueAsString(record, fieldIndex, record.getType().getType(fieldIndex)));
    }

    private static String fieldValueAsString(Group record, int fieldIndex, Type fieldType) {
        if (!fieldType.isPrimitive()) return "non-primitive type";

        var repetitionCount = record.getFieldRepetitionCount(fieldIndex);
        if (repetitionCount == 0) return "null";

        switch (fieldType.asPrimitiveType().getPrimitiveTypeName()) {
            case FLOAT:
            {
                var f = record.getFloat(fieldIndex, 0);
                return Float.isNaN(f) ? "NaN" : Float.toString(f);
            }
            case INT32:
                return Integer.toString(record.getInteger(fieldIndex, 0));
            case INT64:
                return Long.toString(record.getLong(fieldIndex, 0));
            case FIXED_LEN_BYTE_ARRAY:
            case BINARY:
                return record.getValueToString(fieldIndex, 0);
            case DOUBLE:
            {
                var d = record.getDouble(fieldIndex, 0);
                return Double.isNaN(d) ? "NaN" : Double.toString(d);
            }
            case BOOLEAN:
                return Boolean.toString(record.getBoolean(fieldIndex, 0));
            case INT96:
            {
                var i96 = record.getInt96(fieldIndex, 0);
                var nanoTime = NanoTime.fromBinary(i96);
                return nanoTime.toString();
            }
            default:
                throw new IllegalArgumentException("Unknown field type: " + fieldType.asPrimitiveType().getPrimitiveTypeName());
        }
    }
}