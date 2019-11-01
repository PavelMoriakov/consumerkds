package com.naya.consumer.aws.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;

import java.io.IOException;

public class AvroUtils {

    private static AvroMapper avroMapper = new AvroMapper();

    public AvroUtils() {
        this.avroMapper = new AvroMapper();
    }

    private static AvroSchema getSchema() throws JsonMappingException {
        AvroSchema avroSchema = avroMapper.schemaFor(Employee.class);
        return avroSchema;
    }

    public static byte[] writeDatatoBytes(Employee employee) throws JsonMappingException {
        byte[] avroData = new byte[0];
        System.out.println(getSchema().getAvroSchema().toString(true));
        try {
             avroData = avroMapper.writer(getSchema())
                    .writeValueAsBytes(employee);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return avroData;
    }

    public static Employee readDataFromBytes(byte[] input){
        try {
             return avroMapper.readerFor(Employee.class).with(getSchema()).readValue(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
