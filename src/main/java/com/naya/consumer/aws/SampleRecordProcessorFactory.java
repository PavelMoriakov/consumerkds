package com.naya.consumer.aws;

import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

public class SampleRecordProcessorFactory implements ShardRecordProcessorFactory {

    public ShardRecordProcessor shardRecordProcessor() {
        return new SampleRecordProcessor();
    }
}
