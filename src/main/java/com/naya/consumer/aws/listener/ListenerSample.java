package com.naya.consumer.aws.listener;

import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class ListenerSample {

    public static void main(String[] args) {

        KinesisAsyncClient client = KinesisAsyncClient.builder().build();

        ListShardsRequest request = ListShardsRequest
                .builder().streamName("test").maxResults(100)
                .build();

        String shardIterator;
        GetShardIteratorRequest getShardIteratorRequest = GetShardIteratorRequest.builder().streamName("test")
                .shardIteratorType(ShardIteratorType.).build();



        GetShardIteratorResult getShardIteratorResult = ;
        shardIterator = getShardIteratorResult.getShardIterator();

        try {
            ListShardsResponse response = client.listShards(request).get(5000, TimeUnit.MILLISECONDS);
            System.out.println(response.toString());
            CompletableFuture<GetRecordsResponse> records = client.getRecords(GetRecordsRequest.builder().shardIterator().build());
            System.out.println(records.get().records().size());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
