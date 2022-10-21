package com.example;

import javax.swing.plaf.TreeUI;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;


import java.util.List;

/**
 * Hello world!
 *
 */
public class Consumer 
{
    public static void main( String[] args ) throws ParseException
    {
        Options opt = new Options();
        opt.addOption("h", "hello", true, "test description");
        opt.addOption("m","multiply", true, "multiply them bad bois");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(opt, args); 

        if (cmd.hasOption("h")) {
            System.out.println(cmd.getOptionValue("h"));
            System.out.println("These are h args" + args);
        }
        if (cmd.hasOption("m")) {
            System.out.println(cmd.getOptionValue("m"));
            System.out.println("These are m args" + args);
        }

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
        Region region = Region.US_EAST_1;
        S3Client s3 = S3Client.builder()
            .region(region)
            .credentialsProvider(credentialsProvider)
            .build();

        // List buckets
        ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
        ListBucketsResponse listBucketsResponse = s3.listBuckets(listBucketsRequest);
        listBucketsResponse.buckets().stream().forEach(x -> System.out.println(x.name()));
        
    }
}
