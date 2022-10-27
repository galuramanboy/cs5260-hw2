package com.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.ListIterator;

import org.json.simple.JSONObject;
import org.junit.Test;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Unit test for consumer.
 */
public class ConsumerTest 
{
    ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
                Region region = Region.US_EAST_1;
                S3Client s3 = S3Client.builder()
                    .region(region)
                    .credentialsProvider(credentialsProvider)
                    .build();
                    DynamoDbClient ddb = DynamoDbClient.builder().region(region).credentialsProvider(credentialsProvider).build();

                    ListObjectsV2Request request = ListObjectsV2Request.builder().bucket("usu-cs5260-chicken-requests").maxKeys(1).build();
                    ListObjectsV2Response response = s3.listObjectsV2(request);
                    List<S3Object> objects = response.contents();
                    ListIterator iterator = objects.listIterator();
                    S3Object testObject = (S3Object)iterator.next();

                    JSONObject test = new JSONObject();
                    
                    
    /**
     * To test, make sure we have items in request bucket!!
     * @throws IOException
     */
    public void createTestObject(JSONObject test)
    {
        test.put("test1", "value1");
        test.put("test2", "value2: {test3: value3}");
        test.put("test4", "[{key1: val1}, {key2: val2}]");
        test.put("last_modified_on", "3");
        test.put("owner", "tester");
        test.put("id", "testId");
    }


    @Test
    public void canCreateS3() throws IOException
    {
        File thing = Consumer.downloadObject(testObject, s3, "usu-cs5260-chicken-requests");
        createTestObject(test);
        assertNotNull( "Test not null", thing );
        assertEquals(test.toString(), Consumer.parseJSON(thing, "3").toString());
        assertNotNull(Consumer.uploadToS3(test, "usu-cs5260-chicken-web", s3, thing));
    }

    @Test
    public void canUpdateS3()
    {

    }

    @Test
    public void canDeleteS3()
    {

    }

    @Test
    public void canCreateDDB()
    {
        createTestObject(test);
        assertNotNull(Consumer.uploadToDDB(test, "widgets", ddb));
    }
    @Test
    public void canUpdateDDB()
    {

    }
    @Test
    public void canDeleteDDB()
    {

    }
}
