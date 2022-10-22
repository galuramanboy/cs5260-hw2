package com.example;

import javax.swing.plaf.TreeUI;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.protocols.jsoncore.internal.ObjectJsonNode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;


public class Consumer 
{
    public static void main( String[] args ) throws ParseException, IOException
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

        String reqBucket = "usu-cs5260-chicken-requests";
        String writeBucket = "usu-cs5260-chicken-web";
        
         
        
        // Loop until some stop condition met

            // Try to get request
            try {
                ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(reqBucket).maxKeys(1).build();
                ListObjectsV2Response response = s3.listObjectsV2(request);
                List<S3Object> objects = response.contents();
         
                ListIterator<S3Object> listIterator = objects.listIterator();
         
                while (listIterator.hasNext()) {
                    S3Object object = listIterator.next();
                    GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(object.key())
                    .bucket(reqBucket)
                    .build();
                    ResponseInputStream newObj = s3.getObject(objectRequest);
                    File f = new File(object.key());
                    FileOutputStream fos = new FileOutputStream(f);
                    byte[] read_buf = new byte[1024];
                    int read_len = 0;
                    while ((read_len = newObj.read(read_buf)) > 0) {
                        fos.write(read_buf, 0, read_len);
                    }
                    newObj.close();
                    fos.close();
                    JSONParser jsonParser = new JSONParser();
                    try (FileReader reader = new FileReader(f))
                    {
                        //Read JSON file
                        Object obj = jsonParser.parse(reader);
                        JSONArray info = new JSONArray();
                        info.add(obj);
                        JSONObject lo = (JSONObject) obj;
                        System.out.println(lo.get("requestId"));
                        JSONObject toUpload = new JSONObject();
                        toUpload.put("bucketName", reqBucket);
                        FileWriter file = new FileWriter(f);
                        file.write(lo.toJSONString());
                        file.flush();
                        file.close();


                        String newKey = "widgets/" + ((String) lo.get("owner")).replace(" ", "-").toLowerCase() + "/" + (String)lo.get("widgetId");
                        PutObjectRequest uploadRequest = PutObjectRequest.builder()
                        .bucket(writeBucket)
                        .key(newKey)
                        .build();

                        s3.putObject(uploadRequest, RequestBody.fromFile(f));
                    } catch (org.json.simple.parser.ParseException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                
                }
            } catch (Exception e) {
                System.out.println(e);
            }

                

            // If got request 
                // Process request 
            // Else 
                // Wait a while (100ms) 
        // End loop 
 

        
    }


}
