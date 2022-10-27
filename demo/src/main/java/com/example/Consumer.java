package com.example;

import javax.swing.plaf.TreeUI;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
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
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
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
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ProcessHandle.Info;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Consumer 
{
    public static void main( String[] args ) throws ParseException, IOException, InterruptedException
    {
        String reqBucket = null;
        String writeBucket = null;
        String writeTable = null;
        DynamoDbClient ddb = null;
        Options opt = new Options();
        
        opt.addOption("rb", "request-bucket", true, "S3 bucket to pull requests from");
        opt.addOption("wb","write-bucket", true, "S3 bucket to upload object to");
        opt.addOption("h", "help", false, "Print usage statements");
        opt.addOption("wtb", "write-table", true, "DynamoDB table to write to");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(opt, args); 
        HelpFormatter formatter = new HelpFormatter();
        final Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
        FileHandler handler = new FileHandler("consumer.log", true);
        LOGGER.addHandler(handler);
        

        if (cmd.hasOption("h")) {
            formatter.printHelp("consumer", opt);
        }
        if (cmd.hasOption("rb")) {
            reqBucket = cmd.getOptionValue("rb");
        }
        if (cmd.hasOption("wb")) {
            writeBucket = cmd.getOptionValue("wb");
        }
        else if (cmd.hasOption("wtb")) {
            writeTable = cmd.getOptionValue("wtb");
        }

        // access loop here if rb and wb have been specified
        if (reqBucket != null && writeBucket != null || writeTable != null) {
                ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
                Region region = Region.US_EAST_1;
                S3Client s3 = S3Client.builder()
                    .region(region)
                    .credentialsProvider(credentialsProvider)
                    .build();

            if (writeTable != null) {
               ddb = DynamoDbClient.builder().region(region).credentialsProvider(credentialsProvider).build();
            }

        

            
            // Loop until some stop condition met
            while (true) {
                // Try to get request
                ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(reqBucket).maxKeys(1).build();
                ListObjectsV2Response response = s3.listObjectsV2(request);
                List<S3Object> objects = response.contents();
                // If got request 
                if (objects.size() > 0) {
                    // Process request
                    ListIterator<S3Object> listIterator = objects.listIterator();
         
                    S3Object object = listIterator.next();
                    
                    

                    File f = downloadObject(object, s3, reqBucket);

                    JSONObject lo = parseJSON(f, object.lastModified().toString());
                    String reqType = lo.remove("type").toString();
                    lo.remove("requestId");


                    switch (reqType) {
                        case "create":
                            JSONObject toUpload = new JSONObject();
                            String wId = (String) lo.remove("widgetId");
                            toUpload.put("id", wId);
                            for (Object key: lo.keySet()) {
                                toUpload.put(key, lo.get(key));
                            }
                            
                            if (writeBucket != null) {
                                uploadToS3(toUpload, writeBucket, s3, f);
                                
                            }
                            else {
                                uploadToDDB(toUpload, writeTable, ddb);
                                
                            }
                            f.delete();
                            LOGGER.log(Level.INFO, "Processed create request for widget {0}", (String)toUpload.get("id"));
                            break;

                        case "update":
                            if (writeBucket != null) {
                                ListObjectsV2Request request3 = ListObjectsV2Request.builder().bucket(writeBucket).build();
                                ListObjectsV2Response response3 = s3.listObjectsV2(request3);
                                List<S3Object> objects3 = response3.contents();
                                String updateKey = "widgets/" + ((String) lo.get("owner")).replace(" ", "-").toLowerCase() + "/" + (String)lo.get("widgetId");
                                lo.put("id", lo.remove("widgetId"));
                                updateS3Bucket(objects3, updateKey, writeBucket, s3, lo);
                            }
                            else {
                                // do dynamo stuff
                                
                            }
                            f.delete();
                            LOGGER.log(Level.INFO, "Processed update request for widget {0}", (String)lo.get("id"));
                            break;

                        case "delete":
                            if (writeBucket != null) {
                                ListObjectsV2Request request2 = ListObjectsV2Request.builder().bucket(writeBucket).build();
                                ListObjectsV2Response response2 = s3.listObjectsV2(request2);
                                List<S3Object> objects2 = response2.contents();
                                String deleteKey = "widgets/" + ((String) lo.get("owner")).replace(" ", "-").toLowerCase() + "/" + (String)lo.get("widgetId");
                                lo.put("id", lo.remove("widgetId"));
                                deleteFromS3(objects2, deleteKey, writeBucket, s3);
                            }
                            else if (writeTable != null) {
                                // do dynamo stuff
                                System.out.println("Will handle dynamo delete later");
                            }
                            LOGGER.log(Level.INFO, "Processed delete request for widget {0}", (String)lo.get("id"));
                            break;
                    }
                    //delete request item
                deleteFromS3(objects, object.key(), reqBucket, s3);

                }
                else {
                    // wait a while (100ms)
                    TimeUnit.MILLISECONDS.sleep(100);

                }              
            } // End loop
        }  

        else {formatter.printHelp("consumer", opt);}      
        
        
    }

    public static PutItemResponse uploadToDDB(JSONObject toUpload, String writeTable, DynamoDbClient ddb) {
        HashMap<String,AttributeValue> itemValues = new HashMap<String,AttributeValue>();
        
        for (Object key: toUpload.keySet()) {
            if (toUpload.get(key) instanceof JSONArray) {
                // dive into json array
                uploadDDBHelper(toUpload.get(key), itemValues);
            }
            else {
                itemValues.put((String)key, AttributeValue.builder().s((String)toUpload.get(key)).build());
            }
                        
        }
        PutItemRequest request = PutItemRequest.builder()
                .tableName(writeTable)
                .item(itemValues)
                .build();
        // try {
            return ddb.putItem(request);

        // } catch (ResourceNotFoundException e) {
        //     System.err.format("Error: The Amazon DynamoDB table \"%s\" can't be found.\n", writeTable);
        //     System.err.println("Be sure that it exists and that you've typed its name correctly!");
        //     System.exit(1);
        // } catch (DynamoDbException e) {
        //     System.err.println(e.getMessage());
        //     System.exit(1);
        // }
        
        
    }

    public static void uploadDDBHelper(Object jsonArray, HashMap itemValues) {
        JSONArray arr = (JSONArray) jsonArray;
        for (int i = 0; i < arr.size(); i++) {
            JSONObject obj = (JSONObject)arr.get(i);
            String name = null;
            String value = null;
            for (Object key: obj.keySet()) {   
                if (((String)key).equals("name")) {
                    name = (String)obj.get(key);
                    
                    
                }
                if (((String)key).equals("value")) {
                    value = (String)obj.get(key);
                    
                }
                if (name != null && value != null) {
                    itemValues.put(name, AttributeValue.builder().s(value).build());
                    continue;
                }
                // itemValues.put((String)key, AttributeValue.builder().s((String) obj.get(key)).build());
            }
        }
    }

    public static File downloadObject(S3Object object, S3Client s3, String reqBucket) throws IOException {
        GetObjectRequest objectRequest = GetObjectRequest
            .builder()
            .key(object.key())
            .bucket(reqBucket)
            .build();
            ResponseInputStream newObj = s3.getObject(objectRequest);
            File f = new File("temp");
            FileOutputStream fos = new FileOutputStream(f);
            byte[] read_buf = new byte[1024];
            int read_len = 0;
            while ((read_len = newObj.read(read_buf)) > 0) {
                fos.write(read_buf, 0, read_len);
            }
            newObj.close();
            fos.close();

            return f;
    }

    public static JSONObject parseJSON(File f, String modified) throws IOException {
        JSONObject jObject = null;
        JSONParser jsonParser = new JSONParser();
        try (FileReader reader = new FileReader(f))
        {
            //Read JSON file
            Object obj = jsonParser.parse(reader);
            JSONArray info = new JSONArray();
            info.add(obj);
            JSONObject lo = (JSONObject) obj;
            jObject = lo;
            jObject.put("last_modified_on", modified);
            
        } catch (org.json.simple.parser.ParseException e) {
            e.printStackTrace();
        }
        return jObject;
    }

    public static PutObjectResponse uploadToS3(JSONObject lo, String writeBucket, S3Client s3, File f) throws IOException {
        String newKey = "widgets/" + ((String) lo.get("owner")).replace(" ", "-").toLowerCase() + "/" + (String)lo.get("id");
            PutObjectRequest uploadRequest = PutObjectRequest.builder()
            .bucket(writeBucket)
            .key(newKey)
            .build();

            

        FileWriter file = new FileWriter(f);
        file.write(lo.toJSONString());
        file.flush();
        file.close();
        return s3.putObject(uploadRequest, RequestBody.fromFile(f));
        
        
    }

    public static void deleteFromS3(List<S3Object> objects, String deleteKey, String writeBucket, S3Client s3) {
        for (S3Object o: objects) {
            if (o.key().equals(deleteKey)) {
                
                // delete object
                DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(writeBucket)
                .key(deleteKey)
                .build();

                s3.deleteObject(deleteObjectRequest);
                return;
            }
        }
        System.out.println("Object to delete not found");
        return;
    }

    public static void updateS3Bucket(List<S3Object> objects, String updateKey, String writeucket, S3Client s3, JSONObject newJson) throws IOException {
        for (S3Object o: objects) {
            if (o.key().equals(updateKey)) {
                File f = downloadObject(o, s3, writeucket);
                JSONObject oldJson = parseJSON(f, o.lastModified().toString());
                
                for (Object key: newJson.keySet()) {
                    if (oldJson.containsKey(key)) {
                        if (oldJson.get(key) instanceof JSONArray) {
                            updateS3Helper(oldJson.get(key), newJson.get(key));
                        }
                        else if (((String)newJson.get(key)) == "") {
                            oldJson.put(key, null);
                        }
                        else {
                            oldJson.put(key, newJson.get(key));
                        }
                        
                    }
                }
                // for (Object remainingKey: newJson.keySet()) {
                //     oldJson.put(remainingKey, newJson.get(remainingKey));
                // }
                uploadToS3(oldJson, writeucket, s3, f);
                return;
            }
        }
        System.out.println("Object to update not found!");
        return;
    }

    public static void updateS3Helper(Object oldObj, Object newObj) {
        JSONArray newJ = (JSONArray) newObj;
        JSONArray oldJ = (JSONArray) oldObj;
        for (int i = 0; i < newJ.size(); i++) {
            // for every json object in attributes, 
        }

    }


}
