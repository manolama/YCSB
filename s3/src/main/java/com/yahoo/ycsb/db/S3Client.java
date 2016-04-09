/**
 * Copyright (c) 2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 *
 * S3 storage client binding for YCSB.
 */
package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.net.*;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.*;
import com.amazonaws.auth.*;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;

/**
 * S3 Storage client for YCSB framework.
 *
 * Properties to set:
 *
 * s3.accessKeyId=access key S3 aws
 * s3.secretKey=secret key S3 aws
 * s3.endPoint=s3.amazonaws.com
 * s3.region=us-east-1
 * The parameter table is the name of the Bucket where to upload the files.
 * This must be created before to start the benchmark
 * The size of the file to upload is determined by two parameters:
 * - fieldcount this is the number of fields of a record in YCSB
 * - fieldlength this is the size in bytes of a single field in the record
 * together these two parameters define the size of the file to upload,
 * the size in bytes is given by the fieldlength multiplied by the fieldcount.
 * The name of the file is determined by the parameter key.
 *This key is automatically generated by YCSB.
 *
 */
public class S3Client extends DB {

  private static AmazonS3Client s3Client;
  private static String sse;
  private static SSECustomerKey ssecKey;
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /**
  * Cleanup any state for this storage.
  * Called once per S3 instance;
  */
  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      try {
        s3Client.shutdown();
        System.out.println("The client is shutdown successfully");
      } catch (Exception e){
        System.err.println("Could not shutdown the S3Client: "+e.toString());
        e.printStackTrace();
      } finally {
        if (s3Client != null){
          s3Client = null;
        }
      }
    }
  }
  /**
  * Delete a file from S3 Storage.
  *
  * @param bucket
  *            The name of the bucket
  * @param key
  * The record key of the file to delete.
  * @return OK on success, otherwise ERROR. See the
  * {@link DB} class's description for a discussion of error codes.
  */
  @Override
  public Status delete(String bucket, String key) {
    try {
      s3Client.deleteObject(new DeleteObjectRequest(bucket, key));
    } catch (Exception e){
      System.err.println("Not possible to delete the key "+key);
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }
  /**
  * Initialize any state for the storage.
  * Called once per S3 instance; If the client is not null it is re-used.
  */
  @Override
  public void init() throws DBException {
    final int count = INIT_COUNT.incrementAndGet();
    synchronized (S3Client.class){
      Properties propsCL = getProperties();
      int recordcount = Integer.parseInt(
          propsCL.getProperty("recordcount"));
      int operationcount = Integer.parseInt(
          propsCL.getProperty("operationcount"));
      int numberOfOperations = 0;
      if (recordcount > 0){
        if (recordcount > operationcount){
          numberOfOperations = recordcount;
        } else {
          numberOfOperations = operationcount;
        }
      } else {
        numberOfOperations = operationcount;
      }
      if (count <= numberOfOperations) {
        String accessKeyId = null;
        String secretKey = null;
        String endPoint = null;
        String region = null;
        String maxErrorRetry = null;
        String maxConnections = null;
        String protocol = null;
        BasicAWSCredentials s3Credentials;
        ClientConfiguration clientConfig;
        if (s3Client != null) {
          System.out.println("Reusing the same client");
          return;
        }
        try {
          InputStream propFile = S3Client.class.getClassLoader()
              .getResourceAsStream("s3.properties");
          Properties props = new Properties(System.getProperties());
          props.load(propFile);
          accessKeyId = props.getProperty("s3.accessKeyId");
          if (accessKeyId == null){
            accessKeyId = propsCL.getProperty("s3.accessKeyId");
          }
          System.out.println(accessKeyId);
          secretKey = props.getProperty("s3.secretKey");
          if (secretKey == null){
            secretKey = propsCL.getProperty("s3.secretKey");
          }
          System.out.println(secretKey);
          endPoint = props.getProperty("s3.endPoint");
          if (endPoint == null){
            endPoint = propsCL.getProperty("s3.endPoint", "s3.amazonaws.com");
          }
          System.out.println(endPoint);
          region = props.getProperty("s3.region");
          if (region == null){
            region = propsCL.getProperty("s3.region", "us-east-1");
          }
          System.out.println(region);
          maxErrorRetry = props.getProperty("s3.maxErrorRetry");
          if (maxErrorRetry == null){
            maxErrorRetry = propsCL.getProperty("s3.maxErrorRetry", "15");
          }
          maxConnections = props.getProperty("s3.maxConnections");
          if (maxConnections == null){
            maxConnections = propsCL.getProperty("s3.maxConnections");
          }
          protocol = props.getProperty("s3.protocol");
          if (protocol == null){
            protocol = propsCL.getProperty("s3.protocol", "HTTPS");
          }
          sse = props.getProperty("s3.sse");
          if (sse == null){
            sse = propsCL.getProperty("s3.sse", "false");
          }
          String ssec = props.getProperty("s3.ssec");
          if (ssec == null){
            ssec = propsCL.getProperty("s3.ssec", null);
          } else {
            ssecKey = new SSECustomerKey(ssec);
          }
        } catch (Exception e){
          System.err.println("The file properties doesn't exist "+e.toString());
          e.printStackTrace();
        }
        try {
          System.out.println("Inizializing the S3 connection");
          s3Credentials = new BasicAWSCredentials(accessKeyId, secretKey);
          clientConfig = new ClientConfiguration();
          clientConfig.setMaxErrorRetry(Integer.parseInt(maxErrorRetry));
          if(protocol.equals("HTTP")) {
            clientConfig.setProtocol(Protocol.HTTP);
          } else {
            clientConfig.setProtocol(Protocol.HTTPS);
          }
          if(maxConnections != null) {
            clientConfig.setMaxConnections(Integer.parseInt(maxConnections));
          }
          s3Client = new AmazonS3Client(s3Credentials, clientConfig);
          s3Client.setRegion(Region.getRegion(Regions.fromName(region)));
          s3Client.setEndpoint(endPoint);
          System.out.println("Connection successfully initialized");
        } catch (Exception e){
          System.err.println("Could not connect to S3 storage: "+ e.toString());
          e.printStackTrace();
          throw new DBException(e);
        }
      } else {
        System.err.println(
            "The number of threads must be less or equal than the operations");
        throw new DBException(new Error(
            "The number of threads must be less or equal than the operations"));
      }
    }
  }
  /**
  * Create a new File in the Bucket. Any field/value pairs in the specified
  * values HashMap will be written into the file with the specified record
  * key.
  *
  * @param bucket
  *            The name of the bucket
  * @param key
  *      The record key of the file to insert.
  * @param values
  *            A HashMap of field/value pairs to insert in the file.
  *            Only the content of the first field is written to a byteArray
  *            multiplied by the number of field. In this way the size
  *            of the file to upload is determined by the fieldlength
  *            and fieldcount parameters.
  * @return OK on success, ERROR otherwise. See the
  *         {@link DB} class's description for a discussion of error codes.
  */
  @Override
  public Status insert(String bucket, String key,
      HashMap<String, ByteIterator> values) {
    return writeToStorage(bucket, key, values, true, sse, ssecKey);
  }
  /**
  * Read a file from the Bucket. Each field/value pair from the result
  * will be stored in a HashMap.
  *
  * @param bucket
  *            The name of the bucket
  * @param key
  *            The record key of the file to read.
  * @param fields
  *            The list of fields to read, or null for all of them,
  *            it is null by default
  * @param result
  *          A HashMap of field/value pairs for the result
  * @return OK on success, ERROR otherwise.
  */
  @Override
  public Status read(String bucket, String key, Set<String> fields,
        HashMap<String, ByteIterator> result) {
    return readFromStorage(bucket, key, result, ssecKey);
  }
  /**
  * Update a file in the database. Any field/value pairs in the specified
  * values HashMap will be written into the file with the specified file
  * key, overwriting any existing values with the same field name.
  *
  * @param bucket
  *            The name of the bucket
  * @param key
  *            The file key of the file to write.
  * @param values
  *            A HashMap of field/value pairs to update in the record
  * @return OK on success, ERORR otherwise.
  */
  @Override
  public Status update(String bucket, String key,
        HashMap<String, ByteIterator> values) {
    return writeToStorage(bucket, key, values, false, sse, ssecKey);
  }
  /**
  * Perform a range scan for a set of files in the bucket. Each
  * field/value pair from the result will be stored in a HashMap.
  *
  * @param bucket
  *            The name of the bucket
  * @param startkey
  *            The file key of the first file to read.
  * @param recordcount
  *            The number of files to read
  * @param fields
  *            The list of fields to read, or null for all of them
  * @param result
  *            A Vector of HashMaps, where each HashMap is a set field/value
  *            pairs for one file
  * @return OK on success, ERROR otherwise.
  */
  @Override
  public Status scan(String bucket, String startkey, int recordcount,
        Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return scanFromStorage(bucket, startkey, recordcount, result, ssecKey);
  }
  /**
  * Upload a new object to S3 or update an object on S3.
  *
  * @param bucket
  *            The name of the bucket
  * @param key
  *            The file key of the object to upload/update.
  * @param values
  *            The data to be written on the object
  * @param updateMarker
  *            A boolean value. If true a new object will be uploaded
  *            to S3. If false an existing object will be re-uploaded
  *
  */
  protected Status writeToStorage(String bucket, String key,
        HashMap<String, ByteIterator> values, Boolean updateMarker,
            String sseLocal, SSECustomerKey ssecLocal) {
    int totalSize = 0;
    int fieldCount = values.size(); //number of fields to concatenate
    // getting the first field in the values
    Object keyToSearch = values.keySet().toArray()[0];
    // getting the content of just one field
    byte[] sourceArray = values.get(keyToSearch).toArray();
    int sizeArray = sourceArray.length; //size of each array
    if (updateMarker){
      totalSize = sizeArray*fieldCount;
    } else {
      try {
        Map.Entry<S3Object, ObjectMetadata> objectAndMetadata = getS3ObjectAndMetadata(bucket, key, ssecLocal);
        int sizeOfFile = (int)objectAndMetadata.getValue().getContentLength();
        fieldCount = sizeOfFile/sizeArray;
        totalSize = sizeOfFile;
        objectAndMetadata.getKey().close();
      } catch (Exception e){
        System.err.println("Not possible to get the object :"+key);
        e.printStackTrace();
        return Status.ERROR;
      }
    }
    byte[] destinationArray = new byte[totalSize];
    int offset = 0;
    for (int i = 0; i < fieldCount; i++) {
      System.arraycopy(sourceArray, 0, destinationArray, offset, sizeArray);
      offset += sizeArray;
    }
    try (InputStream input = new ByteArrayInputStream(destinationArray)) {
      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setContentLength(totalSize);
      PutObjectRequest putObjectRequest = null;
      if (sseLocal.equals("true")) {
        metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        putObjectRequest = new PutObjectRequest(bucket, key,
            input, metadata);
      } else if (ssecLocal != null) {
        putObjectRequest = new PutObjectRequest(bucket, key,
            input, metadata).withSSECustomerKey(ssecLocal);
      } else {
        putObjectRequest = new PutObjectRequest(bucket, key,
            input, metadata);
      }

      try {
        PutObjectResult res =
            s3Client.putObject(putObjectRequest);
        if(res.getETag() == null) {
          return Status.ERROR;
        } else {
          if (sseLocal.equals("true")) {
            System.out.println("Uploaded object encryption status is " +
                res.getSSEAlgorithm());
          } else if (ssecLocal != null) {
            System.out.println("Uploaded object encryption status is " +
                res.getSSEAlgorithm());
          }
        }
      } catch (Exception e) {
        System.err.println("Not possible to write object :"+key);
        e.printStackTrace();
        return Status.ERROR;
      }
    } catch (Exception e) {
      System.err.println("Error in the creation of the stream :"+e.toString());
      e.printStackTrace();
      return Status.ERROR;
    }

    return Status.OK;
  }

  /**
  * Download an object from S3.
  *
  * @param bucket
  *            The name of the bucket
  * @param key
  *            The file key of the object to upload/update.
  * @param result
  *            The Hash map where data from the object are written
  *
  */
  protected Status readFromStorage(String bucket, String key,
        HashMap<String, ByteIterator> result, SSECustomerKey ssecLocal) {
    try {
      Map.Entry<S3Object, ObjectMetadata> objectAndMetadata = getS3ObjectAndMetadata(bucket, key, ssecLocal);
      InputStream objectData = objectAndMetadata.getKey().getObjectContent(); //consuming the stream
      // writing the stream to bytes and to results
      int sizeOfFile = (int)objectAndMetadata.getValue().getContentLength();
      byte[] inputStreamToByte = new byte[sizeOfFile];
      objectData.read(inputStreamToByte, 0, sizeOfFile);
      result.put(key, new ByteArrayByteIterator(inputStreamToByte));
      objectData.close();
      objectAndMetadata.getKey().close();
    } catch (Exception e){
      System.err.println("Not possible to get the object "+key);
      e.printStackTrace();
      return Status.ERROR;
    }

    return Status.OK;
  }

  private Map.Entry<S3Object, ObjectMetadata> getS3ObjectAndMetadata(String bucket,
                                                                     String key, SSECustomerKey ssecLocal) {
    GetObjectRequest getObjectRequest;
    GetObjectMetadataRequest getObjectMetadataRequest;
    if (ssecLocal != null) {
      getObjectRequest = new GetObjectRequest(bucket,
              key).withSSECustomerKey(ssecLocal);
      getObjectMetadataRequest = new GetObjectMetadataRequest(bucket,
              key).withSSECustomerKey(ssecLocal);
    } else {
      getObjectRequest = new GetObjectRequest(bucket, key);
      getObjectMetadataRequest = new GetObjectMetadataRequest(bucket,
              key);
    }

    return new AbstractMap.SimpleEntry<>(s3Client.getObject(getObjectRequest),
            s3Client.getObjectMetadata(getObjectMetadataRequest));
  }

  /**
  * Perform an emulation of a database scan operation on a S3 bucket.
  *
  * @param bucket
  *            The name of the bucket
  * @param startkey
  *            The file key of the first file to read.
  * @param recordcount
  *            The number of files to read
  * @param fields
  *            The list of fields to read, or null for all of them
  * @param result
  *            A Vector of HashMaps, where each HashMap is a set field/value
  *            pairs for one file
  *
  */
  protected Status scanFromStorage(String bucket, String startkey,
      int recordcount, Vector<HashMap<String, ByteIterator>> result,
          SSECustomerKey ssecLocal) {

    int counter = 0;
    ObjectListing listing = s3Client.listObjects(bucket);
    List<S3ObjectSummary> summaries = listing.getObjectSummaries();
    List<String> keyList = new ArrayList();
    int startkeyNumber = 0;
    int numberOfIteration = 0;
    // getting the list of files in the bucket
    while (listing.isTruncated()) {
      listing = s3Client.listNextBatchOfObjects(listing);
      summaries.addAll(listing.getObjectSummaries());
    }
    for (S3ObjectSummary summary : summaries) {
      String summaryKey = summary.getKey();
      keyList.add(summaryKey);
    }
    // Sorting the list of files in Alphabetical order
    Collections.sort(keyList); // sorting the list
    // Getting the position of the startingfile for the scan
    for (String key : keyList) {
      if (key.equals(startkey)){
        startkeyNumber = counter;
      } else {
        counter = counter + 1;
      }
    }
    // Checking if the total number of file is bigger than the file to read,
    // if not using the total number of Files
    if (recordcount < keyList.size()) {
      numberOfIteration = recordcount;
    } else {
      numberOfIteration = keyList.size();
    }
    // Reading the Files starting from the startkey File till the end
    // of the Files or Till the recordcount number
    for (int i = startkeyNumber; i < numberOfIteration; i++){
      HashMap<String, ByteIterator> resultTemp =
          new HashMap<String, ByteIterator>();
      readFromStorage(bucket, keyList.get(i), resultTemp,
          ssecLocal);
      result.add(resultTemp);
    }
    return Status.OK;
  }
}
