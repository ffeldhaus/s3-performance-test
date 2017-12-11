/*
 * Copyright 2017 NetApp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.netapp.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.HttpMethod;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.client.builder.ExecutorFactory;
import com.amazonaws.regions.AwsSystemPropertyRegionProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.ServiceUtils;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.util.IOUtils;
import com.amazonaws.util.StringUtils;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import org.apache.commons.codec.binary.Hex;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import javax.net.ssl.SSLContext;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.*;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * This class demonstrates different methods to make basic requests to an S3 endpoint
 * <p>
 * <b>Prerequisites:</b> You must have a valid S3 account.
 * <p>
 * <b>Important:</b> Be sure to fill in your S3 access credentials in ~/.aws/credentials
 * (C:\Users\USER_NAME\.aws\credentials for Windows users) before you try to run this sample.
 */
public class PerformanceTest {

    // define some values for megabyte and kilobyte
    private static final long KB = 1 << 10;
    private static final long MB = KB << 10;
    private static final long GB = MB << 10;

    // setup logging
    private final static Logger logger = Logger.getLogger(PerformanceTest.class);
    private final static int numberOfProcessors = Runtime.getRuntime().availableProcessors();
    /*
     * CLI Parameters
     */
    @Parameter(names = {"--endpoint", "-e"}, description = "Custom S3 Endpoint (e.g. https://s3.example.org")
    private String endpoint = "";
    @Parameter(names = {"--size", "-s"}, description = "Size in MB")
    private int sizeInMb = 128;
    @Parameter(names = {"--insecure", "-i"}, description = "Disable SSL Certificate checking")
    private boolean insecure = true;
    @Parameter(names = {"--keep-files", "-k"}, description = "Keep upload source and download destination files")
    private boolean keepFiles = false;
    @Parameter(names = {"--region", "-r"}, description = "AWS Region to be used")
    private String region = new AwsSystemPropertyRegionProvider().getRegion();
    @Parameter(names = {"--skip-validation", "-sv"}, description = "Skip MD5 validation of uploads and downloads")
    private boolean skipValidation = false;
    @Parameter(names = {"--debug", "-d"}, description = "Enable debug level logging")
    private boolean debug = false;
    @Parameter(names = "--help", help = true)
    private boolean help = false;
    @Parameter(names = {"--tempFileDirectory", "-t"}, description = "Path to directory were temp file should be stored")
    private String tempFileDirectory;

    // internal variables
    private Path tempFileDirectoryPath;
    private long partSize;
    private int partCount;
    private String bucketName;
    private String key;
    private AmazonS3 s3Client;
    private File sourceFile;
    private String sourceFileMd5;
    private boolean skipFileUploads = false;
    private MappedByteBuffer mappedByteBuffer;
    private TransferManager transferManager;
    private CloseableHttpClient client;
    private PoolingHttpClientConnectionManager connManager;

    public static void main(String[] args) throws IOException {

        // Parameter parsing
        PerformanceTest PerformanceTest = new PerformanceTest();
        JCommander jCommander = new JCommander(PerformanceTest, args);
        if (PerformanceTest.help) {
            jCommander.usage();
            return;
        }

        // initialize and run tests and always cleanup
        try {
            PerformanceTest.initialize();
            PerformanceTest.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            PerformanceTest.cleanup();
        }
    }

    private static byte[] hexStringToByteArray(String input) {
        int len = input.length();

        if (len == 0) {
            return new byte[] {};
        }

        byte[] data;
        int startIdx;
        if (len % 2 != 0) {
            data = new byte[(len / 2) + 1];
            data[0] = (byte) Character.digit(input.charAt(0), 16);
            startIdx = 1;
        } else {
            data = new byte[len / 2];
            startIdx = 0;
        }

        for (int i = startIdx; i < len; i += 2) {
            data[(i + 1) / 2] = (byte) ((Character.digit(input.charAt(i), 16) << 4)
                    + Character.digit(input.charAt(i+1), 16));
        }
        return data;
    }

    private static Long getObjectPartSize(String bucketName, String key, AmazonS3 s3Client) {
        /* If the object was uploaded using MultiPart upload, then Transfer Manager does not verify the downloaded
         * object against the MD5 sum of the uploaded object, as the Transfer Manager does not know the part size.
         * Therefore checking if the object was uploaded using MultiPart upload and then guessing the part size from the
         * number of parts - if you have the part size of the upload available you do not need to guess.
         * The guessing will fail if the part size is not a power of 2
         */
        ObjectMetadata metadata = s3Client.getObjectMetadata(new GetObjectMetadataRequest(bucketName,key));
        String etag = metadata.getETag();
        Integer partCount = Integer.parseInt(etag.split("-")[1]);
        Long objectSize = metadata.getContentLength();

        // round object size to next power of 2
        objectSize--;
        objectSize |= objectSize >> 1;
        objectSize |= objectSize >> 2;
        objectSize |= objectSize >> 4;
        objectSize |= objectSize >> 8;
        objectSize |= objectSize >> 16;
        objectSize++;

        Long partSize = objectSize / partCount;

        // partSize is 5MB at least
        if (partSize < 5 * MB) {
            partSize = 5 * MB;
        }
        else if (partSize > 1000 * MB) {

        }

        return partSize;
    }

    /**
     * @throws IOException
     */
    private void initialize() throws IOException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        if (debug) {
            logger.getLogger("com.amazonaws.request").setLevel(Level.DEBUG);
            logger.getLogger("com.netapp.s3.PerformanceTest").setLevel(Level.DEBUG);
            logger.getLogger("org.apache.http").setLevel(Level.DEBUG);
            logger.getLogger("org.apache.http.wire").setLevel(Level.ERROR);
        }

        if (endpoint.startsWith("s3-")) {
            String endpointRegion = endpoint.split("[-.]")[1];
            if (endpointRegion != region) {
                logger.warn("Endpoint URL starts with s3-" +  endpointRegion + " but region " + endpointRegion +
                        " in URL does not match configured region " + region);
            }
        }

        if (insecure) {
            // disable certificate checking
            System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
        }

        // AWS allows 5GB parts, but limiting to 1GB to use MappedByteBuffer which is limited to less than 2GB
        if (sizeInMb / (long)numberOfProcessors > GB) {
            partSize = GB;
        } else if (sizeInMb / numberOfProcessors < 5 * MB) {
            partSize = 5 * MB;
        } else {
            partSize = sizeInMb * MB / numberOfProcessors;
        }

        partCount = (int)Math.ceil((double)sizeInMb * MB/partSize);

        // generate random bucket name and object key
        bucketName = "s3-performance-test-bucket-" + UUID.randomUUID();
        key = "s3-performance-test-object-" + UUID.randomUUID();

        logger.info("Endpoint: " + endpoint);
        logger.info("Number of processors: " + numberOfProcessors);
        logger.info("Part Size: " + (partSize / MB) + "MB");
        logger.info("Part Count: " + partCount);
        logger.info("Bucket Name: " + bucketName);
        logger.info("Object Key: " + key);
        logger.info("Object size: " + sizeInMb + "MB");
        if (!StringUtils.isNullOrEmpty(tempFileDirectory)) {
            tempFileDirectoryPath = Paths.get(tempFileDirectory);
        }
        else {
            tempFileDirectoryPath = Files.createTempDirectory("s3-performance-test-");
        }

        logger.info("Directory to store temporary files: " + tempFileDirectoryPath);

        if (insecure) {
            logger.info("Skipping SSL certificate checks");
        }

        if (skipValidation) {
            logger.info("Skipping MD5 validation");
            System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");
            System.setProperty("com.amazonaws.services.s3.disablePutObjectMD5Validation", "true");
        }

        logger.info("Setting up AWS SDK S3 Client");
        if (!endpoint.isEmpty()) {
            s3Client = AmazonS3ClientBuilder
                    .standard()
                    .withPathStyleAccessEnabled(true)
                    .withEndpointConfiguration(new EndpointConfiguration(endpoint, region))
                    .build();
        } else {
            s3Client = AmazonS3ClientBuilder
                    .standard()
                    .build();
        }

        // create a new executor factory to enable multi-threaded uploads with one thread per processor
        ExecutorFactory executorFactory = new ExecutorFactory() {
            public ExecutorService newExecutor() {
                ThreadFactory threadFactory = new ThreadFactory() {
                    private int threadCount = 1;

                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("s3-transfer-manager-worker-" + this.threadCount++);
                        return thread;
                    }
                };
                return Executors.newFixedThreadPool(numberOfProcessors, threadFactory);
            }
        };

        // create a Transfer Manager for managing S3 uploads and downloads with the AWS SDK High-Level API
        transferManager = TransferManagerBuilder
                .standard()
                .withMinimumUploadPartSize(partSize)
                .withMultipartUploadThreshold(partSize)
                .withExecutorFactory(executorFactory)
                .withS3Client(s3Client)
                .build();

        // create HTTP Client for plain HTTP transfers
        if (insecure) {
            // disable certificate checking
            final SSLContext sslContext = new SSLContextBuilder()
                    .loadTrustMaterial(null, new TrustStrategy() {
                        @Override
                        public boolean isTrusted(X509Certificate[] x509CertChain, String authType) {
                            return true;
                        }
                    })
                    .build();
            // create connection manager
            connManager
                    = new PoolingHttpClientConnectionManager(RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("http", PlainConnectionSocketFactory.INSTANCE)
                    .register("https", new SSLConnectionSocketFactory(sslContext,
                            NoopHostnameVerifier.INSTANCE))
                    .build());
            // create client
            client = HttpClients.custom()
                    .setConnectionManager(connManager)
                    .setSSLContext(sslContext)
                    .build();
        } else {
            // create connection manager
            connManager
                    = new PoolingHttpClientConnectionManager(RegistryBuilder.<ConnectionSocketFactory>create()
                    .build());
            // create client
            client = HttpClients.custom()
                    .setConnectionManager(connManager)
                    .build();
        }
        connManager.setDefaultMaxPerRoute(numberOfProcessors);
        connManager.setMaxTotal(numberOfProcessors);
        connManager.setValidateAfterInactivity(-1);

        logger.info("A file with structured data would be highly compressed by HTTP gzip and therefore cannot be used for performance testing.");
        logger.info("Therefore creating temporary file of size " + sizeInMb + "MB with random content.");
        if (sizeInMb > 128) {
            logger.info("Creating the random data may take a while...");
        }
        // create temporary file
        createSampleFile(sizeInMb, tempFileDirectoryPath, partSize);
        logger.info("Path to source file: " + sourceFile.getAbsolutePath());

        if (!skipValidation) {
            sourceFileMd5 = getFileMd5(sourceFile);
        }
        else {
            logger.info("Skipping MD5 sum calculation of file " + sourceFile.getAbsolutePath());
        }

        logger.info("Creating bucket " + bucketName);
        s3Client.createBucket(bucketName);
    }

    private void run() throws IOException, NoSuchAlgorithmException {
        logger.info("### Starting File Upload Tests ###");
        if (!skipFileUploads) {
            uploadFileSinglePartWithAwsHighLevelSdk(bucketName, key, sourceFile, s3Client);
            uploadFileMultiPartWithAwsHighLevelSdk(bucketName, key, sourceFile, transferManager);
            // TODO: Add Exception handling and Multipart aborting
            uploadFileMultiPartWithAwsLowLevelSdk(bucketName,key,sourceFile, s3Client,partSize,partCount);
        }

        logger.info("### Starting Stream Upload Tests ###");

        // create input stream based on memory mapped file
        ByteBufferBackedInputStream byteBufferBackedInputStream = new ByteBufferBackedInputStream(mappedByteBuffer);
        // TODO: Add Exception handling and Multipart aborting
        uploadStreamMultiPartWithAwsLowLevelSdk(bucketName, key, byteBufferBackedInputStream, s3Client, sizeInMb, partSize, partCount);

        mappedByteBuffer.rewind();
        uploadStreamSinglePartWithChunkUpload(bucketName, key, byteBufferBackedInputStream, s3Client, partCount);
        mappedByteBuffer.rewind();
        // TODO: Add Exception handling and Multipart aborting
        uploadStreamMultiPartWithPresignedUrls(bucketName, key, byteBufferBackedInputStream, s3Client);

        if (!keepFiles) {
            logger.info("Removing source file after uploads completed to free up space for downloads");
            sourceFile.delete();
        }

        logger.info("### Starting Object Download Tests ###");
        downloadObjectSinglePartWithAwsSdk(bucketName, key, s3Client);
        downloadObjectMultiPartWithAwsSdk(bucketName, key, transferManager);

        logger.info("### Starting Streaming Download Tests ###");
        //downloadStreamSinglePartWithPresignedUrl(bucketName, key, s3Client);
        //downloadStreamMultiPartWithPresignedUrl(bucketName, key, s3Client);
    }

    private void cleanup() throws IOException {
        logger.info("Deleting object " + key);
        s3Client.deleteObject(bucketName, key);

        logger.info("Deleting bucket " + bucketName);
        s3Client.deleteBucket(bucketName);

        logger.info("Deleting temp file directory " + tempFileDirectoryPath);
        Files.walkFileTree(tempFileDirectoryPath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes basicFileAttributes) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });

        // shutdown Transfer Manager to release threads
        transferManager.shutdownNow();

        // shutting down HTTP client and HTTP connection manager
        client.close();
        connManager.close();
    }
    
    private void uploadFileSinglePartWithAwsHighLevelSdk(String bucketName, String key, File sourceFile, AmazonS3 s3Client) {
        /* see http://docs.aws.amazon.com/AmazonS3/latest/dev/UploadObjSingleOpJava.html for details
         * Use this method if your objects are small or, if the bandwidth to your S3 storage is high, it can also be
         * used for larger objects
         * The high level SDK uses the AWS Transfer Manager which takes care of everything and is the easiest way to
         * initiate uploads
         */
        logger.info("Uploading object in one part using AWS SDK");

        // declare variables for performance measurement
        long startTime;
        long elapsedTime;
        long elapsedSeconds;
        float throughput;

        // create PUT request
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, sourceFile);

        startTime = System.nanoTime();

        // invoke put object request and calculate elapsed time for request
        s3Client.putObject(putObjectRequest);

        elapsedTime = System.nanoTime() - startTime;
        elapsedSeconds = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);

        // calculate throughput from elapsed time and object size
        throughput = (float) sizeInMb / elapsedSeconds;
        logger.info(String.format("Upload took %d seconds. Throughput was %.02f MB/s", elapsedSeconds, throughput));
    }

    private void uploadFileMultiPartWithAwsHighLevelSdk(String bucketName, String key, File sourceFile, TransferManager transferManager) {
        /* see http://docs.aws.amazon.com/AmazonS3/latest/dev/HLuploadFileJava.html for details
         * Use this method if your objects are large or, if the bandwidth to your S3 storage is low or error prone, it
         * should also be used for smaller objects. Do not use this method if your file is smaller than 5MB, as the
         * overhead is much higher than any parallelization gains.
         */
        logger.info("Uploading object in multiple parts using AWS SDK High-Level API");

        // declare variables for performance measurement
        long startTime;
        long elapsedTime;
        long elapsedSeconds;
        float throughput;

        startTime = System.nanoTime();

        // upload object using AWS SDK Transfer Manager
        Upload upload = transferManager.upload(bucketName, key, sourceFile);
        try {
            // block and wait for the upload to finish
            upload.waitForCompletion();
        } catch (AmazonClientException amazonClientException) {
            logger.error("Upload failed");
            amazonClientException.printStackTrace();
            return;
        } catch (java.lang.InterruptedException interruptedException) {
            logger.error("Upload was interrupted");
            interruptedException.printStackTrace();
            return;
        }

        elapsedTime = System.nanoTime() - startTime;
        elapsedSeconds = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);

        // calculate throughput from elapsed time and object size
        throughput = (float) sizeInMb / elapsedSeconds;
        logger.info(String.format("Upload took %d seconds. Throughput was %.02f MB/s", elapsedSeconds, throughput));
    }

    private String initiateMultiPartUploadWithAwsLowLevelSdk(String bucketName, String key, AmazonS3 s3Client) {
        InitiateMultipartUploadRequest initiateMultipartUploadRequest = new InitiateMultipartUploadRequest(bucketName, key);
        InitiateMultipartUploadResult initiateMultipartUploadResult = s3Client.initiateMultipartUpload(initiateMultipartUploadRequest);
        return initiateMultipartUploadResult.getUploadId();
    }

    private CompleteMultipartUploadResult completeMultiPartUploadWithAwsLowLevelSdk(String bucketName, String key, String uploadId, List<PartETag> partETags) {
        CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(bucketName, key, uploadId, partETags);
        return s3Client.completeMultipartUpload(completeMultipartUploadRequest);
    }

    private PartETag uploadFilePartWithAwsLowLevelSdk(String bucketName, String key, File file, AmazonS3 s3Client, String uploadId, int partNumber, long partSize, long offset) {
        // Create request to upload a part.
        UploadPartRequest uploadPartRequest = new UploadPartRequest()
                .withBucketName(bucketName)
                .withKey(key)
                .withUploadId(uploadId)
                .withPartNumber(partNumber)
                .withFileOffset(offset)
                .withFile(file)
                .withPartSize(partSize);

        // upload part
        return s3Client.uploadPart(uploadPartRequest).getPartETag();
    }

    private void uploadFileMultiPartWithAwsLowLevelSdk(String bucketName, String key, File file, AmazonS3 s3Client, Long partSize, int partCount) throws IOException {
        // every part upload will generate an ETag which equals to the MD5 sum of the part and is required to complete the MultiPart request
        List<PartETag> partETags = new ArrayList<>();

        // initiate Multipart Upload
        String uploadId = initiateMultiPartUploadWithAwsLowLevelSdk(bucketName, key, s3Client);

        long fileSize = Files.size(Paths.get(file.toString()));

        try {
            for (int partNumber = 1; partNumber <= partCount; partNumber++) {
                long offset = (partNumber - 1) * partSize;
                if (partNumber == partCount) {
                    long lastPartSize = partSize - partCount * partSize + fileSize;
                    partETags.add(uploadFilePartWithAwsLowLevelSdk(bucketName, key, file, s3Client, uploadId, partNumber, lastPartSize, offset));
                } else {
                    partETags.add(uploadFilePartWithAwsLowLevelSdk(bucketName, key, file, s3Client, uploadId, partNumber, partSize, offset));
                }
            }

            completeMultiPartUploadWithAwsLowLevelSdk(bucketName,key,uploadId,partETags);
        }
        catch (Exception e) {
            logger.warn("Failed to upload part in Multipart Upload, aborting Multipart Upload");
            AbortMultipartUploadRequest abortMultipartUploadRequest = new AbortMultipartUploadRequest(bucketName,key,uploadId);
            s3Client.abortMultipartUpload(abortMultipartUploadRequest);
            throw e;
        }
    }

    private PartETag uploadStreamPartWithAwsLowLevelSdk(String bucketName, String key, InputStream inputStream, AmazonS3 s3Client, String uploadId, int partNumber, long partSize) {
        // Create request to upload a part.
        UploadPartRequest uploadPartRequest = new UploadPartRequest()
                .withBucketName(bucketName)
                .withKey(key)
                .withUploadId(uploadId)
                .withPartNumber(partNumber)
                .withInputStream(inputStream)
                .withPartSize(partSize);

        // upload part
        return s3Client.uploadPart(uploadPartRequest).getPartETag();
    }

    private void uploadStreamMultiPartWithAwsLowLevelSdk(String bucketName, String key, InputStream inputStream, AmazonS3 s3Client, long sizeInMb, long partSize, int partCount) {
        // every part upload will generate an ETag which equals to the MD5 sum of the part and is required to complete the MultiPart request
        List<PartETag> partETags = new ArrayList<>();

        // initiate Multipart Upload
        String uploadId = initiateMultiPartUploadWithAwsLowLevelSdk(bucketName, key, s3Client);

        try {
            for (int partNumber = 1; partNumber <= partCount; partNumber++) {
                mappedByteBuffer.rewind();
                if (partNumber == partCount) {
                    long lastPartSize = partSize - partCount * partSize + sizeInMb * MB;
                    partETags.add(uploadStreamPartWithAwsLowLevelSdk(bucketName, key, inputStream, s3Client, uploadId, partNumber, lastPartSize));
                } else {
                    partETags.add(uploadStreamPartWithAwsLowLevelSdk(bucketName, key, inputStream, s3Client, uploadId, partNumber, partSize));
                }
            }
            completeMultiPartUploadWithAwsLowLevelSdk(bucketName, key, uploadId, partETags);
        }
        catch (Exception e) {
            logger.warn("Failed to upload part in Multipart Upload, aborting Multipart Upload");
            AbortMultipartUploadRequest abortMultipartUploadRequest = new AbortMultipartUploadRequest(bucketName,key,uploadId);
            s3Client.abortMultipartUpload(abortMultipartUploadRequest);
            throw e;
        }
    }

    private void uploadStreamMultiPartWithPresignedUrls(String bucketName, String key, InputStream inputStream, AmazonS3 s3Client) throws IOException {

        logger.info("Uploading stream in multiple parts using Pre-Signed URLs");

        // every part upload will generate an ETag which equals to the MD5 sum of the part and is required to complete the MultiPart request
        List<PartETag> partETags = new ArrayList<>();

        // declare variables for performance measurement
        long startTime;
        long elapsedTime;
        long elapsedSeconds;
        float throughput;

        startTime = System.nanoTime();

        // initiate MultiPart Upload
        String uploadId = initiateMultiPartUploadWithAwsLowLevelSdk(bucketName,key,s3Client);

        // create presigned upload URL for each part
        // the presigned URLs can be used by any HTTP client, even in a different application
        long numberOfParts = (sizeInMb * MB + partSize - 1) / partSize;

        try {
            for (int partNumber = 1; partNumber <= numberOfParts; partNumber++) {
                // generate presigned URL request
                final GeneratePresignedUrlRequest presignedUrlRequest = new GeneratePresignedUrlRequest(bucketName, key, HttpMethod.PUT);
                presignedUrlRequest.addRequestParameter("uploadId", uploadId);
                presignedUrlRequest.addRequestParameter("partNumber", Long.toString(partNumber));

                // add expiration date to one hour in the future for presigned URL request
                Date expiration = new Date();
                expiration.setTime(expiration.getTime() + 1000 * 60 * 60);
                presignedUrlRequest.setExpiration(expiration);

                // create presigned URL
                URL url = s3Client.generatePresignedUrl(presignedUrlRequest);

                HttpPut httpPut = new HttpPut(url.toString());

                BasicHttpEntity entity = new BasicHttpEntity();
                entity.setContent(inputStream);
                entity.setContentLength(partSize);

                CloseableHttpResponse closeableHttpResponse = client.execute(httpPut);
                partETags.add(new PartETag(partNumber, closeableHttpResponse.getFirstHeader("ETag").getValue()));
            }

            completeMultiPartUploadWithAwsLowLevelSdk(bucketName, key, uploadId, partETags);

            elapsedTime = System.nanoTime() - startTime;
            elapsedSeconds = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
            throughput = (float) sizeInMb / elapsedSeconds;
            logger.info(String.format("Upload took %d seconds. Throughput was %.02f MB/s", elapsedSeconds, throughput));
        }
        catch (Exception e) {
            logger.warn("Failed to upload part in Multipart Upload, aborting Multipart Upload");
            AbortMultipartUploadRequest abortMultipartUploadRequest = new AbortMultipartUploadRequest(bucketName,key,uploadId);
            s3Client.abortMultipartUpload(abortMultipartUploadRequest);
            throw e;
        }
    }

    private void uploadStreamSinglePartWithChunkUpload(String bucketName, String key, InputStream inputStream, AmazonS3 s3Client, int partCount) {

        logger.info("Uploading stream as chunk upload");

        // declare variables for performance measurement
        long startTime;
        long elapsedTime;
        long elapsedSeconds;
        float throughput;

        startTime = System.nanoTime();

        /*

        URL url = new URL(endpoint + '/' + bucketName);

        HttpPut httpPut = new HttpPut(url.toString());

        Map<String, String> headers = new HashMap<String, String>();
        headers.put("x-amz-storage-class", "REDUCED_REDUNDANCY");
        headers.put("x-amz-content-sha256", AWS4SignerForChunkedUpload.STREAMING_BODY_SHA256);
        headers.put("content-encoding", "" + "aws-chunked");
        headers.put("x-amz-decoded-content-length", "" + sampleContent.length());

        AWS4SignerForChunkedUpload signer = new AWS4SignerForChunkedUpload(
                endpointUrl, "PUT", "s3", regionName);

        // how big is the overall request stream going to be once we add the signature
        // 'headers' to each chunk?
        long totalLength = AWS4SignerForChunkedUpload.calculateChunkedContentLength(sampleContent.length(), userDataBlockSize);
        headers.put("content-length", "" + totalLength);

        String authorization = signer.computeSignature(headers,
                null, // no query parameters
                AWS4SignerForChunkedUpload.STREAMING_BODY_SHA256,
                awsAccessKey,
                awsSecretKey);

        // place the computed signature into a formatted 'Authorization' header
        // and call S3
        headers.put("Authorization", authorization);

        // start consuming the data payload in blocks which we subsequently chunk; this prefixes
        // the data with a 'chunk header' containing signature data from the prior chunk (or header
        // signing, if the first chunk) plus length and other data. Each completed chunk is
        // written to the request stream and to complete the upload, we send a final chunk with
        // a zero-length data payload.

        try {
            // first set up the connection
            HttpURLConnection connection = HttpUtils.createHttpConnection(endpointUrl, "PUT", headers);

            // get the request stream and start writing the user data as chunks, as outlined
            // above;
            byte[] buffer = new byte[userDataBlockSize];
            DataOutputStream outputStream = new DataOutputStream(connection.getOutputStream());

            // get the data stream
            ByteArrayInputStream inputStream = new ByteArrayInputStream(sampleContent.getBytes("UTF-8"));

            int bytesRead = 0;
            while ( (bytesRead = inputStream.read(buffer, 0, buffer.length)) != -1 ) {
                // process into a chunk
                byte[] chunk = signer.constructSignedChunk(bytesRead, buffer);

                // send the chunk
                outputStream.write(chunk);
                outputStream.flush();
            }

            // last step is to send a signed zero-length chunk to complete the upload
            byte[] finalChunk = signer.constructSignedChunk(0, buffer);
            outputStream.write(finalChunk);
            outputStream.flush();
            outputStream.close();

            // make the call to Amazon S3
            String response = HttpUtils.executeHttpRequest(connection);
        }

        */

        elapsedTime = System.nanoTime() - startTime;
        elapsedSeconds = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
        throughput = (float) sizeInMb / elapsedSeconds;
        logger.info(String.format("Upload took %d seconds. Throughput was %.02f MB/s", elapsedSeconds, throughput));
    }

    private void downloadObjectSinglePartWithAwsSdk(String bucketName, String key, AmazonS3 s3Client) throws IOException {

        logger.info("Downloading object in one part using AWS SDK");

        // declare variables for performance measurement
        long startTime;
        long elapsedTime;
        long elapsedSeconds;
        float throughput;

        // create temporary File to save download to
        File destinationFile = createTemporaryFile(sizeInMb,tempFileDirectoryPath).toFile();
        logger.info("Path to destination file: " + destinationFile.getAbsolutePath());

        startTime = System.nanoTime();

        // HEAD S3 Object to get etag

        // GET S3 Object
        S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, key));
        S3ObjectInputStream inputStream = object.getObjectContent();

        // copy object to file and calculate MD5 sum while copying
        try {
            OutputStream outputStream = new FileOutputStream(destinationFile);
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            DigestInputStream digestInputStream = new DigestInputStream(inputStream, messageDigest);
            IOUtils.copy(digestInputStream, outputStream);
            String md5sum = Hex.encodeHexString(messageDigest.digest());
            logger.info("MD5 sum of destination file: " + md5sum);
            inputStream.close();
            outputStream.close();
        } catch (java.security.NoSuchAlgorithmException noSuchAlgorithmException) {
            noSuchAlgorithmException.printStackTrace();
        }
        finally {
            // delete destination file
            if (!keepFiles) {
                destinationFile.delete();
            }
        }

        elapsedTime = System.nanoTime() - startTime;
        elapsedSeconds = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
        throughput = (float) sizeInMb / elapsedSeconds;
        logger.info(String.format("Download took %d seconds. Throughput was %.02f MB/s", elapsedSeconds, throughput));
    }

    private void downloadObjectMultiPartWithAwsSdk(String bucketName, String key, TransferManager transferManager) throws IOException {
        logger.info("Downloading object with AWS SDK High-Level API");

        // declare variables for performance measurement
        long startTime;
        long elapsedTime;
        long elapsedSeconds;
        float throughput;

        // create temporary File to save download to
        File destinationFile = createTemporaryFile(sizeInMb,tempFileDirectoryPath).toFile();
        logger.info("Path to destination file: " + destinationFile.getAbsolutePath());

        startTime = System.nanoTime();

        // download file using AWS SDK Transfer Manager
        Download download = transferManager.download(bucketName, key, destinationFile);
        try  {
            download.waitForCompletion();
        } catch (java.lang.InterruptedException ie) {
            ie.printStackTrace();
        }


        String md5sum = getFileMd5(destinationFile);
        logger.info("MD5 sum of destination file: " + md5sum);

        elapsedTime = System.nanoTime() - startTime;
        elapsedSeconds = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
        throughput = (float) sizeInMb / elapsedSeconds;
        logger.info(String.format("Download took %d seconds. Throughput was %.02f MB/s", elapsedSeconds, throughput));
        destinationFile.delete();
    }

    private void downloadObjectMultiPartWithPresignedUrl(String bucketName, String key, AmazonS3 s3Client) throws IOException, NoSuchAlgorithmException {
        logger.info("Downloading object multithreaded using pre-signed URLs and multiple parts");

        // declare variables for performance measurement
        long startTime;
        long elapsedTime;
        long elapsedSeconds;
        float throughput;

        // create temporary File to save download to
        logger.info("Creating temporary file for download");
        File destinationFile;
        if (StringUtils.isNullOrEmpty(tempFileDirectory)) {
            destinationFile = File.createTempFile("s3-performance-test-destination-", ".dat");
        } else {
            File directory = new File(tempFileDirectory);
            destinationFile = File.createTempFile("s3-performance-test-destination-", ".dat", directory);
        }
        if (!keepFiles) {
            destinationFile.deleteOnExit();
        }
        // set file to given size by creating sparse file
        RandomAccessFile randomAccessFile = new RandomAccessFile(destinationFile, "rw");
        randomAccessFile.setLength(sizeInMb * MB);
        randomAccessFile.close();

        logger.info("Path to destination file: " + destinationFile.getAbsolutePath());

        String[] md5sums;

        // check if multipart download is supported
        logger.info("Checking if multipart downloads are supported by S3 endpoint");
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
        Integer partCount = ServiceUtils.getPartCount(getObjectRequest,s3Client);
        if (partCount != null) {
            logger.info("Part count: " + partCount);
            md5sums = new String[partCount];
            MultiHttpClientConnThread[] threads = new MultiHttpClientConnThread[partCount];
            GeneratePresignedUrlRequest pur = new GeneratePresignedUrlRequest(bucketName, key, HttpMethod.GET);
            pur.addRequestParameter("partNumber","1");
            URL url = s3Client.generatePresignedUrl(pur);

            for (int i = 0; i < partCount; i++) {
                HttpGet get = new HttpGet(url.toString());
                threads[i] = new MultiHttpClientConnThread(client, get, destinationFile, true, logger);
                threads[i].number = i;
            }

            startTime = System.nanoTime();
            for (MultiHttpClientConnThread thread : threads) {
                thread.start();
            }
            for (MultiHttpClientConnThread thread : threads) {
                try {
                    thread.join();
                    md5sums[thread.number] = thread.md5sum;
                } catch (java.lang.InterruptedException ie) {
                    ie.printStackTrace();
                }
            }

            elapsedTime = System.nanoTime() - startTime;
            elapsedSeconds = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
            throughput = (float) sizeInMb / elapsedSeconds;
            logger.info(String.format("Download took %d seconds. Throughput was %.02f MB/s", elapsedSeconds, throughput));
            destinationFile.delete();
        }
        else {
            logger.info("Multipart downloads not supported, using range reads");

            partCount = (int) ((float) (sizeInMb * MB) / partSize);
            md5sums = new String[partCount];

            logger.info("Number of download threads:" + partCount);
            MultiHttpClientConnThread[] threads = new MultiHttpClientConnThread[partCount];
            GeneratePresignedUrlRequest pur = new GeneratePresignedUrlRequest(bucketName, key, HttpMethod.GET);
            URL url = s3Client.generatePresignedUrl(pur);
            long startByte;
            long endByte;
            for (int i = 0; i < partCount; i++) {
                HttpGet get = new HttpGet(url.toString());
                startByte = i * partSize;
                if (i == partCount - 1) {
                    endByte = sizeInMb * MB + 1;
                } else {
                    endByte = (i + 1) * partSize - 1;
                }
                get.setHeader("Range", "bytes=" + startByte + "-" + endByte);
                threads[i] = new MultiHttpClientConnThread(client, get, destinationFile, true, logger);
                threads[i].number = i;
            }

            startTime = System.nanoTime();
            for (MultiHttpClientConnThread thread : threads) {
                thread.start();
            }
            for (MultiHttpClientConnThread thread : threads) {
                try {
                    thread.join();
                    md5sums[thread.number] = thread.md5sum;
                } catch (java.lang.InterruptedException ie) {
                    ie.printStackTrace();
                }
            }
        }

        String joinedMd5 = StringUtils.join("",md5sums);
        byte[] byteArray = hexStringToByteArray(joinedMd5);
        MessageDigest md5Digest = MessageDigest.getInstance("MD5");
        md5Digest.update(byteArray);
        String etag = Hex.encodeHexString(md5Digest.digest()) + "-" + partCount;
        logger.info("ETag sum as calculated from MD5 sum of parts: " + etag);

        ObjectMetadata objectMetadata = s3Client.getObjectMetadata(new GetObjectMetadataRequest(bucketName,key));
        logger.info("Etag from object: " + objectMetadata.getETag());

        elapsedTime = System.nanoTime() - startTime;
        elapsedSeconds = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
        throughput = (float) sizeInMb / elapsedSeconds;

        String md5sum = getFileMd5(destinationFile);
        logger.info("MD5 sum of destination file: " + md5sum);

        logger.info(String.format("Download took %d seconds. Throughput was %.02f MB/s", elapsedSeconds, throughput));
    }

    private void downloadObjectRangeRequestWithPresignedUrl(String bucketName, String key, AmazonS3 s3Client) throws IOException, NoSuchAlgorithmException {
        logger.info("Downloading object multithreaded using pre-signed URLs");

        // declare variables for performance measurement
        long startTime;
        long elapsedTime;
        long elapsedSeconds;
        float throughput;

        // create temporary File to save download to
        logger.info("Creating temporary file for download");
        File destinationFile;
        if (StringUtils.isNullOrEmpty(tempFileDirectory)) {
            destinationFile = File.createTempFile("s3-performance-test-destination-", ".dat");
        } else {
            File directory = new File(tempFileDirectory);
            destinationFile = File.createTempFile("s3-performance-test-destination-", ".dat", directory);
        }
        if (!keepFiles) {
            destinationFile.deleteOnExit();
        }
        // set file to given size by creating sparse file
        RandomAccessFile randomAccessFile = new RandomAccessFile(destinationFile, "rw");
        randomAccessFile.setLength(sizeInMb * MB);
        randomAccessFile.close();

        logger.info("Path to destination file: " + destinationFile.getAbsolutePath());

        String[] md5sums;

        // check if multipart download is supported
        logger.info("Checking if multipart downloads are supported by S3 endpoint");
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
        Integer partCount = ServiceUtils.getPartCount(getObjectRequest,s3Client);
        if (partCount != null) {
            logger.info("Part count: " + partCount);
            md5sums = new String[partCount];
            MultiHttpClientConnThread[] threads = new MultiHttpClientConnThread[partCount];
            GeneratePresignedUrlRequest pur = new GeneratePresignedUrlRequest(bucketName, key, HttpMethod.GET);
            pur.addRequestParameter("partNumber","1");
            URL url = s3Client.generatePresignedUrl(pur);

            for (int i = 0; i < partCount; i++) {
                HttpGet get = new HttpGet(url.toString());
                threads[i] = new MultiHttpClientConnThread(client, get, destinationFile, true, logger);
                threads[i].number = i;
            }

            startTime = System.nanoTime();
            for (MultiHttpClientConnThread thread : threads) {
                thread.start();
            }
            for (MultiHttpClientConnThread thread : threads) {
                try {
                    thread.join();
                    md5sums[thread.number] = thread.md5sum;
                } catch (java.lang.InterruptedException ie) {
                    ie.printStackTrace();
                }
            }

            elapsedTime = System.nanoTime() - startTime;
            elapsedSeconds = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
            throughput = (float) sizeInMb / elapsedSeconds;
            logger.info(String.format("Download took %d seconds. Throughput was %.02f MB/s", elapsedSeconds, throughput));
            destinationFile.delete();
        }
        else {
            logger.info("Multipart downloads not supported, using range reads");

            partCount = (int) ((float) (sizeInMb * MB) / partSize);
            md5sums = new String[partCount];

            logger.info("Number of download threads:" + partCount);
            MultiHttpClientConnThread[] threads = new MultiHttpClientConnThread[partCount];
            GeneratePresignedUrlRequest pur = new GeneratePresignedUrlRequest(bucketName, key, HttpMethod.GET);
            URL url = s3Client.generatePresignedUrl(pur);
            long startByte;
            long endByte;
            for (int i = 0; i < partCount; i++) {
                HttpGet get = new HttpGet(url.toString());
                startByte = i * partSize;
                if (i == partCount - 1) {
                    endByte = sizeInMb * MB + 1;
                } else {
                    endByte = (i + 1) * partSize - 1;
                }
                get.setHeader("Range", "bytes=" + startByte + "-" + endByte);
                threads[i] = new MultiHttpClientConnThread(client, get, destinationFile, true, logger);
                threads[i].number = i;
            }

            startTime = System.nanoTime();
            for (MultiHttpClientConnThread thread : threads) {
                thread.start();
            }
            for (MultiHttpClientConnThread thread : threads) {
                try {
                    thread.join();
                    md5sums[thread.number] = thread.md5sum;
                } catch (java.lang.InterruptedException ie) {
                    ie.printStackTrace();
                }
            }
        }

        String joinedMd5 = StringUtils.join("",md5sums);
        byte[] byteArray = hexStringToByteArray(joinedMd5);
        MessageDigest md5Digest = MessageDigest.getInstance("MD5");
        md5Digest.update(byteArray);
        String etag = Hex.encodeHexString(md5Digest.digest()) + "-" + partCount;
        logger.info("ETag sum as calculated from MD5 sum of parts: " + etag);

        ObjectMetadata objectMetadata = s3Client.getObjectMetadata(new GetObjectMetadataRequest(bucketName,key));
        logger.info("Etag from object: " + objectMetadata.getETag());

        elapsedTime = System.nanoTime() - startTime;
        elapsedSeconds = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
        throughput = (float) sizeInMb / elapsedSeconds;

        String md5sum = getFileMd5(destinationFile);
        logger.info("MD5 sum of destination file: " + md5sum);

        logger.info(String.format("Download took %d seconds. Throughput was %.02f MB/s", elapsedSeconds, throughput));
    }

    /**
     * Creates a sample file of size sizeInMb. If tempFileDirectory is not empty the file
     * will be created in tempFileDirectory, otherwise in the default temp directory of the OS.
     *
     * @param sizeInMb              File size
     * @param tempFileDirectoryPath Optional directory to be used for storing temporary file
     * @return A newly created      temporary file of size sizeInMb
     * @throws IOException
     */
    private void createSampleFile(final long sizeInMb, final Path tempFileDirectoryPath, long partSize) throws IOException {
        Path temporaryFile = createTemporaryFile(sizeInMb,tempFileDirectoryPath);
        FileChannel fileChannel = FileChannel.open(temporaryFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.TRUNCATE_EXISTING);

        // using mappedByteBuffer to map the partSize
        mappedByteBuffer =  fileChannel.map(FileChannel.MapMode.READ_WRITE,0,partSize);
        long position=0;
        for (;position<partSize; position+=8) {
            mappedByteBuffer.putLong(ThreadLocalRandom.current().nextLong());
        }
        mappedByteBuffer.rewind();

        if (!skipFileUploads) {
            for (;position < sizeInMb * MB; position+=partSize) {
                if (position + partSize > sizeInMb * MB) {
                    ByteBuffer lastByteBuffer = mappedByteBuffer.duplicate();
                    lastByteBuffer.limit((int)(sizeInMb*MB-position));
                    fileChannel.write(lastByteBuffer, position);
                }
                else {
                    ByteBuffer byteBuffer = mappedByteBuffer.duplicate();
                    fileChannel.write(byteBuffer, position);
                }
            }
        }
        sourceFile = temporaryFile.toFile();
    }

    private Path createTemporaryFile(final long sizeInMb, final Path tempFileDirectoryPath) throws IOException {
        FileStore store = Files.getFileStore(tempFileDirectoryPath);
        long usableSpace = store.getUsableSpace();
        long size = sizeInMb * MB;
        if (usableSpace < size) {
            if (usableSpace < partSize) {
                throw new IOException("Not enough space to create file to back memory mapped byte buffer of size " + partSize);
            }
            logger.warn("not enough free space to create file for upload skipping all file based uploads");
            skipFileUploads = true;
            size = partSize;
        }

        logger.info("Creating temporary file");
        Path tempFile = Files.createTempFile(tempFileDirectoryPath,"s3-performance-test-",".dat");
        logger.info("Created temporary file " + tempFile);
        RandomAccessFile randomAccessFile = new RandomAccessFile(tempFile.toFile(),"rw");
        randomAccessFile.setLength(size);

        return tempFile;
    }

    private String getFileMd5(File file) throws IOException {
        logger.info("Calculating MD5 sum of file " + file.getAbsolutePath());
        FileInputStream fileInputStream = new FileInputStream(file);

        String md5sum = "";
        MessageDigest messageDigest = null;
        try {
            messageDigest = MessageDigest.getInstance("MD5");
            DigestInputStream digestInputStream = new DigestInputStream(fileInputStream, messageDigest);
            digestInputStream.read();
            byte[] buffer = new byte[8192];
            while (digestInputStream.read(buffer) != -1) ;
            digestInputStream.close();
            md5sum = Hex.encodeHexString(messageDigest.digest());
            fileInputStream.close();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        logger.info("MD5 sum is: " + md5sum);
        return md5sum;
    }

    private class MultiHttpClientConnThread extends Thread {
        private final Logger logger;

        private final CloseableHttpClient client;
        private final HttpGet get;
        private final Boolean calculateMd5;
        public String md5sum;
        public int number;

        private File destinationFile;

        public MultiHttpClientConnThread(final CloseableHttpClient client, final HttpGet get, final File destinationFile, final Boolean calculateMd5, Logger logger) {
            this.logger = logger;
            this.client = client;
            this.get = get;
            this.destinationFile = destinationFile;
            this.calculateMd5 = calculateMd5;
        }

        @Override
        public final void run() {
            try {
                logger.debug("Thread Running: " + getName());

                CloseableHttpResponse response = client.execute(get);

                String contentRange = response.getFirstHeader("Content-Range").getValue();
                Long startByte = Long.parseLong(contentRange.split("[ -/]")[1]);
                logger.debug("Start Byte " + startByte);

                Long length = response.getEntity().getContentLength();

                // workaround if Content-Length header is not available
                if (length == -1) {
                    logger.debug("Content-Length header not available, extracting length from Content-Range header");
                    length = Long.parseLong(contentRange.split("[ -/]")[2]) - Long.parseLong(contentRange.split("[ -/]")[1]) + 1    ;
                }
                logger.debug("Length: " + length);

                InputStream inputStream = response.getEntity().getContent();

                ReadableByteChannel readableByteChannel;

                MessageDigest messageDigest = MessageDigest.getInstance("MD5");
                DigestInputStream digestInputStream = new DigestInputStream(inputStream, messageDigest);
                if (calculateMd5) {
                    logger.debug("Calculating MD5 sum enabled");
                    readableByteChannel = Channels.newChannel(digestInputStream);
                } else {
                    readableByteChannel = Channels.newChannel(inputStream);
                }

                RandomAccessFile randomAccessFile = new RandomAccessFile(destinationFile, "rw");
                FileChannel fileChannel = randomAccessFile.getChannel();

                fileChannel.transferFrom(readableByteChannel, startByte, length);

                if (calculateMd5) {
                    md5sum = Hex.encodeHexString(messageDigest.digest());
                    logger.debug("Part MD5 sum: " + md5sum);
                }

                logger.debug("Thread Finished: " + getName());

                digestInputStream.close();
                response.close();
                fileChannel.close();
                randomAccessFile.close();
            } catch (final ClientProtocolException ex) {
                logger.error("", ex);
            } catch (final IOException ex) {
                logger.error("", ex);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
    }
}