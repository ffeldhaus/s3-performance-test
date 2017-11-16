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
import org.apache.commons.codec.binary.Hex;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
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
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.security.*;
import java.security.cert.X509Certificate;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * This class demonstrates different methods to make basic requests to
 * an S3 endpoint
 * <p>
 * <b>Prerequisites:</b> You must have a valid S3 account.
 * <p>
 * <b>Important:</b> Be sure to fill in your S3 access credentials in
 * ~/.aws/credentials (C:\Users\USER_NAME\.aws\credentials for Windows
 * users) before you try to run this sample.
 */
public class PerformanceTest {

    // define some values for megabyte and kilobyte
    private static final long KB = 1024;
    private static final long MB = 1024 * KB;

    // setup logging
    final static private Logger logger = Logger.getLogger(PerformanceTest.class);
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
    @Parameter(names = {"--debug", "-d"}, description = "Enable debug level logging")
    private boolean debug = false;
    @Parameter(names = "--help", help = true)
    private boolean help = false;
    @Parameter(names = {"--tempFileDirectory", "-t"}, description = "Path to directory were temp file should be stored")
    private String tempFileDirectory = null;
    // internal variables
    private long partSize;
    private String bucketName;
    private String key;
    private AmazonS3 s3Client;
    private File sourceFile;
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

        try {
            PerformanceTest.initialize();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (KeyStoreException e) {
            e.printStackTrace();
        } catch (KeyManagementException e) {
            e.printStackTrace();
        }

        try {
            PerformanceTest.run();
        } catch (java.io.IOException ioe) {
            ioe.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        PerformanceTest.cleanup();
    }

    /**
     * Creates a sample file of size sizeInMb. If tempFileDirectory is not empty the file
     * will be created in tempFileDirectory, otherwise in the default temp directory of the OS.
     *
     * @param sizeInMb          File size
     * @param tempFileDirectory Optional directory to be used for storing temporary file
     * @return A newly created temporary file of size sizeInMb
     * @throws IOException
     */
    private static File createSampleFile(final long sizeInMb, final String tempFileDirectory) throws IOException {
        File file;
        if (StringUtils.isNullOrEmpty(tempFileDirectory)) {
            file = File.createTempFile("s3-performance-test-source-", ".dat");
        } else {
            File directory = new File(tempFileDirectory);
            file = File.createTempFile("s3-performance-test-source-", ".dat", directory);
        }

        OutputStream outputStream = new FileOutputStream(file);
        InputStream inputStream = new RandomInputStream(sizeInMb * MB);

        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            DigestInputStream digestInputStream = new DigestInputStream(inputStream, messageDigest);
            IOUtils.copy(digestInputStream, outputStream);
            String md5sum = Hex.encodeHexString(messageDigest.digest());
            logger.info("MD5 sum of source file: " + md5sum);
            inputStream.close();
            outputStream.close();
        } catch (java.security.NoSuchAlgorithmException nsae) {
            nsae.printStackTrace();
        }

        return file;
    }

    public static byte[] hexStringToByteArray(String input) {
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

    private String getFileMd5(File file) throws IOException {
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
        return md5sum;
    }

    private void uploadObjectSinglePartWithAwsSdk(String bucketName, String key, File sourceFile, AmazonS3 s3Client) {

        logger.info("Uploading object in one part using AWS SDK");

        // declare variables for performance measurement
        long startTime;
        long elapsedTime;
        long elapsedSeconds;
        float throughput;

        // create PUT request
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, sourceFile);

        // invoke put object request and calculate elapsed time for request
        startTime = System.nanoTime();
        s3Client.putObject(putObjectRequest);
        elapsedTime = System.nanoTime() - startTime;
        elapsedSeconds = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);

        // calculate throughput from elapsed time and object size
        throughput = (float) sizeInMb / elapsedSeconds;
        logger.info(String.format("Upload took %d seconds. Throughput was %.02f MB/s", elapsedSeconds, throughput));
    }

    private void uploadObjectMultiPartWithAwsSdk(String bucketName, String key, File sourceFile, TransferManager transferManager) {

        logger.info("Uploading object in multiple parts using AWS SDK High-Level API");

        // declare variables for performance measurement
        long startTime;
        long elapsedTime;
        long elapsedSeconds;
        float throughput;

        startTime = System.nanoTime();
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
        throughput = (float) sizeInMb / elapsedSeconds;
        logger.info(String.format("Upload took %d seconds. Throughput was %.02f MB/s", elapsedSeconds, throughput));
    }

    private void uploadStreamMultiPartWithPresignedUrls(String bucketName, String key, InputStream inputStream, AmazonS3 s3Client) {

        logger.info("Uploading stream in multiple parts using Pre-Signed URLs");

        // declare variables for performance measurement
        long startTime;
        long elapsedTime;
        long elapsedSeconds;
        float throughput;

        startTime = System.nanoTime();

        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, key);
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
        long numberOfParts = (sizeInMb * MB + partSize - 1) / partSize;
        for (long partNumber = 1; partNumber <= numberOfParts; partNumber++) {
            final GeneratePresignedUrlRequest pur = new GeneratePresignedUrlRequest(bucketName, key, HttpMethod.PUT);
            pur.addRequestParameter("uploadId", initResponse.getUploadId());
            pur.addRequestParameter("partNumber", Long.toString(partNumber));
            URL url = s3Client.generatePresignedUrl(pur);
            HttpURLConnection connection = null;
            try {
                connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("POST");
                connection.setRequestProperty("Content-Length", Long.toString(sizeInMb * MB));
                connection.setDoOutput(true);
                DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
                byte[] buffer = new byte[4 * 1024]; // Adjust if you want
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    wr.write(buffer, 0, bytesRead);
                }
                wr.close();
                System.out.println(connection.getContent());
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        }

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
        logger.info("Path to destination file: " + destinationFile.getAbsolutePath());

        OutputStream outputStream = new FileOutputStream(destinationFile);

        startTime = System.nanoTime();

        S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, key));
        S3ObjectInputStream inputStream = object.getObjectContent();
        try {
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

        elapsedTime = System.nanoTime() - startTime;
        elapsedSeconds = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
        throughput = (float) sizeInMb / elapsedSeconds;
        logger.info(String.format("Download took %d seconds. Throughput was %.02f MB/s", elapsedSeconds, throughput));
        destinationFile.delete();
    }

    private void downloadObjectMultiPartWithAwsSdk(String bucketName, String key, TransferManager transferManager) throws IOException {
        logger.info("Downloading object with AWS SDK High-Level API");

        // declare variables for performance measurement
        long startTime;
        long elapsedTime;
        long elapsedSeconds;
        float throughput;

        // create temporary File to save download to
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
        logger.info("Path to destination file: " + destinationFile.getAbsolutePath());

        startTime = System.nanoTime();
        try

        {
            Download download = transferManager.download(bucketName, key, destinationFile);
            download.waitForCompletion();
        } catch (
                java.lang.InterruptedException ie)

        {
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
     * @throws IOException
     */
    private void initialize() throws IOException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        if (debug) {
            logger.getLogger("com.amazonaws.request").setLevel(Level.DEBUG);
            logger.getLogger("com.netapp.s3.PerformanceTest").setLevel(Level.DEBUG);
            logger.getLogger("org.apache.http").setLevel(Level.DEBUG);
            logger.getLogger("org.apache.http.wire").setLevel(Level.ERROR);
        }

        if (insecure) {
            // disable certificate checking
            System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
        }

        if (sizeInMb < 5) {
            logger.warn("Size was specified to be below 5MB. Changing to 5MB as this is the minimum size for Multipart Uploads.");
            sizeInMb = 5;
        }

        // calculate minimum multipart upload size and limit it to max. 512MB
        partSize = sizeInMb / (long)numberOfProcessors;

        if (partSize > 512) {
            partSize = 512 * MB;
        } else if (partSize < 5) {
            partSize = 5 * MB;
        } else {
            partSize = sizeInMb * MB / numberOfProcessors;
        }

        // generate random bucket name and object key
        bucketName = "s3-performance-test-bucket-" + UUID.randomUUID();
        key = "s3-performance-test-object-" + UUID.randomUUID();

        logger.info("Endpoint: " + endpoint);
        if (insecure) {
            logger.info("Skipping SSL certificate checks");
        }
        logger.info("Number of processors: " + numberOfProcessors);
        logger.info("Part Size: " + (partSize / MB) + "MB");
        logger.info("Bucket Name: " + bucketName);
        logger.info("Object Key: " + key);
        logger.info("Object size: " + sizeInMb + "MB");
        if (!StringUtils.isNullOrEmpty(tempFileDirectory)) {
            logger.info("Directory to store temporary file: " + tempFileDirectory);
        }

        logger.info("Setting up AWS SDK S3 Client");
        if (!endpoint.isEmpty()) {
            s3Client = AmazonS3ClientBuilder
                    .standard()
                    .withPathStyleAccessEnabled(true)
                    .withEndpointConfiguration(new EndpointConfiguration(endpoint, "eu-central-1"))
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
                .withExecutorFactory(executorFactory)
                .withS3Client(s3Client)
                .build();

        // create HTTP Client
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
        logger.info("Therefore creating temporary file of size " + sizeInMb + "MB with random content. This may take a while...");
        // create temporary file
        sourceFile = createSampleFile(sizeInMb, tempFileDirectory);
        if (!keepFiles) {
            sourceFile.deleteOnExit();
        }
        logger.info("Path to source file: " + sourceFile.getAbsolutePath());

        logger.info("Creating bucket " + bucketName);
        s3Client.createBucket(bucketName);
    }

    private void run() throws IOException, NoSuchAlgorithmException {
        logger.info("### Starting Object Upload Tests ###");
        //uploadObjectSinglePartWithAwsSdk(bucketName, key, sourceFile, s3Client);
        uploadObjectMultiPartWithAwsSdk(bucketName, key, sourceFile, transferManager);

        sourceFile.delete();

        // logger.info("### Starting Streaming Upload Tests ###")
        // TODO: Create/Improve Streaming Upload Tests using Chunk uploads and PresignedUrls
        //uploadStreamMultiPartWithPresignedUrls(bucketName, key, new RandomStream(partSize / MB, 0), s3Client);

        logger.info("### Starting Object Download Tests ###");
        downloadObjectSinglePartWithAwsSdk(bucketName, key, s3Client);
        downloadObjectMultiPartWithAwsSdk(bucketName, key, transferManager);

        logger.info("### Starting Streaming Download Tests ###");
        //TODO: Create/Improve Streaming Download Tests using PresignedUrls
        downloadObjectMultiPartWithPresignedUrl(bucketName, key, s3Client);
    }

    private void cleanup() throws IOException {
        logger.info("Deleting object " + key);
        s3Client.deleteObject(bucketName, key);

        logger.info("Deleting bucket " + bucketName);
        s3Client.deleteBucket(bucketName);

        // shutdown Transfer Manager to release threads
        transferManager.shutdownNow();

        // shutting down HTTP client and HTTP connection manager
        client.close();
        connManager.close();
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