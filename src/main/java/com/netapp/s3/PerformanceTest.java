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
import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.client.builder.ExecutorFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
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
import org.apache.commons.lang3.RandomUtils;
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
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import javax.net.ssl.SSLContext;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * This sample demonstrates how to make basic requests to S3 using
 * the AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid S3 account.
 * <p>
 * <b>Important:</b> Be sure to fill in your S3 access credentials in
 * ~/.aws/credentials (C:\Users\USER_NAME\.aws\credentials for Windows
 * users) before you try to run this sample.
 */
public class PerformanceTest {

    private static final int KB = 1024;
    private static final int MB = 1024 * KB;
    // setup logging
    final static private Logger logger = Logger.getLogger(PerformanceTest.class);
    /*
     * CLI Parameters
     */
    @Parameter(names = {"--endpoint", "-e"})
    private String endpoint = "florianf-s3.muccbc.hq.netapp.com:8082";
    @Parameter(names = {"--size", "-s"}, description = "Size in MB")
    private int sizeInMb = 5;
    @Parameter(names = {"--insecure", "-i"}, description = "Disable SSL Certificate checking")
    private boolean insecure = true;
    @Parameter(names = "--help", help = true)
    private boolean help = false;
    @Parameter(names = {"--tempFileDirectory", "-d"}, description = "Path to directory were temp file should be stored")
    private String tempFileDirectory = null;

    public static void main(String[] args) throws IOException {

        // Parameter parsing
        PerformanceTest PerformanceTest = new PerformanceTest();
        JCommander jCommander = new JCommander(PerformanceTest, args);
        if (PerformanceTest.help) {
            jCommander.usage();
            return;
        }

        // run programm
        PerformanceTest.run();
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
            file = File.createTempFile("s3-performance-test-", ".dat");
        } else {
            File directory = new File(tempFileDirectory);
            file = File.createTempFile("s3-performance-test-", ".dat", directory);
        }
        file.deleteOnExit();

        OutputStream outputStream = new FileOutputStream(file);
        InputStream inputStream = new RandomStream(sizeInMb * MB);

        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            DigestInputStream digestInputStream = new DigestInputStream(inputStream, messageDigest);
            IOUtils.copy(digestInputStream, outputStream);
            String md5sum = Hex.encodeHexString(messageDigest.digest());
            logger.info("MD5 sum: " + md5sum);
            inputStream.close();
            outputStream.close();
        } catch (java.security.NoSuchAlgorithmException nsae) {
            nsae.printStackTrace();
        }

        return file;
    }

    private void run() {
        if (insecure) {
            // disable certificate checking
            System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
        }

        // retrieve number of processors as basis for number of parallel threads
        final int numberOfProcessors = Runtime.getRuntime().availableProcessors();

        if (sizeInMb < 5) {
            logger.warn("Size was specified to be below 5MB. Changing to 5MB as this is the minimum size for Multipart Uploads.");
            sizeInMb = 5;
        }

        // calculate minimum multipart upload size and limit it to max. 512MB
        int partSize = sizeInMb / numberOfProcessors;

        if (partSize > 512) {
            partSize = 512 * MB;
        }
        else if (partSize < 5) {
            partSize = 5 * MB;
        }
        else {
            partSize = sizeInMb * MB / numberOfProcessors;
        }

        // initialize object metadata with content length
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(sizeInMb * MB);

        // generate random bucket name and object key
        String bucketName = "s3-performance-test-bucket-" + UUID.randomUUID();
        String key = "s3-performance-test-object-" + UUID.randomUUID();

        // declare variables for performance measurement
        long startTime;
        long elapsedTime;
        long elapsedSeconds;
        float throughput;

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

        // build S3 Client
        AmazonS3 s3Client;

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
        TransferManager tm = TransferManagerBuilder
                .standard()
                .withMinimumUploadPartSize((long) partSize)
                .withExecutorFactory(executorFactory)
                .withS3Client(s3Client)
                .build();

        try {
            logger.info("A file with structured data would be highly compressed by HTTP gzip and therefore cannot be used for performance testing.");
            logger.info("Therefore creating temporary file of size " + sizeInMb + "MB with random content. This may take a while...");
            // create temporary file
            File file;
            file = createSampleFile(sizeInMb, tempFileDirectory);
            file.deleteOnExit();
            logger.info("Path to temporary file: " + file.getAbsolutePath());

            logger.info("Creating bucket " + bucketName);
            s3Client.createBucket(bucketName);

            logger.info("Uploading object in one part");
            // create PUT request
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, file);
            // invoke put object request and calculate elapsed time for request
            startTime = System.nanoTime();
            s3Client.putObject(putObjectRequest);
            elapsedTime = System.nanoTime() - startTime;
            elapsedSeconds = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
            // calculate throughput from elapsed time and object size
            throughput = (float) sizeInMb / elapsedSeconds;
            logger.info(String.format("Upload took %d seconds. Throughput was %.02f MB/s", elapsedSeconds, throughput));

            logger.info("Uploading object in multiple parts using AWS SDK High-Level API");
            startTime = System.nanoTime();
            Upload upload = tm.upload(bucketName, key, file);
            try {
                // block and wait for the upload to finish
                upload.waitForCompletion();
            } catch (AmazonClientException amazonClientException) {
                System.out.println("Unable to upload file, upload was aborted.");
                amazonClientException.printStackTrace();
            } catch (java.lang.InterruptedException ie) {
                ie.printStackTrace();
            }
            elapsedTime = System.nanoTime() - startTime;
            elapsedSeconds = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
            throughput = (float) sizeInMb / elapsedSeconds;
            logger.info(String.format("Upload took %d seconds. Throughput was %.02f MB/s", elapsedSeconds, throughput));

            System.out.println("Uploading object in multiple parts using Pre-Signed URLs");

            InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, key);
            InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
            long numberOfParts = (sizeInMb * MB + partSize - 1) / partSize;
            for (long partNumber = 1; partNumber <= numberOfParts; partNumber++) {
                RandomStream sampleInputStream = new RandomStream(partSize / MB);
                final GeneratePresignedUrlRequest pur = new GeneratePresignedUrlRequest(bucketName, key, HttpMethod.PUT);
                pur.addRequestParameter("uploadId", initResponse.getUploadId());
                pur.addRequestParameter("partNumber", Long.toString(partNumber));
                URL url = s3Client.generatePresignedUrl(pur);
                HttpURLConnection connection = null;
                try {
                    connection = (HttpURLConnection) url.openConnection();
                    connection.setRequestMethod("POST");
                    connection.setRequestProperty("Content-Length", Integer.toString(sizeInMb * MB));
                    connection.setDoOutput(true);
                    DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
                    byte[] buffer = new byte[4 * 1024]; // Adjust if you want
                    int bytesRead;
                    while ((bytesRead = sampleInputStream.read(buffer)) != -1) {
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

            logger.info("Downloading object in one part");
            startTime = System.nanoTime();
            S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, key));
            S3ObjectInputStream inputStream = object.getObjectContent();
            byte[] buffer = new byte[4 * 1024];
            try {
                while (inputStream.read(buffer) > 0) ;
            } catch (java.io.IOException ioe) {
                System.out.println("Caught an java.io.IOException while reading downloaded data");
            }
            elapsedTime = System.nanoTime() - startTime;
            elapsedSeconds = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
            throughput = (float) sizeInMb / elapsedSeconds;
            logger.info(String.format("Download took %d seconds. Throughput was %.02f MB/s", elapsedSeconds, throughput));

            logger.info("Downloading object with AWS SDK High-Level API");
            startTime = System.nanoTime();
            try {
                Download download = tm.download(bucketName, key, file);
                download.waitForCompletion();
            } catch (java.lang.InterruptedException ie) {
                ie.printStackTrace();
            }
            elapsedTime = System.nanoTime() - startTime;
            elapsedSeconds = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
            throughput = (float) sizeInMb / elapsedSeconds;
            logger.info(String.format("Download took %d seconds. Throughput was %.02f MB/s", elapsedSeconds, throughput));

            logger.info("Downloading object multithreaded using presigned URLs");
            CloseableHttpClient client;
            PoolingHttpClientConnectionManager connManager;
            if (insecure) {
                // disable certificate checking
                final SSLContext sslContext = new SSLContextBuilder()
                        .loadTrustMaterial(null, new TrustStrategy() {
                            @Override
                            public boolean isTrusted(X509Certificate[] x509CertChain, String authType) throws CertificateException {
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
            int numberOfDownloadThreads = (int) ((float) (sizeInMb * MB) / partSize);
            logger.info("Number of download threads:" + numberOfDownloadThreads);
            MultiHttpClientConnThread[] threads
                    = new MultiHttpClientConnThread[numberOfDownloadThreads];
            GeneratePresignedUrlRequest pur = new GeneratePresignedUrlRequest(bucketName, key, HttpMethod.GET);
            URL url = s3Client.generatePresignedUrl(pur);
            long startByte;
            long endByte;
            for (int i = 0; i < numberOfDownloadThreads; i++) {
                HttpGet get = new HttpGet(url.toString());
                startByte = i * partSize;
                if (i == numberOfDownloadThreads - 1) {
                    endByte = sizeInMb * MB;
                } else {
                    endByte = (i + 1) * partSize - 1;
                }
                get.setHeader("Range", "bytes=" + startByte + "-" + endByte);
                threads[i] = new MultiHttpClientConnThread(client, get, connManager, startByte, file);
            }
            startTime = System.nanoTime();
            for (MultiHttpClientConnThread thread : threads) {
                thread.start();
            }
            for (MultiHttpClientConnThread thread : threads) {
                try {
                    thread.join();
                } catch (java.lang.InterruptedException ie) {
                    ie.printStackTrace();
                }
            }
            elapsedTime = System.nanoTime() - startTime;
            elapsedSeconds = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
            throughput = (float) sizeInMb / elapsedSeconds;
            logger.info(String.format("Download took %d seconds. Throughput was %.02f MB/s", elapsedSeconds, throughput));
            client.close();
            connManager.close();

            logger.info("Deleting object " + key);
            s3Client.deleteObject(bucketName, key);

            logger.info("Deleting bucket " + bucketName);
            s3Client.deleteBucket(bucketName);

            // shutdown Transfer Manager to release threads
            tm.shutdownNow();
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        } catch (java.io.IOException ioe) {
            logger.error("Temporary file could not be created");
            ioe.printStackTrace();
        } catch (java.security.NoSuchAlgorithmException nsae) {
            nsae.printStackTrace();
        } catch (java.security.KeyStoreException kse) {
            kse.printStackTrace();
        } catch (java.security.KeyManagementException kme) {
            kme.printStackTrace();
        }
    }

    /**
     * Creates a temporary inputStream of size sizeInMb with random data generated by a fast random number generator
     *
     * @return A newly created randomStream with size siteInMb.
     * @throws IOException
     */

    private static class RandomStream extends InputStream {

        private long pos, size;
        private int seed;

        RandomStream(long size) {
            this(size, RandomUtils.nextInt());
        }

        RandomStream(long size, int seed) {
            this.size = size;
            this.seed = seed;
        }

        @Override
        public int read() {
            byte[] data = new byte[1];
            int len = read(data, 0, 1);
            return len <= 0 ? len : data[0] & 255;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (pos >= size) {
                return -1;
            }
            len = (int) Math.min(size - pos, len);
            int x = seed, end = off + len;
            // a fast and very simple pseudo-random number generator
            // with a period length of 4 GB
            // also good: x * 9 + 1, shift 6; x * 11 + 1, shift 7
            while (off < end) {
                x = (x << 4) + x + 1;
                b[off++] = (byte) (x >> 8);
            }
            seed = x;
            pos += len;
            return len;
        }

    }

    private class MultiHttpClientConnThread extends Thread {
        private final Logger logger = Logger.getLogger(getClass());

        private final CloseableHttpClient client;
        private final HttpGet get;

        private File file;
        private long startByte = 0;

        private PoolingHttpClientConnectionManager connManager;

        public MultiHttpClientConnThread(final CloseableHttpClient client, final HttpGet get, final PoolingHttpClientConnectionManager connManager, final long startByte, final File file) {
            this.client = client;
            this.get = get;
            this.connManager = connManager;
            this.file = file;
            this.startByte = startByte;
        }

        @Override
        public final void run() {
            try {
                logger.debug("Thread Running: " + getName());

                final int size = 4 * KB;
                long offset = this.startByte;

                final CloseableHttpResponse response = client.execute(get);

                MessageDigest messageDigest = MessageDigest.getInstance("MD5");
                InputStream inputStream = response.getEntity().getContent();
                DigestInputStream digestInputStream = new DigestInputStream(inputStream, messageDigest);
                ReadableByteChannel sourceChannel = Channels.newChannel(digestInputStream);
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                FileChannel destinationChannel = randomAccessFile.getChannel();
                FileLock lock = destinationChannel.tryLock(offset, size, true);

                destinationChannel.transferFrom(sourceChannel,0, Long.MAX_VALUE);

                digestInputStream.close();
                lock.release();
                String md5sum = Hex.encodeHexString(messageDigest.digest());
                logger.info("MD5 sum: " + md5sum);

                response.close();

                logger.debug("Thread Finished: " + getName());
            } catch (final ClientProtocolException ex) {
                logger.error("", ex);
            } catch (final IOException ex) {
                logger.error("", ex);
            } catch (final java.security.NoSuchAlgorithmException ex) {
                logger.error("", ex);
            }
        }

    }
}