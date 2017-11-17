# s3-performance-test

Java S3 Performance Tests demonstrating different methods to upload and download data via S3.

## Usage

[Download the latest release of the jar file from the release page](https://github.com/ffeldhaus/s3-performance-test/releases/latest).

Setup S3 Credentials as described by Amazon in [Set up AWS Credentials and Region for Development ](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html).

Show the help with
```
java -jar performance-test.jar --help
```

To run with the defaults (e.g. against AWS S3) just use
```
java -jar performance-test.jar
```

To run with custom endpoint and specific object size, specify optional parameters and change 
`<protocol>://<endpoint>[:<port>]` (e.g. `https://s3.example.com:8082`) and `<size in MB>` (e.g. 1024`) as required:
```
java -jar performance-test.jar --endpoint <protocol>://<endpoint>[:<port>] --size <size in MB>
```