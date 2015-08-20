# ebis.elasticsearch.poc

---

## A Java API that allows you to connect to the following databases:

* Oracle

* MongoDB

* MSSQL

* CSV files

This will expand to include other databases in the future



## The API allows you to utilize configuration files to generate data for Elasticsearch

The configuration files allow you to set properties that make connections to the to the databases, 

read the data from tables or collections via .sql file or .json files specified in the .properties files.

Configuration files also determine whether to write the source data to a JSON file,

append it to an existing Elasticsearch index, or replace an elasticsearch index

with the new data.

You will need to specify which configuration file you want to use via a command line argument.

## The API provides the result set data in its normal database state or as JSON objects.

The JSON objects can then be used to generate Elasticsearch bulk load .json 

files that can be ingested by Elasticsearch.


## Examples of the .properties files

* [oracledb.properties](oracledb.properties.example.md)

* [mongodb.properties](mongodb.properties.example.md)

* [mssqldb.properties] (mssqldb.properties.example.md)

* [csvdb.properties] (csvdb.properties.example.md)

These can actually be copied directly but don't forget to rename them by removing the ".example.md" part of the file name.

Please note that if copying these example.md files as your base do not forget to remove the Markdown syntax in them.



## Test Packages with JUnit

The packages contain Tests that allow you to examine and test the code, so long 
as you provide the correct properties files and .sql or .json files for testing.
These files must be located in the same directory as the test code you run.

The tests also provide examples of how to use the methods in the tested classes

* [Test Packages Configuration HOWTO](docs/TestPackagesConfig_HowTo.md)


## Built on NetBeans 8.0.2

So if you fork it and use NetBeans as your IDE it should have all the NetBeans project info.


## Requirements

Java 7 or 8 JDK (Java Developers Kit)

Please note all of the required libraries are in the libs folder
### Libs

* ojdbc7.jar
* commons-configuration-1.10-javadoc.jar
* commons-configuration-1.10-sources.jar
* commons-configuration-1.10-test-sources.jar
* commons-configuration-1.10-tests.jar
* commons-configuration-1.10.jar
* commons-logging-1.2-javadoc.jar
* commons-logging-1.2.jar
* commons-lang-2.6-javadoc.jar
* commons-lang-2.6-sources.jar
* commons-lang-2.6.jar
* commons-io-2.4-javadoc.jar
* commons-io-2.4-sources.jar
* commons-io-2.4-test-sources.jar
* commons-io-2.4-tests.jar
* commons-io-2.4.jar
* log4j-core-2.3-javadoc.jar
* log4j-core-2.3-sources.jar
* log4j-core-2.3-tests.jar
* log4j-core-2.3.jar
* log4j-api-2.3-javadoc.jar
* log4j-api-2.3-sources.jar
* log4j-api-2.3.jar
* libs-log4j2_resources
* json-lib-2.4-jdk15.jar
* ezmorph-1.0.6.jar
* commons-collections-3.2.1-javadoc.jar
* commons-collections-3.2.1-sources.jar
* commons-collections-3.2.1.jar
* commons-collections-testframework-3.2.1.jar
* commons-beanutils-1.8.3-javadoc.jar
* commons-beanutils-1.8.3-sources.jar
* commons-beanutils-1.8.3.jar
* commons-beanutils-bean-collections-1.8.3.jar
* commons-beanutils-core-1.8.3.jar
* mongo-java-driver-3.0.2.jar
* mongo-java-driver-3.0.2-javadoc.jar
* mongo-java-driver-3.0.2-sources.jar
* elasticsearch-1.6.0.jar
* opencsv-3.4.jar
* lucene-queries-4.10.4.jar
* lucene-analyzers-common-4.10.4.jar
* lucene-core-4.10.4.jar
* jtds-1.2.5.jar
* joda-time-2.4.jar

## Elasticsearch HOWTO
[Installing Elasticsearch, Marvel Sense and Kibana](elasticsetup.md)
