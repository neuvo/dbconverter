# Test Packages Configuration HowTo
This section will provide a How To on getting the existing Unit Test configured in the NetBeans IDE so that you can get an idea of how the methods in the classes work.

In order for this to work when installing the NetBeans IDE, JUnit must be installed. 

## Test Packages
### gov.nasa.jpl.ebis.elasticsearch.dao.util

####1. Copy or create the oracledb.properties and mongodb.properties. 

        * The copy can be made from oracledb.properties.example.md and mongodb.properties.example.md
        * But don't forget to remove the Markdown syntax

####2. Place these files under gov.nasa.jpl.ebis.elasticsearch.dao package

        * If you copied the files rename them to remove the ".example.md" from the file name
        * For the test these can be left as is or modified to contain your own values for the properties

####3. Run the test

        * Right click on the ConfigCommonsTest.java
        * Select "Test File"
        * If you have JUnit installed this will run the test and provide information under a Test Results window

### gov.nasa.jpl.ebis.elasticsearch.dao

####1. Copy or create the oracledb.properties and mongodb.properties. 

        * The copy can be made from oracledb.properties.example.md and mongodb.properties.example.md
        * But don't forget to remove the Markdown syntax

####2. Place these files under gov.nasa.jpl.ebis.elasticsearch.dao package

        * If you copied the files rename them to remove the ".example.md" from the file name
        * This one requires you have really values associated with the properties in the oracledb.properties
          and mongodb.properties files.
        * It also requires real .sql files and real .json files, these are placed in the same location as the .properties files.
        * If you do not create these most of your tests will fail.

####3. Run the test

        * Right click on the MongoDAOTest.java or OracleDAOTest.java (depending on which one you want to run)
        * Select "Test File"
        * If you have JUnit installed this will run the test and provide information under a Test Results window
      