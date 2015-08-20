/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbconverter.dao.util;

import dbconverter.dao.util.ConfigCommons;
import dbconverter.dao.util.QueryObject;
import dbconverter.dao.TestUtils;
import java.net.URL;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author sanchez
 */
public class ConfigCommonsTest {

    private final int LINE_LENGTH = 100;
    private final String CLASS_NAME =  ConfigCommonsTest.class.getName();
    private final Logger logger = LogManager.getLogger(CLASS_NAME);


    public ConfigCommonsTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
        logger.info("Processing tests from the class");
        logger.info(CLASS_NAME);
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of loadConfig method, of class ConfigCommons.
     */
    @Test
    public void testLoadConfigOracleDb() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        System.out.println("loadConfig" + TestUtils.getClassBeingTestedInfo(ConfigCommonsTest.class.getName()));
        TestUtils.getLineSeparator("*", LINE_LENGTH);

        String configFileName = "oracledb.properties";
        URL configFile = ConfigCommonsTest.class.getResource(configFileName);
        ConfigCommons instance = new ConfigCommons();
        boolean passed = false;

        try {
            instance.loadConfig(configFile.getFile());
            passed = true;
        } catch (Exception e) {
            logger.error("Could not load config file " + configFile);
            logger.error(e);
        }

        assert passed;
        System.out.println("Loaded config file " + configFileName);
        TestUtils.printSpace();

    }
    
    /**
     * Test of loadConfig method, of class ConfigCommons.
     */
    @Test
    public void testLoadConfigMongoDb() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        System.out.println("loadConfig" + TestUtils.getClassBeingTestedInfo(ConfigCommonsTest.class.getName()));
        TestUtils.getLineSeparator("*", LINE_LENGTH);

        String configFileName = "mongodb.properties";
        URL configFile = ConfigCommonsTest.class.getResource(configFileName);
        ConfigCommons instance = new ConfigCommons();
        boolean passed = false;

        try {
            instance.loadConfig(configFile.getFile());
            passed = true;
        } catch (Exception e) {
            logger.error("Could not load config file " + configFile);
            logger.error(e);
        }

        assert passed;
        System.out.println("Loaded config file " + configFileName);
        TestUtils.printSpace();

    }
    
    /**
     * Test of loadConfig method, of class ConfigCommons.
     * Test case where db is of type mssqldb
     * 
     * @author hightowe
     */
    @Test
    public void testLoadConfigMssqlDb() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        System.out.println("loadConfigMssqlDb" + TestUtils.getClassBeingTestedInfo(ConfigCommonsTest.class.getName()));
        TestUtils.getLineSeparator("*", LINE_LENGTH);

        String configFileName = "mssqldb.properties";
        
        URL configFile = ConfigCommonsTest.class.getResource(configFileName);
        ConfigCommons instance = new ConfigCommons();
        boolean passed = false;

        try {
            instance.loadConfig(configFile.getFile());
            passed = true;
        } catch (Exception e) {
            logger.error("Could not load config file " + configFile);
            logger.error(e);
        }

        assert passed;
        System.out.println("Loaded config file " + configFileName);
        System.out.println("Result: " + instance.getJdbcParametersMssql());
        TestUtils.printSpace();
    }
    
    /**
     * Test of testLoadConfig method for use on a csv properties file
     */
    @Test
    public void testLoadConfigCSVDB() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        System.out.println("loadConfigCSV" + TestUtils.getClassBeingTestedInfo(ConfigCommonsTest.class.getName()));
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        
        String configFileName = "csvdb.properties";
        
        URL configFile = ConfigCommonsTest.class.getResource(configFileName);
        ConfigCommons instance = new ConfigCommons();
        boolean passed = false;

        try {
            instance.loadConfig(configFile.getFile());
            passed = true;
        } catch (Exception e) {
            logger.error("Could not load config file " + configFile);
            logger.error(e);
        }

        assert passed;
        System.out.println("Loaded config file " + configFileName);
        TestUtils.printSpace();
    }

    /**
     * Test of getQueryFiles method, of class ConfigCommons.
     */
    @Test
    public void testGetQueryFilesMssql() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        System.out.println("getQueryFiles Mssql" + TestUtils.getClassBeingTestedInfo(ConfigCommonsTest.class.getName()));
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        ConfigCommons instance = new ConfigCommons();

        String configFileName = "mssqldb.properties";
        URL configFile = ConfigCommonsTest.class.getResource(configFileName);

        instance.loadConfig(configFile.getFile());

        List<String> qFiles = instance.getQueryFiles();
        assert !qFiles.isEmpty();

        for (String filesNames : qFiles) {
            System.out.println(filesNames);
        }

        TestUtils.getLineSeparator("=", LINE_LENGTH);
    }

    /**
     * Test of getQueryFiles method, of class ConfigCommons.
     */
    @Test
    public void testGetQueryFilesOracle() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        System.out.println("getQueryFiles Oracle" + TestUtils.getClassBeingTestedInfo(ConfigCommonsTest.class.getName()));
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        ConfigCommons instance = new ConfigCommons();

        String configFileName = "oracledb.properties";
        URL configFile = ConfigCommonsTest.class.getResource(configFileName);

        instance.loadConfig(configFile.getFile());

        List<String> qFiles = instance.getQueryFiles();
        assert !qFiles.isEmpty();

        for (String filesNames : qFiles) {
            System.out.println(filesNames);
        }

        TestUtils.getLineSeparator("=", LINE_LENGTH);
    }
    
    @Test
    public void testGetQueryFilesMongo() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        System.out.println("getQueryFiles Mongo" + TestUtils.getClassBeingTestedInfo(ConfigCommonsTest.class.getName()));
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        ConfigCommons instance = new ConfigCommons();

        String configFileName = "mongodb.properties";
        URL configFile = ConfigCommonsTest.class.getResource(configFileName);

        instance.loadConfig(configFile.getFile());

        List<String> qFiles = instance.getQueryFiles();
        assert !qFiles.isEmpty();

        for (String filesNames : qFiles) {
            System.out.println(filesNames);
        }

        TestUtils.getLineSeparator("=", LINE_LENGTH);
    }
    
    /**
     * Check that getCsvFiles returns a non-empty list of files, and that getQueryFiles
 returns a null list
     */
    @Test
    public void testGetCSVFiles() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        System.out.println("getCSVFiles" + TestUtils.getClassBeingTestedInfo(ConfigCommonsTest.class.getName()));
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        ConfigCommons instance = new ConfigCommons();

        String configFileName = "csvdb.properties";
        URL configFile = ConfigCommonsTest.class.getResource(configFileName);

        instance.loadConfig(configFile.getFile());

        List<String> qFiles = instance.getQueryFiles();
        assert qFiles == null;
        
        List<String> csvFiles = instance.getQueryFiles();
        assert !csvFiles.isEmpty();

        for (String filesNames : csvFiles) {
            System.out.println(filesNames);
        }

        TestUtils.getLineSeparator("=", LINE_LENGTH);
    }
    
    /**
     * Test of getJdbcParamaters method, of class ConfigCommons.
     */
    @Test
    public void testGetJdbcParamaters() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        System.out.println("getJdbcParamaters" + TestUtils.getClassBeingTestedInfo(ConfigCommonsTest.class.getName()));
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        ConfigCommons instance = new ConfigCommons();

        String configFileName = "oracledb.properties";
        URL configFile = ConfigCommonsTest.class.getResource(configFileName);

        instance.loadConfig(configFile.getFile());
        String jdbcParam = instance.getJdbcParametersOracle();

        assert jdbcParam.contains("oracle");
        System.out.println(instance.getJdbcParametersOracle());

        TestUtils.getLineSeparator("=", LINE_LENGTH);
    }

    /**
     * Test of getEnvironment method, of class ConfigCommons.
     */
    @Test
    public void testGetEnvironment() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        System.out.println("getEnvironment" + TestUtils.getClassBeingTestedInfo(ConfigCommonsTest.class.getName()));
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        
        ConfigCommons instance = new ConfigCommons();

        String configFileName = "oracledb.properties";
        URL configFile = ConfigCommonsTest.class.getResource(configFileName);

        instance.loadConfig(configFile.getFile());
        String env = instance.getEnvironment();

        assert "DEV".equals(env);
        System.out.println(env);

        TestUtils.getLineSeparator("=", LINE_LENGTH);
    }

    /**
     * Test of getDbType method, of class ConfigCommons.
     */
    @Test
    public void testGetDbTypeOracle() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        System.out.println("getDbType" + TestUtils.getClassBeingTestedInfo(ConfigCommonsTest.class.getName()));
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        
        ConfigCommons instance = new ConfigCommons();

        String configFileName = "oracledb.properties";
        URL configFile = ConfigCommonsTest.class.getResource(configFileName);

        instance.loadConfig(configFile.getFile());

        String dbType = instance.getDbType();

        assert dbType != null || !dbType.isEmpty();

        System.out.println(dbType);

        TestUtils.getLineSeparator("=", LINE_LENGTH);
    }
    
    /**
     * Test of getDbType method, of class ConfigCommons.
     */
    @Test
    public void testGetDbTypeMongo() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        System.out.println("getDbType" + TestUtils.getClassBeingTestedInfo(ConfigCommonsTest.class.getName()));
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        
        ConfigCommons instance = new ConfigCommons();

        String configFileName = "mongodb.properties";
        URL configFile = ConfigCommonsTest.class.getResource(configFileName);

        instance.loadConfig(configFile.getFile());

        String dbType = instance.getDbType();

        assert dbType != null || !dbType.isEmpty();

        System.out.println(dbType);

        TestUtils.getLineSeparator("=", LINE_LENGTH);
    }

    /**
     * Test of getMongoDbURI method, of class ConfigCommons.
     */
    @Test
    public void testGetMongoDbURI() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        System.out.println("getMongoDbURI" + TestUtils.getClassBeingTestedInfo(CLASS_NAME));
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        
        
        ConfigCommons instance = new ConfigCommons();

        String configFileName = "mongodb.properties";
        URL configFile = ConfigCommonsTest.class.getResource(configFileName);

        instance.loadConfig(configFile.getFile());
        
        String mongoDbURI = instance.getMongoDbURI();
        
        assert mongoDbURI != null && !mongoDbURI.isEmpty();
        System.out.println(mongoDbURI);
        
        TestUtils.printSpace();
    }

    /**
     * Test of getMongoDbName method, of class ConfigCommons.
     */
    @Test
    public void testGetMongoDbName() {
        
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        System.out.println("getMongoDbName" + TestUtils.getClassBeingTestedInfo(CLASS_NAME));
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        
        ConfigCommons instance = new ConfigCommons();

        String configFileName = "mongodb.properties";
        URL configFile = ConfigCommonsTest.class.getResource(configFileName);

        instance.loadConfig(configFile.getFile());
        
        String mongoDbName = instance.getMongoDbName();
        
        assert mongoDbName != null && !mongoDbName.isEmpty();
        System.out.println(mongoDbName);
        
        TestUtils.printSpace();
    }

    /**
     * Test of getMongoDbCollectionName method, of class ConfigCommons.
     */
    @Test
    public void testGetMongoDbCollectionName() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        System.out.println("getMongoDbCollectionName" + TestUtils.getClassBeingTestedInfo(CLASS_NAME));
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        
        ConfigCommons instance = new ConfigCommons();

        String configFileName = "mongodb.properties";
        URL configFile = ConfigCommonsTest.class.getResource(configFileName);

        instance.loadConfig(configFile.getFile());
        
        String mongoDbCollectionName = instance.getMongoDbCollectionName();
        
        assert mongoDbCollectionName != null && !mongoDbCollectionName.isEmpty();
        System.out.println(mongoDbCollectionName);
        
        TestUtils.printSpace();
    }
    
    @Test
    public void testGetQueryObjects() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        System.out.println("getQueryObjects" + TestUtils.getClassBeingTestedInfo(CLASS_NAME));
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        
        // test on file with only one set of each property (query, index, type, cluster, node, server)
        String configFileName2 = "testdb2.properties";
        URL configFile2 = ConfigCommonsTest.class.getResource(configFileName2);
        
        ConfigCommons instance2 = new ConfigCommons();
        instance2.loadConfig(configFile2.getFile());
        
        List<QueryObject> objects = null;
        objects = instance2.getQueryObjects();
        
        assert objects.size() == 1;
        QueryObject theobj = objects.get(0);
        assert theobj.getQueryFile().equals("bigmongo.json");
        assert theobj.getIndexName().equals("increasing");
        assert theobj.getTypeName().equals("increasingdata");
        assert theobj.getClusterName().equals("ebis_poc_es_dev");
        assert theobj.getNodeName().equals("EBIS POC DEV");
        assert theobj.getServerName().equals("137.79.16.244");
        assert theobj.getWriteFlag() == true;
        assert theobj.getUpdateFlag() == true;
        assert theobj.getIndexFlag() == false;
        
        
        String configFileName = "testdb.properties";
        URL configFile = ConfigCommonsTest.class.getResource(configFileName);
        ConfigCommons instance = new ConfigCommons();
        boolean complete = false;
        try {
            instance.loadConfig(configFile.getFile());
            complete = true;
        }
        catch (Exception ex) {
            
        }
        assert complete;
        assert instance.getQueryObjects() == null;
    }

}
