/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbconverter.dao;

import dbconverter.dao.MongoDAO;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.util.JSON;
import dbconverter.dao.util.ConfigCommons;
import dbconverter.dao.util.Utils;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author sanchez
 */
public class MongoDAOTest {

    private static String mongoConfigFileName = "mongodb.properties";
    private static URL mongoConfigFile = OracleDAOTest.class.getResource(mongoConfigFileName);
    private static ConfigCommons configCommons = null;
    private final int LENGTH = 100;
    private final String CLASS_NAME = MongoDAOTest.class.getName();
    private final Logger logger = LogManager.getLogger(CLASS_NAME);

    public MongoDAOTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        configCommons = new ConfigCommons();
        configCommons.loadConfig(mongoConfigFile.getFile());
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of getConnection method, of class MongoDAO.
     */
    @Test
    public void testGetConnection() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LENGTH);
        System.out.println("getConnection" + TestUtils.getClassBeingTestedInfo(CLASS_NAME));
        TestUtils.getLineSeparator("*", LENGTH);
        MongoDAO instance = new MongoDAO();
        
        String mongoURI = configCommons.getMongoDbURI();
        
        MongoClient connection = instance.getConnection(mongoURI);
        assert connection != null;
        System.out.println(connection.getConnectPoint());

        TestUtils.printSpace();

    }

    /**
     * Test of getCollection method, of class MongoDAO.
     */
    @Test
    public void testGetCollection() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LENGTH);
        System.out.println("getCollection" + TestUtils.getClassBeingTestedInfo(CLASS_NAME));
        TestUtils.getLineSeparator("*", LENGTH);

        MongoDAO mongoDAO = new MongoDAO();
        String mongoURI = configCommons.getMongoDbURI();
        
        MongoClient connection = mongoDAO.getConnection(mongoURI);

        String dbName = configCommons.getMongoDbName();
        String collectionName = configCommons.getMongoDbCollectionName();

        MongoCollection<Document> collection = mongoDAO.getCollection(connection, dbName, collectionName);
        
        assert collection.count() > 0L;
        System.out.println("Current number of documents in " + collectionName + " is: " + collection.count());
        
        TestUtils.printSpace();
    }
    
    @Test
    public void testGetDocuments() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LENGTH);
        System.out.println("getDocuments" + TestUtils.getClassBeingTestedInfo(CLASS_NAME));
        TestUtils.getLineSeparator("*", LENGTH);
        
        
        MongoDAO instance = new MongoDAO();
        String mongoURI = configCommons.getMongoDbURI();
        
        MongoClient connection = instance.getConnection(mongoURI);

        String dbName = configCommons.getMongoDbName();
        String collectionName = configCommons.getMongoDbCollectionName();
        
        
        
        Utils util = new Utils();
        String query = configCommons.getQueryFiles().get(0);

        URL queryFileName = MongoDAOTest.class.getResource(query);
        String jsonQuery = util.readQueryFile(queryFileName.getFile());
       

        MongoCollection<Document> collection = instance.getCollection(connection, dbName, collectionName);
        
        FindIterable<Document> document = instance.getDocuments(jsonQuery, collection);
        
        assert document != null;
        System.out.println(document.first().toJson());
        
        
        
        TestUtils.printSpace();
    }

    /**
     * Test of getDocumentsAll method, of class MongoDAO.
     */
    @Test
    public void testGetDocumentsAll() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LENGTH);
        System.out.println("getDocumentsAll" + TestUtils.getClassBeingTestedInfo(CLASS_NAME));
        TestUtils.getLineSeparator("*", LENGTH);
        
        
        MongoDAO instance = new MongoDAO();
        String mongoURI = configCommons.getMongoDbURI();
        
        MongoClient connection = instance.getConnection(mongoURI);

        String dbName = configCommons.getMongoDbName();
        String collectionName = configCommons.getMongoDbCollectionName();
        
        
        
        Utils util = new Utils();
        String query = configCommons.getQueryFiles().get(0);

        URL queryFileName = MongoDAOTest.class.getResource(query);
        String jsonQuery = util.readQueryFile(queryFileName.getFile());
       

        MongoCollection<Document> collection = instance.getCollection(connection, dbName, collectionName);
        
        FindIterable<Document> document = instance.getDocumentsAll(collection);
        
        assert document != null;
        System.out.println(document.first().toJson());
        
        
        
        
        TestUtils.printSpace();
    }

    /**
     * Test of getDocumentsInJSONFormat method, of class MongoDAO.
     */
    @Test
    public void testGetDocumentsInJSONFormat() {
        
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LENGTH);
        System.out.println("getDocumentsInJSONFormat" + TestUtils.getClassBeingTestedInfo(CLASS_NAME));
        TestUtils.getLineSeparator("*", LENGTH);
        
         MongoDAO instance = new MongoDAO();
        String mongoURI = configCommons.getMongoDbURI();
        
        MongoClient connection = instance.getConnection(mongoURI);

        String dbName = configCommons.getMongoDbName();
        String collectionName = configCommons.getMongoDbCollectionName();
        
        
        
        Utils util = new Utils();
        String query = configCommons.getQueryFiles().get(0);

        URL queryFileName = MongoDAOTest.class.getResource(query);
        String jsonQuery = util.readQueryFile(queryFileName.getFile());
       

        MongoCollection<Document> collection = instance.getCollection(connection, dbName, collectionName);
        
        FindIterable<Document> documents = instance.getDocuments(jsonQuery, collection);
        
        List<String> jsonDocuments = new ArrayList<>();
        
        jsonDocuments.addAll(instance.getDocumentsInJSONFormat(documents));
        
        assert jsonDocuments.size() > 0;
        System.out.println("There are " + jsonDocuments.size() + " documents.");
        System.out.println("Filtered by the json query");
        System.out.println(jsonQuery);
        System.out.println(jsonDocuments.get(0));
        
        
        
        TestUtils.printSpace();
    }

}
