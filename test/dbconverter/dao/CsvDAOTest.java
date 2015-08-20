/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbconverter.dao;

import dbconverter.dao.CsvDAO;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author hightowe
 */
public class CsvDAOTest {
    
    //private static String mongoConfigFileName = "mongodb.properties";
    //private static URL mongoConfigFile = OracleDAOTest.class.getResource(mongoConfigFileName);
    private final int LENGTH = 100;
    private final String CLASS_NAME = CsvDAOTest.class.getName();
    private final Logger logger = LogManager.getLogger(CLASS_NAME);
    private final String FILE_NAME = "test.csv";
    private final URL dbURL = CsvDAOTest.class.getResource(FILE_NAME);
    private final String FILE_PATH = dbURL.getFile();
    
    public CsvDAOTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
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
     * Test of readFile method, of class CsvDAO.
     * @author hightowe
     */
    @Test
    public void testReadFile() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LENGTH);
        System.out.println("readFile" + TestUtils.getClassBeingTestedInfo(CLASS_NAME));
        TestUtils.getLineSeparator("*", LENGTH);
        
        System.out.println("Getting " + FILE_PATH);
        
        CsvDAO instance = new CsvDAO();
        List<String[]> results = instance.readFile(FILE_PATH);
        assert results != null;
        System.out.println("Successfully read file.");
        
//        for (String[] i:results) {
//            for (String j:i) {
//                System.out.print(j + ", ");
//            }
//            System.out.println("");
//        }
    }
    
    /**
     * Test of getObjects method, of class CsvDAO.
     * Confirms that getObjects doesn't return null or have runtime errors
     * @author hightowe
     */
    @Test
    public void testGetObjects() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LENGTH);
        System.out.println("getObjects" + TestUtils.getClassBeingTestedInfo(CLASS_NAME));
        TestUtils.getLineSeparator("*", LENGTH);
        
        CsvDAO instance = new CsvDAO();
        List<Map> objects = instance.getObjects(FILE_PATH);
        assert objects != null: "objects should not be null!";
        
        
        //System.out.println(objects.toString());
        
    }
}
