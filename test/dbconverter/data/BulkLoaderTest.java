/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbconverter.data;

import dbconverter.data.BulkLoader;
import dbconverter.dao.util.ConfigurationException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
//import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author hightowe
 */
public class BulkLoaderTest {
    
    public BulkLoaderTest() {
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
     * Test of BulkLoad method, of class BulkLoader.
     */
    @Test
    public void testBulkIndex() {
        // set up elasticsearch's log4j with some default settings
//        BasicConfigurator.configure();
        
        System.out.println("BulkIndex");
        
        // create the test json documents, then put them in a list
        Map<String, Object> goldfish = new HashMap();
        goldfish.put("title", "We Come Together");
        Map<String, Object> century = new HashMap();
        century.put("title", "21st Century Breakdown");
        Map<String,Object> fob = new HashMap();
        fob.put("title","Save Rock and Roll");
        
        List<Map> jsonDocs = new ArrayList();
        jsonDocs.add(goldfish);
        jsonDocs.add(century);
        jsonDocs.add(fob);
        
        
        BulkLoader instance = new BulkLoader();
        boolean passed = false;
        try {
            instance.config("music", "albums", "ebis_poc_es_dev", "EBIS POC DEV", "137.79.16.244");
            instance.bulkIndex(jsonDocs);
            
            instance.config("music1","albums","ebis_poc_es_dev","EBIS POC DEV", "137.79.16.244");
            instance.bulkIndex(jsonDocs);
            instance.config("music1","albums","ebis_poc_es_dev","EBIS POC DEV", "137.79.16.244");
            instance.bulkIndex(jsonDocs);
            
            instance.config("music2","albums2","ebis_poc_es_dev","EBIS POC DEV", "137.79.16.244");
            instance.bulkIndex(jsonDocs);
            
            passed = true;
        } catch (Exception ex) {
            System.out.println(ex);
        }
        
        assert passed;
        
        System.out.println("Check your elasticsearch cluster/node for the new documents");
    }
    
    @Test
    public void testBulkUpdate() {
        System.out.println("BulkUpdate");
        
        BulkLoader instance = new BulkLoader();
        
        instance.config("video", "television", "ebis_poc_es_dev", "EBIS POC DEV", "137.79.16.244");
        // create the test json documents, then put them in a list
        Map<String, Object> max = new HashMap();
        max.put("title", "Mad Max: Fury Road");
        Map<String, Object> deus = new HashMap();
        deus.put("title", "Ex Machina");
        Map<String, Object> pixar = new HashMap();
        pixar.put("title", "Inside Out");
        
        List<Map> jsonDocs = new ArrayList();
        jsonDocs.add(max);
        jsonDocs.add(deus);
        jsonDocs.add(pixar);
        
        try {
            instance.bulkDelete();  // clean the database
        }
        catch (ConfigurationException ex) {
            System.err.println(ex);
        }
        
        boolean passed = false;
        try {
            instance.bulkUpdate(jsonDocs);
            passed = true;
        } catch (Exception ex) {
            System.err.println(ex);
        }
        assert passed: "oldData = 0, newData > 0";
        
        Map<String,Object> starwars = new HashMap();
        starwars.put("title", "Star Wars Episode VII");
        
        jsonDocs.add(starwars);
        
        // test update with additional data
        passed = false;
        try {
            instance.bulkUpdate(jsonDocs);
            passed = true;
        } catch (Exception ex) {
            System.err.println(ex);
        }
        assert passed == true: "newData > oldData";
        
        // test update with identical data
        passed = false;
        try {
            instance.bulkUpdate(jsonDocs);
            passed = true;
        } catch (Exception ex) {
            System.err.println(ex);
        }
        assert passed == true: "newData = oldData";
        
        // test update with smaller data set
        List<Map> newJsonDocs = new ArrayList();
        
        Map<String,Object> rocky = new HashMap<>();
        rocky.put("rating",5);
        newJsonDocs.add(rocky);
        
        passed = false;
        try {
            instance.bulkUpdate(newJsonDocs);
            passed = true;
        } catch (Exception ex) {
            System.err.println(ex);
        }
        assert passed == true: "newData < oldData";
        
        // test update on empty data set
//        List<Map> newestJsonDocs = new ArrayList();
//        passed = false;
//        try {
//            instance.bulkUpdate(newestJsonDocs);
//            passed = true;
//        } catch (Exception ex) {
//            System.err.println(ex);
//        }
//        assert passed == true: "newData should be empty";
    }
    
    @Test
    public void testBulkDelete() {
        System.out.println("BulkDelete");
        
        BulkLoader instance = new BulkLoader();
        instance.config("video", "films", "ebis_poc_es_dev", "EBIS POC DEV", "137.79.16.244");
        
        // create the test json documents, then put them in a list
        Map<String, Object> max = new HashMap();
        max.put("title", "Mad Max: Fury Road");
        
        Map<String, Object> deus = new HashMap();
        deus.put("title", "Ex Machina");
        
        Map<String, Object> pixar = new HashMap();
        pixar.put("title", "Inside Out");
        
        Map<String,Object> chrispratt = new HashMap();
        chrispratt.put("title", "Jurassic World");
        
        Map<String,Object> dundundundun = new HashMap();
        dundundundun.put("title", "Jaws");
        
        Map<String,Object> heavybreathing = new HashMap();
        heavybreathing.put("title","Star Wars IV");
        
        Map<String,Object> jaime = new HashMap();
        jaime.put("title","Star Wars V");
        
        Map<String,Object> teddyguerrillas = new HashMap();
        teddyguerrillas.put("title","Star Wars VI");
        
        try {
            instance.bulkDelete();  // clean the database
        }
        catch (ConfigurationException ex) {
            System.err.println(ex);
        }
        
        List<Map> jsonDocs = new ArrayList();
        jsonDocs.add(max);
        jsonDocs.add(deus);
        jsonDocs.add(pixar);
        jsonDocs.add(chrispratt);
        jsonDocs.add(dundundundun);
        jsonDocs.add(heavybreathing);
        jsonDocs.add(jaime);
        jsonDocs.add(teddyguerrillas);
        
        instance.bulkIndex(jsonDocs);
        
        // test delete on non-empty data set
        boolean passed = false;
        try {
            instance.bulkDelete();
            passed = true;
        } catch (Exception ex) {
            System.err.println(ex);
        }
        assert passed == true: "non-empty data set";
        
        // test delete on empty data set
        passed = false;
        try {
            instance.bulkDelete();
            passed = true;
        } catch (Exception ex) {
            System.err.println(ex);
        }
        assert passed == true: "empty data set";
        
        // try to delete the entire data set
        instance.bulkIndex(jsonDocs);
        passed = false;
        try {
            instance.bulkDelete(0,8);
            passed = true;
        } catch (Exception ex) {
            System.err.println(ex);
        }
        assert passed == true: "delete(0,end)";
        
        try {
            instance.bulkDelete();  // clean the database
        }
        catch (ConfigurationException ex) {
            System.err.println(ex);
        }
        
        // test delete(int,int) on high end of data
        instance.bulkIndex(jsonDocs);
        passed = false;
        try {
            instance.bulkDelete(4,8);
            passed = true;
        } catch (Exception ex) {
            System.err.println(ex);
        }
        assert passed == true: "delete high end";
        
        try {
            instance.bulkDelete();  // clean the database
        }
        catch (ConfigurationException ex) {
            System.err.println(ex);
        }
        
        // remove from the low end
        instance.bulkIndex(jsonDocs);
        passed = false;
        try {
            instance.bulkDelete(0,5);
            passed = true;
        } catch (Exception ex) {
            System.err.println(ex);
        }
        assert passed == true: "delete low end";
        
        try {
            instance.bulkDelete();  // clean the database
        }
        catch (ConfigurationException ex) {
            System.err.println(ex);
        }
        
        // take a piece out of the middle
        instance.bulkIndex(jsonDocs);
        passed = false;
        try {
            instance.bulkDelete(2,7);
            passed = true;
        } catch (Exception ex) {
            System.err.println(ex);
        }
        assert passed == true: "delete from middle";
        
        // try to delete the entire data set when the middle is missing
        passed = false;
        try {
            instance.bulkDelete(0,8);
            passed = true;
        } catch (Exception ex) {
            System.err.println(ex);
        }
        assert passed == true: "wipe with missing center";
        
        try {
            instance.bulkDelete();  // clean the database
        }
        catch (ConfigurationException ex) {
            System.err.println(ex);
        }
    }
    
    // TODO: write testBulkDelete(int,int)
    
}
