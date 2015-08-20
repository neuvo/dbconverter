/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbconverter.data;

import dbconverter.dao.util.ConfigurationException;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.*;
import static java.lang.System.in;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.logging.log4j.LogManager;

/**
 * Object to handle sending JSON data to Elasticsearch
 * 
 * @author hightowe
 */
public class BulkLoader {
    private String INDEX_NAME;
    private String TYPE_NAME;
    private String CLUSTER_NAME;
    private String NODE_NAME;
    private String SERVER_NAME;
    
    // ensure the server has port 9300 open
    private final int SERVER_PORT = 9300;
    
    // name of the file containing records of last used _id values
    private final String RECORDS_FILE = "bulkRecords.txt";
    
    // delimiter used in RECORDS_FILE
    private final char DELIMITER = ',';
    
    private final static org.apache.logging.log4j.Logger logger = LogManager.getLogger(BulkLoader.class.getName());
    
    /**
     * Configures the BulkLoader so that it can perform load operations properly
     * Settings are persistent through all function calls, until config is
     *  called again
     * @param index
     * @param type
     * @param cluster_name
     * @param node_name
     * @param server_name 
     */
    public void config(String index, String type, String cluster_name, 
                       String node_name, String server_name) {
        INDEX_NAME = index;
        TYPE_NAME = type;
        CLUSTER_NAME = cluster_name;
        NODE_NAME = node_name;
        SERVER_NAME = server_name;
    }
    
    /**
     * Handles bulk indexing of documents, as well as ID assignment
     * This method appends all new data to the existing elasticsearch data
     * Does NOT delete existing data
     * Relies on locally stored bulkRecords.txt to assign ID values
     * @param jsonDocs The documents, as Maps, to be indexed
     * @author hightowe
     */
    public void bulkIndex(List<Map> jsonDocs) {
        if (!this.isConfigured()) {
            logger.error("BulkLoader is not configured");
            return;
        }
        
        Client client = null;
        
        try {
            client = this.getClient();
        }
        catch (Exception ex) {
            logger.error(ex);
        }
        
        // client is only null if there's no connection
        assert client != null;
        
        // the object which will execute the bulk request
        BulkRequestBuilder bulkRequestor = client.prepareBulk();
        
        int count = this.getLastID();
        
        for (Map m : jsonDocs) {
            bulkRequestor.add(client.prepareIndex(INDEX_NAME,TYPE_NAME,Integer.toString(count))
                .setSource(m));
            count++;    // increment count, so IDs are unique
        }
        
        BulkResponse response = this.executeRequest(bulkRequestor);
        
        if (response == null) {
            logger.fatal("bulkIndex failed");
        }
        else {
            // update bulkRecords with the new id starting point
            this.setLastID(count);
        }
        
        client.close();
    }
    
    /**
     * Updates existing Elasticsearch data with the given configuration
     * Replaces the existing Elasticsearch data with data passed in
     * This means that any old data that isn't represented in the new data will 
     *  be deleted
     * @param jsonDocs The documents, in Map form, to be uploaded
     * @author hightowe
     */
    public void bulkUpdate(List<Map> jsonDocs) {
        if (!this.isConfigured()) {
            logger.error("BulkLoader is not configured");
            return;
        }
        
        assert jsonDocs.size() > 0;
        
        int elasticSize = jsonDocs.size();
        int lastID = this.getLastID();
        
        // delete any excess old data on elastic server
        if (elasticSize < lastID) {
            try {
                this.bulkDelete(elasticSize,lastID);
            }
            catch (ConfigurationException ex) {
                logger.error("Failed to delete excess documents: " + ex);
            }
        }
        
        // reset lastID to 0, index as per normal
        this.setLastID(0);
        
        // should always upload new data
        this.bulkIndex(jsonDocs);
    }
    
    /**
     * Deletes all documents in the current Index and Type where _id is in [0,lastID)
     * Careful not to delete anything you need!
     * 
     * @throws ConfigurationException
     * @author hightowe
     */
    public void bulkDelete() throws ConfigurationException {
        
        if (!this.isConfigured()) {
            throw new ConfigurationException("BulkLoader is not configured");
        }
        
        int lastID = this.getLastID();
        
        if (lastID == 0) {
            logger.info("Empty data set, aborting delete...");
            return;
        }
        
        Client client = null;
        
        try {
            client = this.getClient();
        }
        catch (Exception ex) {
            logger.error(ex);
        }
        
        assert client != null;
        
        // the object which will execute the bulk request
        BulkRequestBuilder bulkRequestor = client.prepareBulk();
        
        int id = lastID;
        
        // prepare to delete every document where _id is in range (0,lastID]
        for (int i = 0; i < id; i++) {
            bulkRequestor.add(client.prepareDelete(INDEX_NAME,TYPE_NAME,Integer.toString(i)));
        }
        
        BulkResponse response = this.executeRequest(bulkRequestor);
        
        if (response == null) {
            logger.fatal("bulkDelete() failed");
        }
        else {
            // reset bulkRecords's lastId parameter to 0
            this.setLastID(0);
        }
        
        client.close();
        
        logger.info("Deleted " + Integer.toString(lastID) + " documents");
    }
    
    /**
     * Executes actionGet for the inputted BulkRequestBuilder
     * Returns null if unable to begin executing request
     * Small method created in accordance with DRY principle
     * @param bulkRequestor The BulkRequestBuilder to be executed
     * @return a BulkResponse object, which can be used to investigate errors
     * @author hightowe
     */
    BulkResponse executeRequest(BulkRequestBuilder bulkRequestor) {
        BulkResponse response = null;
        try {
            response = bulkRequestor.execute().actionGet();
            
            if (response.hasFailures()) {
                logger.fatal(response.buildFailureMessage());
            }
        }
        catch (NoNodeAvailableException nonode) {
            logger.error(nonode);
            logger.error("Check that port 9300 is open on the server");
        }
        
        return response;
    }
    
    /**
     * Deletes all documents with "_id"s in [start,end)
     * Make sure not to take chunks out of the middle of the database
     *   unless you're very sure of what you're doing
     * WARNING: Does NOT update records file
     * @param start The id of the first document to delete
     * @param end One greater than the id of the last document to delete
     * @throws ConfigurationException
     * @author hightowe
     */
    public void bulkDelete(int start, int end) throws ConfigurationException {
        if (!this.isConfigured()) {
            throw new ConfigurationException("BulkLoader is not configured");
        }
        
        // check how many documents the index contains
        int lastID = this.getLastID();
        
        if (lastID == 0) {
            throw new ConfigurationException("Empty data set, aborting delete...");
        }
        
        if (start >= end || start < 0) {
            throw new ConfigurationException("Invalid start, end parameters: " + 
                    Integer.toString(start) + ", " + Integer.toString(end));
        }
        
        Client client = null;
        
        try {
            client = this.getClient();
        }
        catch (Exception ex) {
            logger.error(ex);
        }
        
        assert client != null;
        
        // the object which will execute the bulk request
        BulkRequestBuilder bulkRequestor = client.prepareBulk();
        
        // prepare bulkRequestor for deletion of all docs in [start,end)
        for (int i = start; i < end; i++) {
            bulkRequestor.add(client.prepareDelete(INDEX_NAME,TYPE_NAME,
                    Integer.toString(i)));
        }
        
        BulkResponse response = this.executeRequest(bulkRequestor);
        
        if (response == null) {
            logger.fatal("bulkDelete(" + start + "," + end + ") failed");
        }
        
        client.close();
        
        logger.info("Deleted " + Integer.toString(end-start) + " documents");
    }
    
    /**
     * Reads file containing previous queries' Types, Indices, and final IDs
     * Returns the next id to be used for new entries into INDEX and TYPE
     * If INDEX and TYPE are empty or don't exist, returns 0
     * @return
     * @author hightowe
     */
    public int getLastID() {
        
        if (!this.isConfigured()) {
            logger.error("BulkLoader is not configured");
            return -1;
        }
        
        String key = this.recordsKey();
        
        // assign the next valid id value
        // if this index and type has never had documents indexed, returns 0
        int id = 0;
        try {
            FileInputStream fstream = new FileInputStream(RECORDS_FILE);
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
            String strLine;
            while ((strLine = br.readLine()) != null) {
                if (strLine.contains(key)) {
                    int equalsIndex = strLine.lastIndexOf('=');
                    String tempID = strLine.substring(equalsIndex+1,strLine.length());
                    id = Integer.parseInt(tempID);
                    break;
                }
            }
        }
        catch (IOException ex) {
            logger.info("ID info does not exist, starting at 0: " + ex);
        }
        
        return id;
    }
    
    /**
     * Updates the records file with the next valid id for the index, type on
     *  this server.
     * Code liberally borrowed from here:
     *  http://stackoverflow.com/questions/11100381/to-edit-a-specific-line-in-a-textfile-using-java-program
     * @param id The value of the next valid id
     * @author hightowe
     */
    public void setLastID(int id) {
        
        if (!this.isConfigured()) {
            logger.error("BulkLoader is not configured");
            return;
        }
        
        FileInputStream fstream = null;
        
        String key = this.recordsKey();
        String lineToAdd = makeLine(key,Integer.toString(id));
        
        // attempt to read from records file. if it doesn't exist, create it,
        // and write the new data to it
        try {
            fstream = new FileInputStream(RECORDS_FILE);
        } catch (FileNotFoundException fnfe) {
            this.appendLine(lineToAdd);
            return;
        }
        
        assert fstream != null: "Failed to read file";
        
        // prepare to read the original file
        BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
        String strLine;
        
        // will ultimately write this string to the records file
        StringBuilder fileContent = new StringBuilder();
        
        try {
            // check if the file already contains a line corresponding to key
            boolean found = false;
            //Read File Line By Line
            while ((strLine = br.readLine()) != null) {
                if (strLine.contains(key)) {
                    strLine = lineToAdd;
                    found = true;
                }

                // append strLine no matter what
                fileContent.append(strLine);
                fileContent.append('\n');
            }
            
            // file doesn't contain a line for this key, append one
            if (!found) {
                fileContent.append(lineToAdd);
            }
            
            FileWriter fstreamWrite = new FileWriter(RECORDS_FILE);
            BufferedWriter out = new BufferedWriter(fstreamWrite);
            // write fileContent to file, overwriting old contents
            out.write(fileContent.toString());
            out.close();
            // Close the input stream
            in.close();
        }
        catch (IOException ex) {
            logger.fatal("setLastID: " + ex);
        }
        
        try {
            br.close();
        }
        catch (IOException ex) {
            logger.error(ex);
        }
    }
    
    /**
     * Produces a properly formatted line for the records file
     * @param key The properties field name
     * @param value The properties field value
     * @return 
     */
    private String makeLine(String key, String value) {
        StringBuilder newString = new StringBuilder();
        newString.append(key);
        newString.append('=');
        newString.append(value);

        return newString.toString();
    }
    
    /**
     * Writes the given line to the records file by appending the entire line
     * If records file does not exist, appendLine will create the file
     * Code taken from top answer here:
     * http://stackoverflow.com/questions/2885173/how-to-create-a-file-and-write-to-a-file-in-java
     * @param line The line to append
     * @author hightowe
     */
    private void appendLine(String line) {
        PrintWriter writer = null;
        
        try {
            writer = new PrintWriter(RECORDS_FILE, "UTF-8");
        } catch (FileNotFoundException | UnsupportedEncodingException ex) {
            logger.info(RECORDS_FILE + " not found, attempting to create...");
        }
        
        assert writer != null: "Failed to access file " + RECORDS_FILE;
        
        writer.println(line);
        writer.close();
    }
    
    /**
     * Constructs a string used to look up IDs from the records file
     * @return 
     * @author hightowe
     */
    private String recordsKey() {
        if (!this.isConfigured()) {
            logger.error("BulkLoader is not configured");
            return null;
        }
        return CLUSTER_NAME + DELIMITER + NODE_NAME + DELIMITER + SERVER_NAME + DELIMITER
               + INDEX_NAME + DELIMITER + TYPE_NAME;
    }
    
    /**
     * Returns a client object, according to config settings
     * Written in accordance with the DRY principle
     * @return Configured Client object
     * @author hightowe
     */
    private Client getClient() {
        
        if (!this.isConfigured()) {
            logger.error("BulkLoader is not configured");
            return null;
        }
        
        Client client = null;
        
        // settings necessary to find the correct cluster and node
        Settings settings = ImmutableSettings
                                .settingsBuilder()
                                .put("node.name",NODE_NAME)
                                .put("cluster.name",CLUSTER_NAME)
                                .build();
        try {
            // create transport client to connect to node, apply settings
            // transport client doesn't require creating a node instance
            client = new TransportClient(settings)
            .addTransportAddress(new InetSocketTransportAddress(SERVER_NAME, SERVER_PORT));
        }
        catch (Exception ex) {
            logger.fatal(ex);
        }
        
        return client;
    }
    
    /**
     * Checks if all vital fields have been given a value
     * @return True if all vital fields have a value, false otherwise
     * @author hightowe
     */
    public boolean isConfigured() {
        boolean configged = true;
        
        if (INDEX_NAME == null || TYPE_NAME == null || CLUSTER_NAME == null
                || NODE_NAME == null || SERVER_NAME == null) {
            configged = false;
        }
        
        return configged;
    }
}