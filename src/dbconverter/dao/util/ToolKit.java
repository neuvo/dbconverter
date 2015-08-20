/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbconverter.dao.util;

import com.mongodb.client.FindIterable;
import com.opencsv.CSVReader;
import dbconverter.data.BulkLoader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.sf.json.JSONObject;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.joda.time.DateTime;

/**
 * Container for a variety of miscellaneous functions, mainly those that
 * perform bulk operations to Elasticsearch
 * @author hightowe
 */
public class ToolKit {
    private final static Logger logger = LogManager.getLogger(ToolKit.class.getName());
    private static final String PARAMETER_ERROR = "Parameters cannot be null";
    private static final String TIME_STAMP_FORMAT = "yyyy-MM-dd'T'HH:mm'Z'";
    private static final String TIME_STAMP = "elastic timestamp";
    private static final String MONGO_ID = "mongo_id";
    
    /**
     * Converts a given map to a JSON String
     * @param mapToConvert The map to be converted
     * @return String representation of JSON object
     */
    public static String convertMapToJson(Map mapToConvert) {
        JSONObject json = new JSONObject();
        json.putAll(mapToConvert);
        return json.toString();
    }

    /**
     * Given a ResultSet, writes the contained data as JSON to a target file,
     *  with the expectation that said file will be used in an Elasticsearch
     *  bulk index operation.
     * This method supports arbitrary-sized ResultSets, provided interval is set low enough
     * @param resultSet The ResultSet to save to a file
     * @param obj A QueryObject which must contain the index and type of the target
     * @param interval Determines how many documents should be stored within Java at a time
     *                 If you run out of heap space, try decreasing this value
     * @param fileName The name of the file to write to
     * @author hightowe
     */
    public static void writeResultSetToJson(ResultSet resultSet, QueryObject obj, int interval, String fileName) {
        assert resultSet != null : "ResultSet cannont be null!";

        List<String> resultsList = new ArrayList<>();

        try {
            ResultSetMetaData rsMetaData = resultSet.getMetaData();
            int columnNumbers = rsMetaData.getColumnCount();
            int count = 0;
            int prev = 0;
            while (resultSet.next()) {
                Map<String, Object> dataMap = new HashMap<>();
                
                // add all column names to the map key-set
                for (int i = 1; i <= columnNumbers; i++) {
                    dataMap.put(rsMetaData.getColumnLabel(i), resultSet.getObject(i));
                }
                
                dataMap.put(TIME_STAMP, getISOTime(TIME_STAMP_FORMAT));

                // Add the data to List of Maps
                String json = ToolKit.convertMapToJson(dataMap);
                resultsList.add(json);
                count++;
                
                // write to file after every (interval)th run, then clear
                // resultsList to avoid heap space errors
                if (count % interval == 0) {
                    writeJsonStringsToFile(resultsList, fileName, obj, prev);
                    prev += interval;
                    resultsList.clear();
                }
            }
            
            writeJsonStringsToFile(resultsList,fileName,obj,prev);
            
        } catch (SQLException e) {
            logger.error(e);
        }
    }
    
    /**
     * Creates a file containing all the given JSON documents in the order they
     *  were provided, formatted for future bulk index operations
     * @param jsonStrings The JSON documents in String format
     * @param fileName The name of the file to write to
     * @param obj A configured QueryObject
     * @param startID The lowest _id value to be used
     * @author hightowe
     */
    public static void writeJsonStringsToFile(List<String> jsonStrings, 
            String fileName, QueryObject obj, int startID) {
        
        File file = new File(fileName);
        
        int numberOfJsonObjects = jsonStrings.size();
        List<String> jsonBulkLoadList = new ArrayList<>();
        String indexName = obj.getIndexName();
        String typeName = obj.getTypeName();

        for (int i = 0; i < numberOfJsonObjects; i++) {
            // This map will build the index map portion for the bulk load
            Map<String, Object> indexMap = new HashMap<>();
            indexMap.put("_index", indexName);
            indexMap.put("_type", typeName);
            indexMap.put("_id", i + startID);

            String mapData = convertMapToJson(indexMap);

            indexMap = new HashMap<>();
            indexMap.put("index", mapData);

            String esIndex = convertMapToJson(indexMap);

            jsonBulkLoadList.add(esIndex + "\n");
            jsonBulkLoadList.add(jsonStrings.get(i) + "\n");

        }
        
        for (String bulkLoadData : jsonBulkLoadList) {
            try {
                FileUtils.writeStringToFile(file, bulkLoadData, true);
            } catch (IOException ex) {
                logger.error("Could not write " + bulkLoadData + " to file " + file.getAbsoluteFile(), ex);
            }
        }
    }
    
    /**
     * Replaces old data on an Elasticsearch server with given data
     * Can handle arbitrarily large data sets
     * @param documents The data to be uploaded
     * @param queryObject QueryObject loaded with data to direct upload
     * @param uploadInterval The number of documents to upload at a time
     * @return The number of documents uploaded
     */
    public static int bulkUpdateDocuments(FindIterable<Document> documents,
            QueryObject queryObject, int uploadInterval) {
        assert documents != null: PARAMETER_ERROR;
        assert queryObject != null: PARAMETER_ERROR;
        assert uploadInterval > 0: PARAMETER_ERROR;
        
        BulkLoader bl = getBulkLoaderFromQueryObject(queryObject);
        int oldID = bl.getLastID(); // store the previous number of documents
        bl.setLastID(0);    // reset lastID so that old data will be overwritten
        int newID = bulkIndexDocuments(documents, bl, uploadInterval);
        
        // if there's still old data on the server, delete it
        if (newID < oldID) {
            try {
                bl.bulkDelete(newID, oldID);
            }
            catch (ConfigurationException ex) {
                logger.error(ex);
            }
        }
        
        return newID;
    }
    
    /**
     * Indexes every document within a FindIterable object
     * Useful for uploading MongoDB data
     * @param documents The FindIterable containing all documents to be indexed
     * @param queryObject Determines where to index the data
     * @param uploadInterval Determines how frequently to clear local memory
     * @return The number of documents indexed
     * @author hightowe
     */
    public static int bulkIndexDocuments(FindIterable<Document> documents, 
            QueryObject queryObject, int uploadInterval) {
        assert documents != null: PARAMETER_ERROR;
        assert uploadInterval > 0: PARAMETER_ERROR;
        assert queryObject != null: PARAMETER_ERROR;
        
        BulkLoader bl = getBulkLoaderFromQueryObject(queryObject);
        
        return bulkIndexDocuments(documents, bl, uploadInterval);
    }
    
    /**
     * Indexes every document within a FindIterable object
     * Useful for uploading MongoDB data
     * @param documents The FindIterable containing all documents to be indexed
     * @param bl Determines where to index the data
     * @param uploadInterval Determines how frequently to clear local memory
     * @return The number of documents indexed
     * @author hightowe
     */
    public static int bulkIndexDocuments(FindIterable<Document> documents, 
            BulkLoader bl, int uploadInterval) {
        
        assert documents != null: PARAMETER_ERROR;
        assert uploadInterval > 0: PARAMETER_ERROR;
        assert bl != null && bl.isConfigured(): PARAMETER_ERROR;
        
        int count = 0;
        Set<String> keyset = null;
        List<Map> docsList = new ArrayList<>();

        for (Document doc : documents) {
            if (count == 0) {
                keyset = doc.keySet();
            }

            Map<String,Object> currMap = new HashMap<>();
            for (String key : keyset) {
                // need to swap out _id field with an alternative, or else 
                // bulk load will fail
                if (key.equals("_id")) {
                    currMap.put(MONGO_ID, doc.get(key));
                }
                else {
                    currMap.put(key,doc.get(key));
                }
            }
            
            // append a timestamp of when this document was created
            currMap.put(TIME_STAMP, getISOTime(TIME_STAMP_FORMAT));
            
            docsList.add(currMap);
            count++;
            if (count % uploadInterval == 0) {
                bl.bulkIndex(docsList);
                logger.info("Indexed " + count + " documents " + getISOTime(TIME_STAMP_FORMAT));
                docsList.clear();    // this line should prevent heap space errors
            }
        }
        
        if (docsList.size() > 0) {
            bl.bulkIndex(docsList);
            logger.info("Indexed " + count + " documents " + getISOTime(TIME_STAMP_FORMAT));
        }
        
        logger.info("Total documents indexed: " + count + ", " + getISOTime(TIME_STAMP_FORMAT));
        
        return count;
    }
    
    /**
     * Creates and loads a BulkLoader object with the contents of a QueryObject
     * @param queryObject The QueryObject to be referenced
     * @return A configured BulkLoader
     * @author hightowe
     */
    public static BulkLoader getBulkLoaderFromQueryObject(QueryObject queryObject) {
        BulkLoader bl = new BulkLoader();
        
        // get relevant parameters from queryObject
        String indexName = queryObject.getIndexName();
        String typeName = queryObject.getTypeName();
        String clusterName = queryObject.getClusterName();
        String nodeName = queryObject.getNodeName();
        String serverName = queryObject.getServerName();

        bl.config(indexName,typeName,clusterName,nodeName,serverName);
        
        return bl;
    }
    
    /**
     * Replaces old data on an Elasticsearch server with given data
     * Can handle arbitrarily large data sets
     * @param resultSet The data to be uploaded
     * @param queryObject QueryObject loaded with data to direct upload
     * @param uploadInterval The number of documents to upload at a time
     * @return The number of documents uploaded
     */
    public static int bulkUpdateResultSet(ResultSet resultSet,
            QueryObject queryObject, int uploadInterval) {
        assert resultSet != null : PARAMETER_ERROR;
        assert uploadInterval > 0 : PARAMETER_ERROR;
        assert queryObject != null : PARAMETER_ERROR;
        
        BulkLoader bl = getBulkLoaderFromQueryObject(queryObject);
        int oldID = bl.getLastID(); // store the previous number of documents
        bl.setLastID(0);    // reset lastID so that old data will be overwritten
        int newID = bulkIndexResultSet(resultSet, queryObject, uploadInterval);
        
        if (newID < oldID) {
            try {
                bl.bulkDelete(newID, oldID);
            }
            catch (ConfigurationException ex) {
                logger.error(ex);
            }
        }
        
        return newID;
    }
    
    /**
     * Converts contents of CSV file to JSON documents, and indexes the results
     * @param queryFile The name of the CSV file to index. Must be in the application root directory
     * @param bl Determines where to index the data
     * @param uploadInterval Determines how frequently to clear local memory
     * @return The number of documents indexed
     * @throws ConfigurationException 
     * @author hightowe
     */
    public static int bulkIndexCsv(String queryFile, BulkLoader bl, 
            int uploadInterval) throws ConfigurationException {
        
        List<Map> csvRows = new ArrayList<>();
        CSVReader csvreader;
        try {
            csvreader = new CSVReader(new FileReader(queryFile));
        }
        catch (IOException ex) {
            // can't read this file
            logger.error(ex);
            throw new ConfigurationException("Failed to read file " + queryFile);
        }

        assert csvreader != null;
        
        int count = 0;  // tracks number of documents
        
        try {
            // first row contains all the column names
            String[] headers = csvreader.readNext();

            while (true) {
                // each subsequent row contains data
                String[] currRow = csvreader.readNext();

                if (currRow == null) {
                    bl.bulkIndex(csvRows);
                    break;
                }

                Map<String,Object> objectMap = new HashMap<>();
                
                // pair each data item with its column
                // only read as many data items as we expect (size of headers)
                // any extra items will be ignored
                // if the current row is too short, skips to the next row
                for (int i = 0; i < headers.length; i++) {
                    try {
                        objectMap.put(headers[i],currRow[i]);
                    } catch (NullPointerException npe) {
                        logger.error(npe);
                    }
                }
                
                // append a timestamp of when this document was created
                objectMap.put(TIME_STAMP, getISOTime(TIME_STAMP_FORMAT));

                csvRows.add(objectMap);
                count++;

                if (count % uploadInterval == 0) {   // index in chunks
                    bl.bulkIndex(csvRows);
                    logger.info("Indexed " + count + " documents " + getISOTime(TIME_STAMP_FORMAT));
                    csvRows.clear();    // clear to avoid heap over-use
                }
            }
        }
        catch (IOException ex) {
            logger.info(ex);
            // reached end of file
            bl.bulkIndex(csvRows);
            logger.info("Indexed " + count + " documents " + getISOTime(TIME_STAMP_FORMAT));
        }
        
        try {
            csvreader.close();
        }
        catch (IOException ex) {
            logger.error(ex);
        }
        
        return count;
    }
    
    /**
     * Returns a String representing the current moment
     * Formatted based on TIME_STAMP_FORMAT
     * Code taken from Carlos Heuberger's top answer here: 
     * http://stackoverflow.com/questions/3914404/how-to-get-current-moment-in-iso-8601-format
     * @param timeStampFormat String representation of the desired date format
     * @return Current time as String
     * @author hightowe
     */
    public static String getISOTime(String timeStampFormat) {
//        DateFormat df = new SimpleDateFormat(timeStampFormat);
//        String nowAsISO = df.format(new Date());
//        return nowAsISO;
        DateTime dt = new DateTime();
        return dt.toString();
    }
    
    /**
     * Converts contents of CSV file to JSON documents, and indexes the results
     * @param queryFile The name of the CSV file to index. Must be in the application root directory
     * @param queryObject Determines where to index the data
     * @param uploadInterval Determines how frequently to clear local memory
     * @return The number of documents indexed
     * @throws ConfigurationException 
     * @author hightowe
     */
    public static int bulkIndexCsv(String queryFile, QueryObject queryObject,
            int uploadInterval) throws ConfigurationException {
        
        BulkLoader bl = getBulkLoaderFromQueryObject(queryObject);
        
        int count = -1;
        
        try {
            count = bulkIndexCsv(queryFile, bl, uploadInterval);
        }
        catch (ConfigurationException ex) {
            throw ex;
        }
        
        assert count >= 0;
        
        return count;
    }
    
    /**
     * Replaces old data on an Elasticsearch server with given data
     * Can handle arbitrarily large data sets
     * @param queryFile The name of the CSV file to convert and upload
     * @param queryObject QueryObject loaded with data to direct upload
     * @param uploadInterval The number of documents to upload at a time
     * @return The number of documents uploaded
     * @author hightowe
     * @throws ConfigurationException
     */
    public static int bulkUpdateCsv(String queryFile, QueryObject queryObject,
            int uploadInterval) throws ConfigurationException {
        BulkLoader bl = getBulkLoaderFromQueryObject(queryObject);
        
        int prevLastID = bl.getLastID();
        
        boolean updateFlag = queryObject.getUpdateFlag();
        
        if (updateFlag) {
            bl.setLastID(0);
        }
        
        int newLastID = -1; // will eventually contain the number of documents indexed
        
        try {
            newLastID = bulkIndexCsv(queryFile, bl, uploadInterval);
        }
        catch (ConfigurationException ex) {
            throw ex;
        }
        
        assert newLastID > 0;
        
        // delete excess data from destination if updateFlag is set
        if (newLastID < prevLastID && updateFlag) {
            try {
                bl.bulkDelete(newLastID,prevLastID);
            }
            catch (ConfigurationException ex) {
                logger.error(ex);
            }
        }
        
        return newLastID;
    }
    
    /**
     * Indexes every document within a ResultSet object
     * @param resultSet The ResultSet containing all documents to be indexed
     * @param queryObject Determines where to index the data
     * @param uploadInterval Determines how frequently to clear local memory
     * @return The number of documents indexed
     * @author hightowe
     */
    public static int bulkIndexResultSet(ResultSet resultSet, 
            QueryObject queryObject, int uploadInterval) {
        assert resultSet != null : PARAMETER_ERROR;
        assert uploadInterval > 0 : PARAMETER_ERROR;
        assert queryObject != null : PARAMETER_ERROR;
        
        BulkLoader bl = getBulkLoaderFromQueryObject(queryObject);
        
        return bulkIndexResultSet(resultSet, bl, uploadInterval);
    }
    
    /**
     * Indexes every document within a ResultSet object
     * @param resultSet The ResultSet containing all documents to be indexed
     * @param bl Determines where to index the data
     * @param uploadInterval Determines how frequently to clear local memory
     * @return The number of documents indexed
     * @author hightowe
     */
    public static int bulkIndexResultSet(ResultSet resultSet, 
            BulkLoader bl, int uploadInterval) {
        assert resultSet != null : PARAMETER_ERROR;
        assert uploadInterval > 0 : PARAMETER_ERROR;
        assert bl != null && bl.isConfigured(): PARAMETER_ERROR;
        
        int count = 0;
        try {
            ResultSetMetaData rsMetaData = resultSet.getMetaData();
            int columnNumbers = rsMetaData.getColumnCount();
            List<Map> docsList = new ArrayList<>();
            
            while (resultSet.next()) {
                Map<String, Object> dataMap = new HashMap<>();
                for (int i = 1; i <= columnNumbers; i++) {
                    dataMap.put(rsMetaData.getColumnLabel(i), resultSet.getString(i));
                }
                
                // append a timestamp of when this document was created
                dataMap.put(TIME_STAMP, getISOTime(TIME_STAMP_FORMAT));

                docsList.add(dataMap);
                count++;

                if (count % uploadInterval == 0) {
                    bl.bulkIndex(docsList);
                    logger.info("Indexed " + count + " documents " + getISOTime(TIME_STAMP_FORMAT));
                    docsList.clear();
                }
            }
            
            if (docsList.size() > 0) {
                bl.bulkIndex(docsList);
                logger.info("Indexed " + count + " documents " + getISOTime(TIME_STAMP_FORMAT));
            }
        }
        catch (SQLException ex) {
            logger.error(ex);
        }
        
        logger.info("Total documents indexed: " + count + ", " + getISOTime(TIME_STAMP_FORMAT));
        
        return count;
    }
    
    /**
     * Exports a given CSV file as a JSON file, prepared for bulk indexing to Elasticsearch
     * If you experience heap space errors, try reducing the writeInterval
     * @param dataFile The name of the original CSV file
     * @param newFile The name to be used for the output file, including extension (.json)
     * @param queryObject A configured QueryObject
     * @param writeInterval The maximum number of documents to be stored at once
     * @author hightowe
     */
    public static void writeCsvToJson(String dataFile, String newFile, 
            QueryObject queryObject, int writeInterval) {
        
        String index = queryObject.getIndexName();
        String type = queryObject.getTypeName();
        
        CSVReader csvreader = null;
        
        try {
            csvreader = new CSVReader(new FileReader(dataFile));
        }
        catch (FileNotFoundException fnfe) {
            logger.fatal(fnfe);
        }
        
        assert csvreader != null;
        
        List<String> jsonData = new ArrayList<>();
        
        // An exception may be thrown if loop passes end of file
        try {
            int count = 0;  // tracks number of documents written to file
            String[] headers = csvreader.readNext(); // first row has field names

            while (true) {
                String[] currRow = csvreader.readNext();

                // currRow is null if we've reached the end of the file
                if (currRow == null) {
                    // bulk load any remaining data
                    if (jsonData.size() > 0) {
                        writeToElasticsearchBulkLoadJsonFile(index, type, 
                                newFile, jsonData);
                    }
                    break;
                }

                // map to store the current document
                Map<String,Object> objectMap = new HashMap<>();

                /*  for each header value, get the corresponding data, put it in objectMap.
                    if a row has fewer columns than there are headers, will log
                        the error and continue on
                */
                for (int i = 0; i < headers.length; i++) {
                    try {
                        objectMap.put(headers[i],currRow[i]);
                    } catch (NullPointerException npe) {
                        logger.error(npe);
                        logger.error("Missing column at row " + Integer.toString(count)
                                + ", column " + Integer.toString(i));
                    }
                }
                
                objectMap.put(TIME_STAMP, getISOTime(TIME_STAMP_FORMAT));
                
                // convert objectMap to JSON string and store it temporarily
                String objectString = convertMapToJson(objectMap);
                jsonData.add(objectString);

                count++;

                // if this is the (writeInterval)th document, perform bulk load
                // and clear jsonData to prevent heap space over-allocation
                if (count % writeInterval == 0) {
                    writeToElasticsearchBulkLoadJsonFile(index, type, 
                                newFile, jsonData);
                    jsonData.clear();
                }
            }
        }
        catch (IOException ex) {
            // reached end of file or encountered error, write any remaining data to file
            logger.error(ex);
            if (jsonData.size() > 0) {
                writeToElasticsearchBulkLoadJsonFile(index, type, 
                                newFile, jsonData);
            }
        }
        
        try {
            csvreader.close();
        }
        catch (IOException ex) {
            logger.error(ex);
        }
    }
    
    /**
     * Method will provide a .json file for a specific Elasticsearch index and type so that it can be loaded
     *  via Elasticsearch Bulkload
     * If there already exists a file with name = jsonFileName, this will append to that file
     * @param indexName - name of the Elasticsearch index this .json bulk load is being built for
     * @param typeName - name of the Elasticsearch type this .json bulk load is being built for
     * @param jsonFileName - name of the .json file that will be created
     * @param jsonObjectData - The JSON object data that will be written to .json file.
     */
    public static void writeToElasticsearchBulkLoadJsonFile(String indexName, 
            String typeName, String jsonFileName, List<String> jsonObjectData) {

        int numberOfJsonObjects = jsonObjectData.size();
        List<String> jsonBulkLoadList = new ArrayList<>();

        for (int i = 0; i < numberOfJsonObjects; i++) {
            // This map will build the index map portion for the bulk load
            Map<String, Object> indexMap = new HashMap<>();
            indexMap.put("_index", indexName);
            indexMap.put("_type", typeName);
            indexMap.put("_id", i);

            String mapData = ToolKit.convertMapToJson(indexMap);

            // wrap the document in an index command
            indexMap = new HashMap<>();
            indexMap.put("index", mapData);

            String esIndex = ToolKit.convertMapToJson(indexMap);

            jsonBulkLoadList.add(esIndex + "\n");
            jsonBulkLoadList.add(jsonObjectData.get(i) + "\n");
        }

        File file = new File(jsonFileName);

        for (String bulkLoadData : jsonBulkLoadList) {
            try {
                FileUtils.writeStringToFile(file, bulkLoadData, true);
            } catch (IOException ex) {
                logger.error("Could not write " + bulkLoadData + " to file " + file.getAbsoluteFile(), ex);
            }
        }

    }
    
    /**
     * Converts a Mongo result set to JSON and stores the converted data in a 
     * file, prepared for bulk loading into ElasticSearch
     * WARNING: does NOT overwrite the old file
     * @param results The Mongo result set
     * @param queryObject Must contain the index and type of the target
     * @param jsonFileName The name of the file to write converted data to
     * @param writeInterval Determines how frequently to clear local memory
     * @author hightowe
     */
    public static void writeMongoResultsToJson(FindIterable<Document> results,
            QueryObject queryObject, String jsonFileName, int writeInterval) {
        List<String> jsonResults = new ArrayList<>();
        int fcount = 0;
        String theIndex = queryObject.getIndexName();
        String theType = queryObject.getTypeName();

        for (Document doc : results) {
            // add a timestamp of when the document was written
            doc.append(TIME_STAMP, getISOTime(TIME_STAMP_FORMAT));
            String jsonDoc = doc.toJson();
            // replace _id field name with MONGO_ID to avoid errors when indexing
            jsonDoc = jsonDoc.replace("_id", MONGO_ID);
            jsonResults.add(jsonDoc);
            fcount++;

            if (fcount % writeInterval == 0) {
                logger.info("Writing JSON document " + Integer.toString(fcount));
                ToolKit.writeToElasticsearchBulkLoadJsonFile(theIndex, theType, 
                    jsonFileName, jsonResults);
                jsonResults.clear();    // clear local memory
            }
        }

        logger.info("Writing JSON document " + Integer.toString(fcount));
        ToolKit.writeToElasticsearchBulkLoadJsonFile(theIndex, theType, 
            jsonFileName, jsonResults);

        logger.info("Wrote " + Integer.toString(fcount) + " JSON documents");
                
    }
    
    /**
     * Given a full filename of the original file (including extension),
     * returns a String representation of the new filename by appending
     * appendNum to the original filename sans extension, and adding the .json
     * extension.
     * Example: getNewJsonFileName("example.sql", 23) returns "example23.json"
     * @param fileName The old file name, including extension
     * @param appendNum The number to append
     * @return String containing new JSON file name
     * @author hightowe
     */
    public static String getNewJsonFileName(String fileName, int appendNum) {
        String newName;
        int extIndex = fileName.lastIndexOf('.');
        newName = fileName.substring(0, extIndex) + 
                Integer.toString(appendNum) + ".json";
        return newName;
    }
}
