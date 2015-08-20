package dbconverter.data;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import dbconverter.dao.MongoDAO;
import dbconverter.dao.MssqlDAO;
import dbconverter.dao.OracleDAO;
import dbconverter.dao.util.ConfigCommons;
import dbconverter.dao.util.ConfigurationException;
import dbconverter.dao.util.QueryObject;
import dbconverter.dao.util.ToolKit;
import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
/**
 * This class is a proof of concept which demonstrates how to read data from 
 * Oracle, MSSQL, MongoDB, and CSV into Java, then export into a JSON file,
 * or upload directly to Elasticsearch.
 * @author sanchez
 * @author hightowe
 */
public class Elastic {

    private static final String ORACLE_DB = "oracle";
    private static final String MSSQL_DB = "mssql";
    private static final String MONGO_DB = "mongo";
    private static final String CSV_DB = "csv";
    
    private static String configFile;
    private static final Logger logger = LogManager.getLogger(Elastic.class.getName());
    
    // determines the number of documents to upload at a time
    // if the program runs out of memory, try decreasing its value
    private static final int UPLOAD_INTERVAL = 10000;

    /**
     * Reads a properties file, given as a command-line argument, and performs
     *  whatever actions the file specifies
     * @param args Command-line arguments. Expects a properties filename.
     * @author sanchez
     * @author hightowe
     */
    public static void main(String[] args) {
        // get command-line arguments
        try {
            configFile = args[0];
        }
        catch (Exception e) {
            logger.fatal("Must provide properties file name, " + e);
            return;
        }
        
        assert configFile != null;
        
        ConfigCommons config = new ConfigCommons();
        config.loadConfig(configFile);
        
        String dbType = config.getDbType();
        
        switch (dbType.toLowerCase()) {
            case ORACLE_DB:
                logger.info("Database type is " + ORACLE_DB);
                logger.info("Attempting to generate data based on the config file " + configFile + "...");
                parseOracle(config);
                break;
                
            case MSSQL_DB:
                logger.info("Database type is " + MSSQL_DB);
                logger.info("Attempting to generate data based on the config file " + configFile + "...");
                parseMssql(config);
                break;
            
            case CSV_DB:
                logger.info("Database type is " + CSV_DB);
                logger.info("Attempting to generate data based on the config file " + configFile + "...");
                parseCsv(config);
                break;
            
            case MONGO_DB:
                logger.info("Database type is " + MONGO_DB);
                logger.info("Attempting to generate data based on the config file " + configFile + "...");
                parseMongo(config);
                break;

            default:
                String errorMsg = dbType + " is not recognized";
                logger.error(errorMsg);
                logger.error("The dbtype property in the config file is unknown");
                logger.error("These are the only valid values for the dbtype property");
                logger.error(ORACLE_DB);
                logger.error(MSSQL_DB);
                logger.error(MONGO_DB);
                logger.error(CSV_DB);
                throw new AssertionError(errorMsg);
        }
    }
    
    /**
     * Loads CSV data into Elasticsearch and/or writes CSV data to JSON file
     * @param config ConfigCommons object loaded with a properties file
     * @author hightowe
     */
    private static void parseCsv(ConfigCommons config) {
        int fileCount = 0;  // used to give unique names to output files
        
        // loop through all query instructions
        for (QueryObject queryObject : config.getQueryObjects()) {
            boolean writeFlag = queryObject.getWriteFlag();
            boolean updateFlag = queryObject.getUpdateFlag();
            boolean indexFlag = queryObject.getIndexFlag();
            
            String queryFile = queryObject.getQueryFile();
            
            if (writeFlag) {
                // get the filename without the index
                int extIndex = queryFile.lastIndexOf('.');
                String newFile = queryFile.substring(0,extIndex) + 
                        Integer.toString(fileCount) + ".json";
                
                // writing to a new file, so increment fileCount
                fileCount++;
                
                File file = new File(newFile);
                // if file exists, delete it
                FileUtils.deleteQuietly(file);
                
                // write the CSV file contents to the new JSON file
                ToolKit.writeCsvToJson(queryFile, newFile, queryObject, 
                        UPLOAD_INTERVAL);
            }
            
            // will execute at most one operation: update or index
            // if both flags are set, will only execute update
            if (updateFlag || indexFlag) {
                try {
                    if (updateFlag) {
                        ToolKit.bulkUpdateCsv(queryFile, queryObject,
                                UPLOAD_INTERVAL);
                    }
                    else {
                        ToolKit.bulkIndexCsv(queryFile, queryObject,
                                UPLOAD_INTERVAL);
                    }
                }
                catch (ConfigurationException ex) {
                    logger.error(ex);
                }
            }
        }
    }
    
    /**
     * Loads MSSQL data into Elasticsearch
     * Can handle arbitrarily large quantities of documents
     * @param config ConfigCommons object loaded with the desired properties file
     * @author hightowe
     */
    private static void parseMssql(ConfigCommons config) {
        MssqlDAO mssqlDAO = new MssqlDAO();
        
        String jdbc = config.getJdbcParametersMssql();
        String user = config.getMssqlUser();
        String pass = config.getMssqlPass();

        Connection conn = mssqlDAO.getConnection(jdbc, user, pass);
        
        assert conn != null: "connection failure";

        int fileCount = 0;  // used to give all files unique names
        
        // loop through all query instructions
        for (QueryObject queryObject : config.getQueryObjects()) {
            boolean writeFlag = queryObject.getWriteFlag();
            boolean updateFlag = queryObject.getUpdateFlag();
            boolean indexFlag = queryObject.getIndexFlag();
            String query = config.readQueryFile(queryObject.getQueryFile());
            
            if (writeFlag) {
                String queryFile = queryObject.getQueryFile();
                int extIndex = queryFile.lastIndexOf('.');
                String newFileName = queryFile.substring(0, extIndex) + 
                        Integer.toString(fileCount) + ".json";
                ResultSet resultSet = mssqlDAO.getResultSet(conn, query);
                
                File file = new File(newFileName);
                // if file exists, delete it
                FileUtils.deleteQuietly(file);
                
                ToolKit.writeResultSetToJson(resultSet, queryObject, 
                        UPLOAD_INTERVAL, newFileName);
                
                fileCount++;
            }
            
            // will execute at most one operation: update or index
            // if both flags are set, will only execute update
            if (updateFlag || indexFlag) {
                ResultSet resultSet = mssqlDAO.getResultSet(conn, query);
                
                if (updateFlag) {
                    ToolKit.bulkUpdateResultSet(resultSet, queryObject, 
                            UPLOAD_INTERVAL);
                }
                else {
                    ToolKit.bulkIndexResultSet(resultSet, queryObject, 
                            UPLOAD_INTERVAL);
                }
            }
        }
        
        mssqlDAO.closeConnection(conn);
    }
    
    /**
     * Loads MongoDB data into Elasticsearch
     * Will eventually support writing MongoDB data to a JSON file
     * Can handle arbitrarily large quantities of documents
     * @param config ConfigCommons object loaded with the desired properties file
     * @author hightowe
     */
    private static void parseMongo(ConfigCommons config) {
        MongoDAO mongoDAO = new MongoDAO();
        
        MongoClient client;
        try {
            client = mongoDAO.getConnection(config.getMongoDbURI());
        }
        catch (Exception ex) {
            logger.fatal(ex);
            return;
        }
        
        String mongoDbName = config.getMongoDbName();
        String mongoColName = config.getMongoDbCollectionName();
        
        int fileCount = 0;  // used to give unique names to all files
        
        // loop through all query instructions
        for (QueryObject queryObject: config.getQueryObjects()) {
            String mongoQuery = config.readQueryFile(queryObject.getQueryFile());
            MongoCollection<Document> collection = mongoDAO.getCollection(client, mongoDbName, mongoColName);
            FindIterable<Document> results = mongoDAO.getDocuments(mongoQuery,collection);
            
            boolean writeFlag = queryObject.getWriteFlag();
            boolean updateFlag = queryObject.getUpdateFlag();
            boolean indexFlag = queryObject.getIndexFlag();
            
            if (writeFlag) {
                String queryFile = queryObject.getQueryFile();
                String outputFileName = queryFile.substring(0, queryFile.length()-5)+fileCount+".json";
                
                File file = new File(outputFileName);
                // if file exists, delete it
                FileUtils.deleteQuietly(file);
                
                ToolKit.writeMongoResultsToJson(results, queryObject, 
                        outputFileName, UPLOAD_INTERVAL);
                
                fileCount++;
            }
            
            // will execute at most one of the following: update, index
            // if both flags are set, defaults to update
            if (updateFlag || indexFlag) {
                if (updateFlag) {
                    ToolKit.bulkUpdateDocuments(results, queryObject, UPLOAD_INTERVAL);
                }
                else {
                    ToolKit.bulkIndexDocuments(results, queryObject, UPLOAD_INTERVAL);
                }
            }
        }
        client.close();
    }
    
    
    
    /**
     * Loads OracleDB data into Elasticsearch
     * Can handle arbitrarily large quantities of documents
     * @param config ConfigCommons object loaded with the desired properties file
     * @author hightowe
     */
    private static void parseOracle(ConfigCommons config) {
        OracleDAO oracleDAO = new OracleDAO();
        Connection conn = oracleDAO.getConnection(config.getJdbcParametersOracle());
        
        int fileCount = 0;  // used to give files unique names
        
        // loop through query instructions
        for (QueryObject queryObject : config.getQueryObjects()) {
            boolean writeFlag = queryObject.getWriteFlag();
            boolean updateFlag = queryObject.getUpdateFlag();
            boolean indexFlag = queryObject.getIndexFlag();
            
            String query = config.readQueryFile(queryObject.getQueryFile());
            
            if (writeFlag) {
                ResultSet resultSet = oracleDAO.getResultSet(conn,query);
                String queryFile = queryObject.getQueryFile();
                int extIndex = queryFile.lastIndexOf('.');
                String newfile = queryFile.substring(0,extIndex) + 
                        Integer.toString(fileCount) + ".json";
                
                File file = new File(newfile);
                // if file exists, delete it
                FileUtils.deleteQuietly(file);
                
                ToolKit.writeResultSetToJson(resultSet, queryObject, UPLOAD_INTERVAL, newfile);
            
                fileCount++;
            }
            
            // will execute at most one operation: update or index
            // if both flags are set, will only execute update
            if (updateFlag || indexFlag) {
                ResultSet resultSet = oracleDAO.getResultSet(conn,query);
                
                if (updateFlag) {
                    ToolKit.bulkUpdateResultSet(resultSet, queryObject, 
                            UPLOAD_INTERVAL);
                }
                else {
                    ToolKit.bulkIndexResultSet(resultSet, queryObject, 
                            UPLOAD_INTERVAL);
                }
            }
        }
    }
}