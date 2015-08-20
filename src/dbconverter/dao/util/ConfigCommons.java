package dbconverter.dao.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class responsible for reading properties files to work with Elastic
 * @author sanchez
 * @author hightowe
 */
public class ConfigCommons {

    private static final Logger logger = LogManager.getLogger(ConfigCommons.class.getName());

    // Config file properties
    private final String USER = "user";
    private final String PASS = "pass";
    private final String HOST = "host";
    private final String SID = "sid";
    private final String PORT = "port";
    private final String JDBC = "jdbc";
    private final String JTDS = "jtds";
    private final String DBTYPE = "dbtype";
    private final String ENV = "env";
    private final String ORACLE_DB = "oracle";
    private final String MONGO_DB = "mongo";
    private final String MONGO_DB_URI = "mongodburi";
    private final String MONGO_DB_NAME = "mongodbname";
    private final String MONGO_DB_COLLECTION = "mongodbcollection";
    private final String QUERY_FILES = "query.files";
    private final String INDEX_NAMES = "index.names";
    private final String TYPE_NAMES = "type.names";
    private final String CLUSTER_NAMES = "cluster.names";
    private final String NODE_NAMES = "node.names";
    private final String SERVER_NAMES = "server.names";
    private final String WRITE_FLAG = "write.flag";
    private final String UPDATE_FLAG = "update.flag";
    private final String INDEX_FLAG = "index.flag";
    private final String MSSQL_DB_NAME = "mssqldbname";
    private final String MSSQL_DB = "mssql";
    private final String SQL_SERVER = "sqlserver";
    private final String CSV_DB = "csv";
    private final char DELIMITER = ',';

    // Helpers
    private final char COLON = ':';

    // For getters and setters
    private String environment;
    private String dbType;
    private String jdbcParamaters;
    private List<String> queryFiles;
    private List<String> jsonFiles;
    private List<String> csvFiles;

    private String mongoDbURI;
    private String mongoDbName;
    private String mongoDbCollectionName;
    
    private String mssqlJdbcParameters;
    private String mssqlUser;
    private String mssqlPass;
    
    private List<QueryObject> queryParams;

    /**
     * Loads the contents of a properties file into a ConfigCommons object
     * @param configFileName name of the file to read
     * @author sanchez
     * @author hightowe
     */
    public void loadConfig(String configFileName) {

        PropertiesConfiguration propConfig = new PropertiesConfiguration();

        try {
            propConfig.load(configFileName);

            logger.info("Attempting to load the data from the properties file " + configFileName);
            dbType = propConfig.getString(DBTYPE);

            switch (dbType.toLowerCase()) {
                case ORACLE_DB:
                    // Load config data into getters
                    setJdbcParametersOracle(propConfig);
                    
                    // Load List of queries
                    setQueryFiles(propConfig.getList(QUERY_FILES));
                    environment = propConfig.getString(ENV);
                    break;
                    
                case MONGO_DB:
                    environment = propConfig.getString(ENV);
                    mongoDbURI = propConfig.getString(MONGO_DB_URI);
                    mongoDbCollectionName = propConfig.getString(MONGO_DB_COLLECTION);
                    mongoDbName = propConfig.getString(MONGO_DB_NAME);
                    break;
                    
                case MSSQL_DB:
                    environment = propConfig.getString(ENV);
                    setJdbcParametersMssql(propConfig);
                    break;
                    
                case CSV_DB:
                    environment = propConfig.getString(ENV);
                    break;

                default:
                    String errorMsg = dbType + " is not valid value for the property type " + DBTYPE;
                    logger.error(errorMsg);
                    logger.info("Valid values for property " + DBTYPE + " are:");
                    logger.info(ORACLE_DB);
                    logger.info(MONGO_DB);
                    logger.info(MSSQL_DB);
                    logger.info(CSV_DB);
                    throw new AssertionError(errorMsg);
            }
            
            setQueryFiles(propConfig.getList(QUERY_FILES));
            
            List<Object> queryList = propConfig.getList(QUERY_FILES);
            List<Object> indexList = propConfig.getList(INDEX_NAMES);
            List<Object> typeList = propConfig.getList(TYPE_NAMES);
            List<Object> clusterList = propConfig.getList(CLUSTER_NAMES);
            List<Object> nodeList = propConfig.getList(NODE_NAMES);
            List<Object> serverList = propConfig.getList(SERVER_NAMES);
            List<Object> writeOpList = propConfig.getList(WRITE_FLAG);
            List<Object> updateOpList = propConfig.getList(UPDATE_FLAG);
            List<Object> indexOpList = propConfig.getList(INDEX_FLAG);
            try {
                setQueryObjects(queryList, indexList, typeList, clusterList, 
                    nodeList, serverList, writeOpList, updateOpList, indexOpList);
            }
            catch (Exception ex) {
                logger.error("Failed to read query instructions: " + ex);
            }

        } catch (ConfigurationException ex) {
            logger.error("Something went wrong trying to read the configuration file " + configFileName, ex);
        }
    }

    public String getJdbcParametersOracle() {
        return jdbcParamaters;
    }

    private void setJdbcParametersOracle(PropertiesConfiguration propsConfData) {

        StringBuilder jdbcSb = new StringBuilder();
        jdbcSb.append(propsConfData.getString(JDBC))
                .append(COLON)
                .append(propsConfData.getString(USER))
                .append("/")
                .append(propsConfData.getString(PASS))
                .append("@")
                .append(propsConfData.getString(HOST))
                .append(COLON)
                .append(propsConfData.getString(PORT))
                .append(COLON)
                .append(propsConfData.getString(SID));

        jdbcParamaters = jdbcSb.toString();

    }
    
    /**
     * Retrieves jdbc parameters for a Mssql database
     * @return string representation of jdbc parameters
     * @author hightowe
     */
    public String getJdbcParametersMssql() {
        return mssqlJdbcParameters;
    }
    
    /**
     * sets jdbc parameters for a mssql database
     * @param propsConfData loaded with contents of properties file
     * @author hightowe
     */
    private void setJdbcParametersMssql(PropertiesConfiguration propsConfData) {
        StringBuilder jdbcSb = new StringBuilder();
        mssqlUser = propsConfData.getString(USER);
        mssqlPass = propsConfData.getString(PASS);
        jdbcSb.append(JDBC)
                .append(COLON)
                .append(JTDS)
                .append(COLON)
                .append(SQL_SERVER)
                .append(COLON)
                .append("//")
                .append(propsConfData.getString(HOST))
                .append(COLON)
                .append(propsConfData.getString(PORT))
                .append("/")
                .append(propsConfData.getString(MSSQL_DB_NAME));
        
        mssqlJdbcParameters = jdbcSb.toString();
    }
    
    public String getMssqlUser() {
        return mssqlUser;
    }
    
    public String getMssqlPass() {
        return mssqlPass;
    }
    
    /**
     * Creates query objects pairing the n-th item in each list with each other
     * All lists must have the same size, but can contain blank indices
     * @param fileList List of file names
     * @param indexList List of index names
     * @param typeList List of type names
     * @param clusterList List of cluster names
     * @param nodeList List of node names
     * @param serverList List of server names
     * @param writeOpList List of write flag values
     * @param updateOpList List of update flag values
     * @param indexOpList List of index flag values
     * @throws Exception 
     * @author hightowe
     */
    private void setQueryObjects(List<Object> fileList, List<Object> indexList, 
            List<Object> typeList, List<Object> clusterList, 
            List<Object> nodeList, List<Object> serverList, 
            List<Object> writeOpList, List<Object> updateOpList, 
            List<Object> indexOpList) throws Exception {
        int baseSize = fileList.size();
        if (!(baseSize == typeList.size() && baseSize == clusterList.size() 
                && baseSize == nodeList.size() && baseSize == serverList.size()
                && baseSize == writeOpList.size() 
                && baseSize == updateOpList.size() 
                && baseSize == indexOpList.size())) {
            throw new Exception("Essential arguments do not have matching sizes");
        }
        
        queryParams = new ArrayList<>();
        
        for (int i = 0; i < baseSize; i++) {
            String currFile = fileList.get(i).toString();
            String currIndex = indexList.get(i).toString();
            String currType = typeList.get(i).toString();
            String currCluster = clusterList.get(i).toString();
            String currNode = nodeList.get(i).toString();
            String currServer = serverList.get(i).toString();
            boolean currWriteOp = writeOpList.get(i).toString().toLowerCase().equals("true");
            boolean currUpdateOp = updateOpList.get(i).toString().toLowerCase().equals("true");
            boolean currIndexOp = indexOpList.get(i).toString().toLowerCase().equals("true");
            
            QueryObject currobj = new QueryObject(currFile, currIndex, currType,
                                        currCluster, currNode, currServer,
                                        currWriteOp, currUpdateOp, currIndexOp);
            queryParams.add(currobj);
        }
    }
    
    public List<QueryObject> getQueryObjects() {
        return queryParams;
    }

    private void setQueryFiles(List<Object> queryFiles) {
        logger.info("Processing retrival of " + QUERY_FILES + " property");
        assert !queryFiles.isEmpty() : QUERY_FILES + " property cannot be empty.";
        this.queryFiles = new ArrayList<>();
        this.queryFiles.clear();

        for (Object obj : queryFiles) {
            this.queryFiles.add(obj.toString());
            System.out.println(obj.toString());
        }
    }

    public List<String> getQueryFiles() {
        return queryFiles;
    }

    public String getEnvironment() {
        return environment;
    }

    public String getDbType() {
        return dbType;
    }

    public String getMongoDbURI() {
        return mongoDbURI;
    }

    public String getMongoDbName() {
        return mongoDbName;
    }

    public String getMongoDbCollectionName() {
        return mongoDbCollectionName;
    }
    
    /**
     * Gets a String representation of the query file's contents
     * @param queryFileName
     * @return the query as a String
     * @author sanchez
     */
    public String readQueryFile(String queryFileName) {
        assert queryFileName != null : "queryFileName cannot be null";
        StringBuilder queryBuilder = new StringBuilder();
        String query = null;
        InputStream input = null;

        try {
            input = new FileInputStream(queryFileName);
            List<String> queryLines = new ArrayList<>();
            queryLines.clear();
            queryLines.addAll(IOUtils.readLines(input));

            if (!queryLines.isEmpty()) {
                for (String queryLine : queryLines) {
                    queryBuilder.append(queryLine).append("\n");
                }
            }

        } catch (FileNotFoundException ex) {
            logger.error(queryFileName + " could not be found", ex);
        } catch (IOException ioe) {
            logger.error("There was a problem with the file " + queryFileName, ioe);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException ex) {
                    logger.error("There was a problem with the file " + queryFileName, ex);
                }
            }
        }

        query = queryBuilder.toString();
        return query;
    }
}
