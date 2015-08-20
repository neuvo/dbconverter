/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbconverter.dao.util;

/**
 * Class for storing data necessary for uploads to Elasticsearch
 * @author hightowe
 */
public class QueryObject {
    private String queryFile;
    private String indexName;
    private String typeName;
    private String clusterName;
    private String nodeName;
    private String serverName;
    private boolean writeFlag;
    private boolean updateFlag;
    private boolean indexFlag;
    
    /**
     * Empty constructor. Use setter methods to configure
     * @author hightowe
     */
    public QueryObject() {
        // leave all instance variables empty
    }
    
    /**
     * Constructor for the essential fields
     * @param queryFile
     * @param indexName
     * @param typeName 
     * @author hightowe
     */
    public QueryObject(String queryFile, String indexName, String typeName) {
        this.queryFile = queryFile;
        this.indexName = indexName;
        this.typeName = typeName;
    }
    
    /**
     * Constructor for all fields
     * @param queryFile
     * @param indexName
     * @param typeName
     * @param clusterName
     * @param nodeName
     * @param serverName
     * @param writeFlag
     * @param updateFlag
     * @param indexFlag 
     * @author hightowe
     */
    public QueryObject(String queryFile, String indexName, String typeName,
            String clusterName, String nodeName, String serverName,
            boolean writeFlag, boolean updateFlag, boolean indexFlag) {
        this.queryFile = queryFile;
        this.indexName = indexName;
        this.typeName = typeName;
        this.clusterName = clusterName;
        this.nodeName = nodeName;
        this.serverName = serverName;
        this.writeFlag = writeFlag;
        this.updateFlag = updateFlag;
        this.indexFlag = indexFlag;
    }
    
    public void setQueryFile(String queryFile) {
        this.queryFile = queryFile;
    }
    
    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }
    
    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }
    
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }
    
    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }
    
    public void setServerName(String serverName) {
        this.serverName = serverName;
    }
    
    public String getQueryFile() {
        return queryFile;
    }
    
    public String getIndexName() {
        return indexName;
    }
    
    public String getTypeName() {
        return typeName;
    }
    
    public String getClusterName() {
        return clusterName;
    }
    
    public String getNodeName() {
        return nodeName;
    }
    
    public String getServerName() {
        return serverName;
    }
    
    public boolean getWriteFlag() {
        return writeFlag;
    }
    
    public boolean getUpdateFlag() {
        return updateFlag;
    }
    
    public boolean getIndexFlag() {
        return indexFlag;
    }
}
