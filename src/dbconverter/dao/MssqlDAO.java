/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dbconverter.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;

/**
 * Class that contains methods that connect and insert data into the ibt mssql
 * data base
 *
 * @author hightowe
 */
public class MssqlDAO {
    private static final String MSSQL_JDBC_DRIVER = "net.sourceforge.jtds.jdbc.Driver";
    private final static org.apache.logging.log4j.Logger logger = LogManager.getLogger(MssqlDAO.class.getName());

    /**
     * Connect to MSSQL DB Code is based on example from
     * http://www.java-tips.org/other-api-tips/jdbc/how-to-connect-microsoft-sql-server-using-jdbc-3.html
     *
     * @param dbConnString
     * @param dbUser
     * @param dbPass
     * @return configured Connection object
     * @author hightowe
     */
    public Connection getConnection(String dbConnString, String dbUser, String dbPass) {
        try {
            Class.forName(MSSQL_JDBC_DRIVER);
        } catch (ClassNotFoundException ex) {
            logger.fatal("Encountered a class not found exception for " + MSSQL_JDBC_DRIVER, ex);
        }

        Connection conn = null;
        try {
            conn = DriverManager.getConnection(
                    dbConnString, dbUser, dbPass);
        } catch (SQLException ex) {
            logger.fatal("Could not establish a connection to the DB " + dbConnString, ex);
        }

        return conn;

    }

    /**
     * Helper method to disconnect from a data base
     *
     * @param conn The connection to close
     * @author hightowe
     */
    public void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ex) {
                logger.fatal("Could not disconnect from the DB", ex);
            }
        }
        else {
            logger.fatal("Connection is null, cannot disconnect");
        }
    }
    
    /**
     * Runs a query on the database
     * @param conn configured Connection object
     * @param sql a SQL query in String form
     * @return the query results
     * @author hightowe
     */
    public ResultSet getResultSet(Connection conn, String sql) {
        assert conn != null : "Connection cannot be null";
        assert sql != null : "sql statement cannot be null";

        ResultSet resultSet = null;
        Statement statement;

        try {
            statement = conn.createStatement();
            resultSet = statement.executeQuery(sql);
        } catch (Exception e) {
            logger.error(e);
        }

        return resultSet;
    }
    
    /**
     * Method will provide a List of Maps that contain the Result Set data. This
     * can be used to create a JSON Object
     *
     * @param resultSet ResultSet to convert
     * @return the result data
     */
    public List<Map> getResultSetMap(ResultSet resultSet) {

        assert resultSet != null : "ResultSet cannot be null!";

        List<Map> resultsList = new ArrayList<>();

        try {
            ResultSetMetaData rsMetaData = resultSet.getMetaData();
            int columnNumbers = rsMetaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> dataMap = new HashMap<>();
                for (int i = 1; i <= columnNumbers; i++) {
                    dataMap.put(rsMetaData.getColumnLabel(i), resultSet.getString(i));
                }

                // Add the data to List of Maps
                resultsList.add(dataMap);
            }

        } catch (SQLException e) {
            logger.error(e);
        }

        return resultsList;
    }
}
