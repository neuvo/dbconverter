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
import org.apache.logging.log4j.Logger;

/**
 * Will provide a class that will allow you to connect to Oracle DBs
 * @author sanchez
 * @author hightowe
 */
public class OracleDAO {
    
    private final static Logger logger = LogManager.getLogger(OracleDAO.class.getName());

    /**
     * Makes a connection to the database specified by its input argument
     * @param jdbcParameters Used to make the connection
     * @return Configured connection object
     * @author sanchez
     */
    public Connection getConnection(String jdbcParameters) {
        assert jdbcParameters != null && !jdbcParameters.isEmpty() : "jdbcParameters cannot be null or empty";

        int hostInfoIndex = jdbcParameters.indexOf("@");
        String hostInfo = jdbcParameters.substring(hostInfoIndex);

        String connectionInfo = jdbcParameters;

        getJDBCDriver();
        Connection connection = null;

        try {
            connection = DriverManager.getConnection(connectionInfo);

        } catch (SQLException e) {
            logger.error("The connection for " + hostInfo + " failed");
            logger.error(e);
        }

        return connection;

    }

    // TODO: figure out what this does
    /**
     * 
     * @author sanchez
     */
    private void getJDBCDriver() {

        String driverName = "oracle.jdbc.driver.OracleDriver";

        try {

            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            logger.error("Could not find the driver: " + driverName);
            logger.error(e);
        }

    }

    /**
     * Executes a query against an Oracle database, returns the results
     * @param conn a connection to an Oracle database
     * @param sql the SQL query in String format
     * @return a ResultSet containing the response to the query
     * @author sanchez
     */
    public ResultSet getResultSet(Connection conn, String sql) {
        assert conn != null : "Connection cannot be null";
        assert sql != null : "sql statement cannot be null";

        ResultSet resultSet = null;
        Statement statement = null;

        try {
            statement = conn.createStatement();
            resultSet = statement.executeQuery(sql);
        } catch (Exception e) {
            logger.error("Something went wrong " + e);
        }

        return resultSet;
    }

    /**
     * Method will provide a List of Maps that contain the Result Set data. This
     * can be used to create a JSON Object
     *
     * @param conn
     * @param resultSet
     * @return
     * @author sanchez
     */
    public List<Map> getResultSetMap(Connection conn, ResultSet resultSet) {

        assert conn != null : "Connection cannot be null!";
        assert resultSet != null : "ResultSet cannot be null!";

        List<Map> resultsMap = new ArrayList<>();

        try {
            ResultSetMetaData rsMetaData = resultSet.getMetaData();
            int columnNumbers = rsMetaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> dataMap = new HashMap<>();
                for (int i = 1; i <= columnNumbers; i++) {
                    dataMap.put(rsMetaData.getColumnLabel(i), resultSet.getString(i));
                }

                // Add the data to List of Maps
                resultsMap.add(dataMap);
            }

        } catch (SQLException e) {
            logger.error(e);
        }

        return resultsMap;
    }
    
    /**
     * Closes given Connection object, very important to avoid errors
     * @param conn the Connection to close
     * @author sanchez
     */
    public void closeConnection(Connection conn){
        if (conn != null){
            try {
                conn.close();
            } catch (SQLException ex) {
                logger.error(ex);
            }
        }
    }
}
