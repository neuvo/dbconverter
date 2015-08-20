/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbconverter.dao;

import dbconverter.dao.OracleDAO;
import dbconverter.dao.util.ConfigCommons;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author sanchez
 */
public class OracleDAOTest {

    private static String oracleConfigFileName = "oracledb.properties";
    private static URL oracleConfigFile = OracleDAOTest.class.getResource(oracleConfigFileName);
    private static ConfigCommons configCommons = null;
    private final String className = OracleDAOTest.class.getName();
    private final int LINE_LENGTH = 100;
    private static final Logger logger = LogManager.getLogger(OracleDAOTest.class.getName());
    

    public OracleDAOTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        configCommons = new ConfigCommons();
        configCommons.loadConfig(oracleConfigFile.getFile());
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
     * Test of getConnection method, of class OracleDAO.
     */
    @Test
    public void testGetConnection() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        System.out.println("getConnection" + TestUtils.getClassBeingTestedInfo(className));
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        
        System.out.println("Test checks that the connection to Oracle is not null");
        

        OracleDAO instance = new OracleDAO();

//        Config config = new Config();
//        config.loadConfig(oracleConfigFile.getFile());
        //instance.getJDBCDriver();
        //Connection connection = instance.getConnection(Config.getJdbcParamaters());
        Connection connection = instance.getConnection(configCommons.getJdbcParametersOracle());

        try {
            assert connection != null;
            connection.close();
        } catch (SQLException ex) {
            if (connection == null) {
                try {
                    connection.close();
                } catch (SQLException ex1) {
                    logger.error("Something went wrong trying to close the DB Connection", ex1);
                }
            }
            logger.error("Something went wrong trying to close the DB Connection", ex);
        }
        TestUtils.getLineSeparator("=", LINE_LENGTH);
    }

    @Test
    public void testGetResultSet() {
        TestUtils.printSpace();

        TestUtils.getLineSeparator("*", LINE_LENGTH);
        System.out.println("getResultSet" + TestUtils.getClassBeingTestedInfo(className));
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        
        System.out.println("Test checks that we can get a result set from a query sent to Oracle DB");
        System.out.println("and the ResultSet being returned is not null.");
        

        OracleDAO instance = new OracleDAO();

        Connection connection = instance.getConnection(configCommons.getJdbcParametersOracle());

        String sqlQuery = "select * from jplpa_ibt_budgets";

        //ResultSet rs = null;
        try {
            ResultSet rs = instance.getResultSet(connection, sqlQuery);
            assert rs != null;
            connection.close();
        } catch (SQLException ex) {
            if (connection == null) {
                try {
                    connection.close();
                } catch (SQLException ex1) {
                    logger.error("Something went wrong trying to close the DB Connection", ex1);
                }
            }
            logger.error("Something went wrong trying to close the DB Connection", ex);
        }
        TestUtils.getLineSeparator("=", LINE_LENGTH);
    }

    @Test
    public void testGetResultSetMap() {
        TestUtils.printSpace();

        TestUtils.getLineSeparator("*", LINE_LENGTH);
        System.out.println("getResultSetMap" + TestUtils.getClassBeingTestedInfo(className));
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        
        System.out.println("Test checks that a List of Maps contains the result set data and it's not empty");
        

        OracleDAO instance = new OracleDAO();
        //instance.getJDBCDriver();
        Connection connection = instance.getConnection(configCommons.getJdbcParametersOracle());

        /**
         * Getting the first query from the query.file property in the
         * configCommons
         */
        String query = configCommons.getQueryFiles().get(0);

        URL queryFileName = OracleDAOTest.class.getResource(query);
        ConfigCommons config = new ConfigCommons();
        String sqlQuery = config.readQueryFile(queryFileName.getFile());

        //ResultSet rs = null;
        try {
            ResultSet rs = instance.getResultSet(connection, sqlQuery);

            List<Map> rsMap = new ArrayList<Map>();
            rsMap.addAll(instance.getResultSetMap(connection, rs));
            assert !rsMap.isEmpty();
            System.out.println("Map size is " + rsMap.size());

            Map<String, Object> testerMap = rsMap.get(0);

            System.out.println("");
            System.out.println("Map sample data");

            for (String key : testerMap.keySet()) {
                System.out.println(key + " : " + testerMap.get(key));
            }

            connection.close();
        } catch (SQLException ex) {
            if (connection == null) {
                try {
                    connection.close();
                } catch (SQLException ex1) {
                    logger.error("Something went wrong trying to close the DB Connection", ex1);
                }
            }
            logger.error("Something went wrong trying to close the DB Connection", ex);
        }
        TestUtils.getLineSeparator("=", LINE_LENGTH);
    }

}
