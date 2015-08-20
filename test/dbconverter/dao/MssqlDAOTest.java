/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbconverter.dao;

import dbconverter.dao.MssqlDAO;
import dbconverter.dao.util.ConfigCommons;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author hightowe
 */
public class MssqlDAOTest {
    
    private final int LINE_LENGTH = 100;
    private static String dbConn;
    private static String dbUser;
    private static String dbPass;
    
    private static final String mssqlConfigFileName = "mssqldb.properties";
    private static URL mssqlConfigFile = MssqlDAOTest.class.getResource(mssqlConfigFileName);
    
    public MssqlDAOTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
        assert mssqlConfigFile != null;
        ConfigCommons config = new ConfigCommons();
        config.loadConfig(mssqlConfigFile.getFile());
        dbConn = config.getJdbcParametersMssql();
        dbUser = config.getMssqlUser();
        dbPass = config.getMssqlPass();
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
     * Test of dbConnect method, of class MssqlDAO.
     */
    @Test
    public void testGetConnection() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        System.out.println("getConnection" + TestUtils.getClassBeingTestedInfo(MssqlDAOTest.class.getName()));
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        
        MssqlDAO instance = new MssqlDAO();
        
        Connection result = instance.getConnection(dbConn, dbUser, dbPass);
        assert result != null;
        
        TestUtils.printSpace();
        
        instance.closeConnection(result);
    }

    /**
     * Test of dbDisconnect method, of class MssqlDAO.
     */
    @Test
    public void testCloseConnection() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        System.out.println("closeConnection" + TestUtils.getClassBeingTestedInfo(MssqlDAOTest.class.getName()));
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        
        MssqlDAO instance = new MssqlDAO();
        
        Connection conn = instance.getConnection(dbConn,dbUser,dbPass);
        
        assert conn != null;
        
        instance.closeConnection(conn);
        
        TestUtils.printSpace();
    }

    /**
     * Test of selectData method, of class MssqlDAO.
     */
    @Test
    public void testGetResultSet() {
        TestUtils.printSpace();
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        System.out.println("getResultSet" + TestUtils.getClassBeingTestedInfo(MssqlDAOTest.class.getName()));
        TestUtils.getLineSeparator("*", LINE_LENGTH);
        
        MssqlDAO instance = new MssqlDAO();
        
        Connection conn = instance.getConnection(dbConn, dbUser, dbPass);
        assert conn != null;
        String query = "select id, user_name, time from ibt.dbo.email_log where id<5;";
        System.out.println(query);
        ResultSet rs = instance.getResultSet(conn, query);
        
        assert rs != null;
        // the following code was taken from here:
        // https://coderwall.com/p/609ppa/printing-the-result-of-resultset
        try { 
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print(",  ");
                    String columnValue = rs.getString(i);
                    System.out.print(columnValue + " " + rsmd.getColumnName(i));
                }
                System.out.println("");
            }

            TestUtils.printSpace();
        }
        catch (SQLException e) {
            System.out.println("Failed to read meta data: " + e);
        }
        
        instance.closeConnection(conn);
        
        boolean except = false; // rs should be closed now
        try {
            rs.getMetaData();
        }
        catch (SQLException ex) {
            except = true;
        }
        assert except;
    }
    
}
