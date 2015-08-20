/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbconverter.dao.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author sanchez
 */
public class Utils {
    
    private final static Logger logger = LogManager.getLogger(Utils.class.getName());
    
    /**
     * Method will take a File that contains SQL data and extract the data into
     * a String object that can be passed to a ResultSet object
     *
     * @param queryFileName
     * @return String object from a SQL file
     */
    public String readQueryFile(String queryFileName) {
        assert queryFileName != null : "queryFileName cannont be null";
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
            //Logger.getLogger(OracleDAO.class.getName()).log(Level.SEVERE, null, ex);
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

        //InputStream input = new URL(queryFileName).openStream();
        query = queryBuilder.toString();
        return query;
    }
    
}
