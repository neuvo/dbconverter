// TODO: delete this class?
package dbconverter.dao;

import com.opencsv.CSVReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class for converting CSV files to JSON, uses OpenCSV
 * http://opencsv.sourceforge.net/
 * @author hightowe
 */
public class CsvDAO {
    
    private final static Logger logger = LogManager.getLogger(OracleDAO.class.getName());
    
    /**
     * Converts the given CSV file to a List object
     * Returns the object if successful, returns null if not successful
     * @param filename
     * @return
     * @author hightowe
     */
    public List<String[]> readFile(String filename) {
        CSVReader reader = null;
        try {
            reader = new CSVReader(new FileReader(filename));
        } catch (FileNotFoundException ex) {
            logger.fatal(filename + " not found, " + ex);
        }
        
        if (reader == null) {
            return null;
        }
        
        List<String[]> result = null;
        
        try {
            // put csv file contents into result
            // potential for heap space error, if file is large enough
            result = reader.readAll();
            assert result != null: "Error reading from file";
            return result;
        } catch (IOException ioe) {
            logger.fatal("Error reading file " + filename + " " + ioe);
        }
        
        try {
            reader.close();
        } catch (IOException ioe) {
            logger.error("Failed to close csvreader: " + ioe);
        }
        
        return result;
    }
    
    /**
     * Returns the data in a csv file as a List<Map>, prepared for conversion
     * to JSON.
     * Keys are field names, values are field values.
     * @param filename
     * @return 
     * @author hightowe
     */
    public List<Map> getObjects(String filename) {
        List<Map> allObjects = new ArrayList<>();
        
        List<String[]> fileRows = this.readFile(filename);
        assert fileRows != null;        // if null, readFile had an error
        assert fileRows.size() > 1;     // must contain data
        
        String[] fieldNames = fileRows.get(0);
        
        // for loop skips first row, which contains the field names
        for (int i = 1; i < fileRows.size(); i++) {
            String[] currentRow = fileRows.get(i);
            Map<String,Object> objectMap = new HashMap<>();
            for (int j = 0; j < fieldNames.length; j++) {
                objectMap.put(fieldNames[j], currentRow[j]);
            }
            allObjects.add(objectMap);
        }
        
        return allObjects;
    }
}
