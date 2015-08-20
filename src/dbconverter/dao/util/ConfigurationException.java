package dbconverter.dao.util;

/**
 * Custom exception class for when a function receives invalid parameters or 
 * configuration settings
 * @author hightowe
 */
public class ConfigurationException extends Exception
{
      //Parameterless Constructor
      public ConfigurationException() {}

      //Constructor that accepts a message
      public ConfigurationException(String message)
      {
         super(message);
      }
 }