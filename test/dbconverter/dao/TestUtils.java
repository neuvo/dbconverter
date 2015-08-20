/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbconverter.dao;

/**
 * This class contains methods that are little helpers when building test
 * @author sanchez
 */
public class TestUtils {
    
    /**
     * This method will provide print out of lines determined by the character provided
     * and it will build the line depending on the length of the int passed
     * @param separatorType
     * @param length 
     */
    public static void getLineSeparator(String separatorType, int length){
        
        
        if (separatorType == null || separatorType.isEmpty() || length == 0){
            separatorType = "=";
            length = 100;
        }
        
        StringBuilder sb = new StringBuilder();
            String separator;
            for (int i = 0; i < length; i++){
                sb.append(separatorType);
            }
            
            separator = sb.toString();
            System.out.println(separator);
        
    }
    
    /**
     * Prints a space using System.out.println()
     */
    public static void printSpace(){
        System.out.println("");
    }
    
    /**
     * Provides a little more info about the origin of the class being tested
     * @param testClass - A String object - You can get this by using <ClassNameTest>.class.getName()
     * @return 
     */
    public static String getClassBeingTestedInfo(String testClass){
        String testClassName = testClass;
        String classBeingTested = testClassName;
        String retVal = null;
        
        if(testClassName.endsWith("Test")){
            int tcNameLen = testClassName.length();
            classBeingTested = testClassName.substring(0, (tcNameLen - 4));
        }
        
        
        retVal = " test for " + classBeingTested + " class.";
        return retVal;
    }
    
}
