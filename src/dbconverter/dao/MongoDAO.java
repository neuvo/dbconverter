package dbconverter.dao;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.bson.Document;

/**
 * Class provides a Data Access Object for MongoDB
 * @author sanchez
 */
public class MongoDAO {
    private final static org.apache.logging.log4j.Logger logger = LogManager.getLogger(MongoDAO.class.getName());

    /**
     * 
     * @param connectionParams - a String variable that contains the connection params
     * for mongo in a URI format 
     * E.g.
     * "mongodb://localhost:27017" or "mongodb://username:password@localhost:27017"
     * @return configured MongoClient
     * @author sanchez
     */
    public MongoClient getConnection(String connectionParams) {

        MongoClient mongoClient = new MongoClient(getConnectionString(connectionParams));
        return mongoClient;

    }

    /**
     * 
     * @param conn a MongoClient object
     * @param dbName a String that represents the data base name
     * @param collectionName a String object that represents the collection name
     * @return a List object MongoCollection of Documents
     * @author sanchez
     */
    public MongoCollection<Document> getCollection(MongoClient conn, String dbName, String collectionName) {

        MongoDatabase mongoDB = conn.getDatabase(dbName);
        MongoCollection<Document> collection = mongoDB.getCollection(collectionName);

        return collection;

    }

    /**
     * Method provides a way to pass a query in json format so that documents
     * returned from the collection are filtered by the query parameters
     *
     * @param jsonQuery
     * @param collection
     * @return FindIterable<Document> List object
     * @author sanchez
     */
    public FindIterable<Document> getDocuments(String jsonQuery, MongoCollection collection) {
        assert jsonQuery != null && !jsonQuery.isEmpty() : "jsonQueryName cannot be empty or null";
        assert collection != null : "collection cannot be null";

        BasicDBObject searchQuery = (BasicDBObject) JSON.parse(jsonQuery);

        FindIterable<Document> documents = collection.find(searchQuery);

        return documents;

    }
    
    /**
     * Will provide an Object of type FindIterable<Document> List which contains 
     * all of the data in the MongoCollection being passed
     * @param collection
     * @return FindIterable<Document> List object
     */
    public FindIterable<Document> getDocumentsAll(MongoCollection collection) {

        FindIterable<Document> documents = collection.find();

        return documents;

    }

    /**
     * Method provides the documents from a FindIterable<Document> object as List<String>
     * that contains all the documents converted to JSON 
     * @param documents The MongoDocuments to convert
     * @return a List<String> object that contains all the documents in JSON format
     */
    public List<String> getDocumentsInJSONFormat(FindIterable<Document> documents) {

        List<String> jsonDocuments = new ArrayList<>();

        for (Document doc : documents) {
            jsonDocuments.add(doc.toJson());
        }

        return jsonDocuments;

    }

    /**
     * Provides a 
     * @param params a String object that represents the mongo URI
     * E.g. mongodb://localhost:27017 or "mongodb://username:password@localhost:27017"
     * @return a MongoClientURI object
     */
    private MongoClientURI getConnectionString(String params) {

        MongoClientURI uri = new MongoClientURI(params);

        return uri;

    }

}
