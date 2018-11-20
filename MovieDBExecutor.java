package space.harbour.java.hw10;

import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import space.harbour.java.hw4.Movie;

import java.util.ArrayList;
import java.util.List;

public class MovieDBExecutor {
	private MongoClient mongo;
	private MongoDatabase mongoDb;
	private MongoCollection<Document> collection;

	public MovieDBExecutor() {
		 mongo = new MongoClient("localhost", 27017);
		 mongoDb = mongo.getDatabase("java-programming-db");
		 collection = mongoDb.getCollection("movie");
	}

	public void execInsert(Movie movie) {
		Document document = Document.parse(movie.toJsonString());
		collection.insertOne(document);
	}

	public long execDelete(Bson query) {
		try {
			DeleteResult result = collection.deleteMany(query);
			if (result.wasAcknowledged())
				return result.getDeletedCount();
		} catch (MongoException e) {
			System.err.println(e);
			return -1;
		}

		return 0;
	}

	public List<Movie> execQuery(BasicDBObject searchQuery) {
		List<Movie> movies = new ArrayList<>();

		FindIterable<Document> result = collection.find(searchQuery);

		result.forEach((Block<Document>) document -> {
			Movie movie = new Movie();
			movie.fromJson(document.toJson());
			movies.add(movie);
		});

		return movies;
	}

	public void execDeleteAll() {
		collection.drop();
	}

	public void close() {
		mongo.close();
	}
}
