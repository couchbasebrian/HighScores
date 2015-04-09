// HighScoreTest.java
// April 8, 2015
//
// This program illustrates a sorted compound key example with Couchbase

package com.couchbase.support;

import java.util.concurrent.TimeUnit;
import java.util.Iterator;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;

// Assuming a bucket exists called "highscores"

// This program also assumes that a view as follows was created in the UI:
// function(doc, meta) {
//   emit(meta.id,doc);
// }

public class HighScoreTest {

	static final String HOSTNAME             = "192.168.41.101";  // please replace with yours
	static final String HIGHSCORESBUCKET     = "highscores";
	static final int    NUMBEROFRANDOMSCORES = 1000;
	
	
	public static void main(String[] args) {
	
		long t1, t2;
		
		// Connect to the cluster and establish a connection to the data bucket
		CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().build();
		Cluster cluster = CouchbaseCluster.create(env, HOSTNAME);
		Bucket  bucket  = cluster.openBucket(HIGHSCORESBUCKET, 10, TimeUnit.SECONDS);			

		// Start by clearing out the bucket's current contents
		t1 = System.currentTimeMillis();		
		bucket.bucketManager().flush();
		t2 = System.currentTimeMillis();
		System.out.println("Took " + (t2 - t1) + " ms to flush the bucket");
		
		// Put scores into the bucket
		HighScoreStore hss = new HighScoreStore(bucket);
		int numScoresToInsert = NUMBEROFRANDOMSCORES;	// 1000 randomly generated player scores
		t1 = System.currentTimeMillis();
		hss.insertRandomScores(numScoresToInsert);
		t2 = System.currentTimeMillis();
		System.out.println("Took " + (t2 - t1) + " ms to insert " + numScoresToInsert + " random scores");

		// Eventual consistency:  Loop until the expected value is received.
		ViewResult topScores;
		int highestScoreObservedThisTime = 0;
		int iteration = 0;
		t1 = System.currentTimeMillis();
		while (highestScoreObservedThisTime != hss.highestScore) {
			System.out.println("---- Iteration: " + iteration + " Highest Score Observed: " + highestScoreObservedThisTime + " ----");
			topScores = getTopScores(hss, 10);		// get the top 10 scores, a process which can also loop.
			highestScoreObservedThisTime = displayHighScoreTable(topScores);
			iteration++;
		}
		t2 = System.currentTimeMillis();
		System.out.println("Took " + (t2 - t1) + " ms and " + iteration + " iterations to get the expected answer");
		
		System.out.println("Goodbye.");
	}	// end of main()
	
	public static ViewResult getTopScores(HighScoreStore hss, int numTopScoresToGet) {

		ViewResult topScores  = null;
		int rowsReceived      = 0;
		long t1, t2;
		int iterations = 0;
		
		// Eventual consistency:  Loop until at least the requested number of results is available
		System.out.print("Querying View: ");
		t1 = System.currentTimeMillis();
		while (rowsReceived < numTopScoresToGet) {
		  topScores = hss.getTopScores(numTopScoresToGet);
		  rowsReceived = topScores.totalRows();
		  System.out.print(".");
		  //System.out.println("I got " + rowsReceived + " rows.");
		  iterations++;
		}
		t2 = System.currentTimeMillis();
		System.out.println();
		System.out.println("Took " + (t2 - t1) + " ms and " + iterations + " iterations to get " + numTopScoresToGet + " top scores");
		System.out.println("I got back " + rowsReceived + " rows");
		return topScores;
	}
	
	public static int displayHighScoreTable(ViewResult topScores) {
		long t1, t2;
		ViewRow eachViewRow;
		JsonDocument eachResultJD;
		JsonObject jsonObject;
		int i = 1;
		int highestScoreSeenInResult = 0;
		String eachKey, eachValue;
		int eachScoreFromResults = 0;
		
		t1 = System.currentTimeMillis();
		Iterator<ViewRow> vri = topScores.rows();
		while (vri.hasNext()) {
			eachViewRow = vri.next();
			eachResultJD = eachViewRow.document();
			jsonObject = eachResultJD.content();
			eachScoreFromResults = (jsonObject.getInt(HighScoreStore.SCORE)).intValue();
			if (eachScoreFromResults > highestScoreSeenInResult) { highestScoreSeenInResult = eachScoreFromResults; };
			eachKey = eachViewRow.key().toString();
			eachValue = eachViewRow.value().toString();
			System.out.printf("Rank: %2d key: %20s value: %s\n", i, eachKey, eachValue);
			i++;
		}
		t2 = System.currentTimeMillis();
		System.out.println("Took " + (t2 - t1) + " ms to display top scores");
		System.out.println("Highest score seen in these results was: " + highestScoreSeenInResult);
		return highestScoreSeenInResult;
		
	}
	
}



class HighScoreStore {

	static int maxHighScore = 100000;

	// attributes in the JSON object that we store in Couchbase for each player score
	public static final String SCORE      = "score";
	public static final String PLAYERNAME = "playerName";

	public static final String DESIGNDOCUMENTNAME = "highscoredd";
	public static final String VIEWNAME           = "highscoreview";
	
	
	Bucket _bucket;
	
	// set these to ridiculous/improbable values
	int lowestScore  = maxHighScore;
	int highestScore = 0;
	
	public int getLowestScore()  { return lowestScore;  };
	public int getHighestScore() { return highestScore; };
	
	// constructor
	public HighScoreStore(Bucket b) {
		_bucket = b;
	}
	
	public ViewResult getTopScores(int numTopScores) {
		
		ViewResult result = _bucket.query(
			    ViewQuery
			        .from(DESIGNDOCUMENTNAME, VIEWNAME)
			        .descending()
			        .limit(numTopScores)
			);		
		
		return result;
	}
	
	public void insertRandomScores(int numScores) {
		JsonObject newScore;
		JsonDocument jsonDoc;
		String scoreKey;
		String playerName;
		int randomScore = -1;
				
		for (int i = 0; i < numScores; i++) {
			
			playerName = "player" + i;
			randomScore = (int) (Math.random() * maxHighScore);
			scoreKey = String.format("%07d_%s", randomScore, playerName);	 	// compound key
			
			if (randomScore > highestScore) { highestScore = randomScore; };
			if (randomScore < lowestScore)  { lowestScore = randomScore; };
			
			newScore = JsonObject.create();
			newScore.put(PLAYERNAME, playerName);
			newScore.put(SCORE, randomScore);
			
			jsonDoc = JsonDocument.create(scoreKey, newScore);
			_bucket.upsert(jsonDoc);
		}
		
		System.out.println("Inserted " + numScores + " random scores between " + lowestScore + " and " + highestScore);
	}
	
}
