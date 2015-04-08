package com.couchbase.support;

import java.util.Observable;
import java.util.Random;
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

// Assuming a bucket exists called highscores

// This assumes a view as follows
// function(doc, meta) {
//   emit(meta.id,doc);
// }

public class HighScoreTest {

	public static void main(String[] args) {
	
		CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().build();
		Cluster cluster = CouchbaseCluster.create(env, "192.168.41.101");
		Bucket bucket  = cluster.openBucket("highscores", 10, TimeUnit.SECONDS);			
		
		HighScoreStore hss = new HighScoreStore(bucket);
	
		hss.insertRandomScores(1000);
	
		int numTopScoresToGet = 10;
		long t1 = System.currentTimeMillis();
		ViewResult topScores = hss.getTopScores(numTopScoresToGet);
		long t2 = System.currentTimeMillis();
		System.out.println("Took " + (t2 - t1) + " ms to get " + numTopScoresToGet + " top scores");
	
		Iterator<ViewRow> vri = topScores.rows();

		ViewRow eachViewRow;
		JsonDocument eachResultJD;
		String eachKey, eachValue;

		while (vri.hasNext()) {
			eachViewRow = vri.next();
			eachResultJD = eachViewRow.document();
			eachKey = eachViewRow.key().toString();
			eachValue = eachViewRow.value().toString();
			System.out.println("key:" + eachKey + " value: " + eachValue);
		}
		
	}
}


class HighScoreStore {
	
	Bucket _bucket;
	static int maxHighScore = 100000;
	
	public HighScoreStore(Bucket b) {
		_bucket = b;
	}
	
	public ViewResult getTopScores(int numTopScores) {
		
		ViewResult result = _bucket.query(
			    ViewQuery
			        .from("highscoredd", "highscoreview")
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
		int highScore = -1;
		
		for (int i = 0; i < numScores; i++) {
			
			playerName = "player" + i;
			highScore = (int) (Math.random() * maxHighScore);
			scoreKey = String.format("%07d_%s", highScore, playerName);	 	// compound key
			
			newScore = JsonObject.create();
			newScore.put("playerName", playerName);
			newScore.put("score", highScore);
			
			jsonDoc = JsonDocument.create(scoreKey, newScore);
			_bucket.upsert(jsonDoc);
		}
	}
	
}
