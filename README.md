# HighScores
An example of storing and retrieving HighScores from Couchbase

This was developed using Eclipse, Java 1.8, [Couchbase Java SDK 2.0.3](http://packages.couchbase.com/clients/java/2.0.3/Couchbase-Java-Client-2.0.3.zip), and Couchbase Server 3.0.2

It assumes the existence of a bucket named "highscores", a design document named "highscoredd", and a published Production view named "highscoreview"

When the program starts up, it will insert 1,000 random player high scores between 0 and 100,000 into the bucket.
Then it will query the view for the records for the top 10 players.

The view, highscoreview, looks like this:

Sample Output

    Took 96 ms to get 10 top scores
    key:0096465_player7 value: {"score":96465,"playerName":"player7"}
    key:0094668_player8 value: {"score":94668,"playerName":"player8"}
    key:0093035_player2 value: {"score":93035,"playerName":"player2"}
    key:0090151_player1 value: {"score":90151,"playerName":"player1"}
    key:0087016_player5 value: {"score":87016,"playerName":"player5"}
    key:0085318_player3 value: {"score":85318,"playerName":"player3"}
    key:0084352_player2 value: {"score":84352,"playerName":"player2"}
    key:0083528_player4 value: {"score":83528,"playerName":"player4"}
    key:0078201_player1 value: {"score":78201,"playerName":"player1"}
    key:0078011_player9 value: {"score":78011,"playerName":"player9"}

The view that is used looks like this:

    function(doc, meta) {
      emit(meta.id,doc);
    }
