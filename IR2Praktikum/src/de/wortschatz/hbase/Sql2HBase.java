package de.wortschatz.hbase;

/**
 * Created by max on 30.03.15.
 */
public class Sql2HBase {
    public static void main(String[] args) {
            ThreadGroup tg = new ThreadGroup("emigrators");

            new Thread(tg,new CooccurrenceEmigrationManager()).start();
            new Thread(tg,new SentenceEmigrationManager()).start();
            new Thread(tg,new SourceEmigrationManager()).start();
            new Thread(tg,new WordEmigrationManager()).start();

    }

}
