package de.wortschatz.hbase;

/**
 * Created by max on 24.01.15.
 */
public class Cooccurrence {

    private String word1;
    private String word2;
    private Float significance;
    private Long frequency;

    public Cooccurrence(String word1, String word2, Float significance, Long frequency) {
        this.word1 = word1;
        this.word2 = word2;
        this.significance = significance;
        this.frequency = frequency;
    }

    public String getWord1() {
        return word1;
    }

    public String getWord2() {
        return word2;
    }

    public Float getSignificance() {
        return significance;
    }

    public Long getFrequency() {
        return frequency;
    }

    public void setSignificance(Float significance) {
        this.significance = significance;
    }
}
