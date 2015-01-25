package de.wortschatz.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

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

    public Cooccurrence(Result row) {
        word2 = Arrays.toString(row.getValue(Bytes.toBytes("data"), Bytes.toBytes("word2")));
        word1 = Arrays.toString(row.getValue(Bytes.toBytes("data"), Bytes.toBytes("word1")));
        byte[] sigByte = row.getValue(Bytes.toBytes("data"),Bytes.toBytes("sig"));
        significance = ByteBuffer.wrap(sigByte).getFloat();
        byte[] freqByte = row.getValue(Bytes.toBytes("data"),Bytes.toBytes("freq"));
        frequency = ByteBuffer.wrap(freqByte).getLong();
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Cooccurrence)) return false;

        Cooccurrence that = (Cooccurrence) o;

        if (frequency != null ? !frequency.equals(that.frequency) : that.frequency != null) return false;
        if (significance != null ? !significance.equals(that.significance) : that.significance != null) return false;
        if (word1 != null ? !word1.equals(that.word1) : that.word1 != null) return false;
        if (word2 != null ? !word2.equals(that.word2) : that.word2 != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = word1 != null ? word1.hashCode() : 0;
        result = 31 * result + (word2 != null ? word2.hashCode() : 0);
        result = 31 * result + (significance != null ? significance.hashCode() : 0);
        result = 31 * result + (frequency != null ? frequency.hashCode() : 0);
        return result;
    }
}
