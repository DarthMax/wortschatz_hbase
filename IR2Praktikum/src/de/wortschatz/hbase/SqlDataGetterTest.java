package de.wortschatz.hbase;

import junit.framework.TestCase;

import java.sql.Connection;

public class SqlDataGetterTest extends TestCase {

    public void testGetDataFromQuery() throws Exception {
        Connection con = SqlConnector.get_connection();
        String query = "select w1.word, 'l', c.sig, w2.word from words w1, words w2, co_s c " +
                "where w1.w_id=c.w1_id and c.w2_id=w2.w_id and w1.word=\"Auto\"";
        SqlDataGetter sqlDataGetter = new SqlDataGetter(con);

    }

    public void testGetDataFromQuery1() throws Exception {

    }
}