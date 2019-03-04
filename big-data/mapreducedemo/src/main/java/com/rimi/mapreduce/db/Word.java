package com.rimi.mapreduce.db;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author admin
 * @date 2018-09-14
 */
public class Word implements DBWritable {

    private String word;

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1,this.word);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.word = resultSet.getString(1);
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }
}
