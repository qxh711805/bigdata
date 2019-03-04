package com.rimi.mapreduce.db;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author admin
 * @date 2018-09-14
 */
public class Result implements DBWritable {

    private String word;
    private Integer num;// 个数

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1,this.word);
        statement.setInt(2,this.num);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.word = resultSet.getString(1);
        this.num = resultSet.getInt(2);
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }
}
