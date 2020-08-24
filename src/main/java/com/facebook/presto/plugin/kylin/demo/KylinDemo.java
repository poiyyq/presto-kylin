package com.facebook.presto.plugin.kylin.demo;

import org.apache.kylin.jdbc.Driver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class KylinDemo {
    public static void main(String[] args) throws SQLException {
        Driver driver = new Driver();
        Properties props = new Properties();
        props.put("user","ADMIN");
        props.put("password","KYLIN");
        Connection connect = driver.connect("jdbc:kylin://testcdh001:7070/learn_kylin", props);
        Statement statement = connect.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from kylin_country");
        while(resultSet.next()){
            System.out.println(resultSet.getString(1));
        }
    }
}
