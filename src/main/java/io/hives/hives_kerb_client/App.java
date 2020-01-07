package io.hives.hives_kerb_client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;


class App  {

  public static void main(String args[]) throws ClassNotFoundException, SQLException {


        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        Connection con = DriverManager.getConnection(
                                "jdbc:hive2://ec2-3-94-194-21.compute-1.amazonaws.com:10000/;principal=hive/ip-10-238-144-183.ec2.internal@techvedika.com");
        Statement sqlstatement = con.createStatement();
        //ResultSet result = sqlstatement.executeQuery("INSERT INTO TABLE students VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32)");
        ResultSet result = sqlstatement.executeQuery("select name from students");
        while (result.next()) {
           System.out.println("Toatal number of Students : " + result.getString(1));
        }
  }

}
