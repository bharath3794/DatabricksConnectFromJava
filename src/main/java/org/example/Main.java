package org.example;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Main {
    /*
    * Connection and Query Results:
        1. Connect to JDBC using below:
            https://docs.databricks.com/integrations/jdbc-odbc-bi.html#jdbc-driver
        2. Add Databricks JDBC Driver Jar file to class path in IntelliJ
        3. Write Java code using below
            https://stackoverflow.com/questions/56485607/how-to-connect-to-databricks-delta-table-using-jdbc-driver
        4. Disable Arrow usage by adding "EnableArrow=0" at end of your JDBC URL
            https://community.databricks.com/s/question/0D58Y00009AHCDSSA5/jdbc-driver-support-for-openjdk-17
     */
    public static void main(String[] args) throws SQLException {
        String url = "jdbc:databricks://<YOUR_HOST_NAME>:443/default;transportMode=http;ssl=1;httpPath=<YOUR_HTTP_PATH>;AuthMech=3;UID=token;EnableArrow=0;";
        Properties p = new java.util.Properties();
        p.put("PWD", "YOUR_PERSONAL_ACCESS_TOKEN");

        Connection connection = DriverManager.getConnection(url, p);
        if(connection != null){
            System.out.println("Connection Established");

        }
        else {
            System.out.println("Connection Failed");
        }

        List<String> docUUID = new ArrayList<>();
        List<String> firstName = new ArrayList<>();
        Statement statement = connection.createStatement();
        try {
            ResultSet resultSet = statement.executeQuery("SELECT doc_uuid, first_name FROM test.table_name LIMIT 2");
            while (resultSet.next()) {
                // Iterate through Result set
                docUUID.add(resultSet.getString(1));
                firstName.add(resultSet.getString(2));
            }
            System.out.println("docUUID: "+docUUID);
            System.out.println("firstName: "+firstName);
        } finally {
            statement.close();
            connection.close();
        }
    }
}