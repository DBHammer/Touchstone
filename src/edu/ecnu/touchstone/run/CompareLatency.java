package edu.ecnu.touchstone.run;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author wangqingshuai
 */
public class CompareLatency {
    public static Connection getDBConnection(String ip, int port, String dbName,
                                             String userName, String passwd) {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://" + ip + ":" + port + "/" + dbName;
        Connection conn = null;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, userName, passwd);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void main(String[] args) throws IOException, SQLException {
        String dataBase = args[0];
        Connection connection = getDBConnection("10.24.14.56", 3306, dataBase, "qswang", "Biui1227..");
        String dir = args[1];
        int end = Integer.parseInt(args[2]);
        for (int i = 2; i <= end; i++) {
            System.out.print(dir + i + ".sql\t");
            BufferedReader bufferedReader = new BufferedReader(new FileReader(dir + i + ".sql"));
            StringBuilder sql = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                sql.append(line).append(" ");
            }
            PreparedStatement statement = connection.prepareStatement(sql.toString());
            for (int j = 0; j < 10; j++) {
                statement.executeQuery();
            }
            int testCount = 5;
            long start = System.currentTimeMillis();
            for (int j = 0; j < testCount; j++) {
                statement.executeQuery();
            }
            System.out.println((System.currentTimeMillis() - start) / testCount);
        }
    }
}
