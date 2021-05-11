import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.sql.SQLException;

/**
 * @author wangqingshuai
 */
public class CompareLatency {
    public static Connection getDBConnection(String ip, int port, String dbName, String userName, String passwd) {
        String driver = "com.mysql.cj.jdbc.Driver";
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
        System.out.print(dataBase+"\t");
        Connection connection = getDBConnection("127.0.0.1", 3306, dataBase, "qswang", "Biui1227..");
        String dir = args[1];
        for (int i = Integer.parseInt(args[2]); i <= Integer.parseInt(args[3]); i++) {
            System.out.print(dir + i + ".sql\t");
            BufferedReader bufferedReader = new BufferedReader(new FileReader(dir + i + ".sql"));
            StringBuilder sql = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if (!line.trim().startsWith("--"))
                    sql.append(line).append(" ");
            }
            bufferedReader.close();
            Statement statement = connection.createStatement();
            long testStart = System.currentTimeMillis();
            for (int j = 0; j < 4; j++) {
       //         System.out.println(sql.toString());
                statement.executeQuery(sql.toString());
            }
            int testCount = 5;
            int reduceCount = 1;
            if((System.currentTimeMillis()-testStart)/4<1000){
                testCount = 10;
                reduceCount = 3;
            }
            long[] latencyArray = new long[testCount];
            long start = System.currentTimeMillis();
            long end;
            for (int j = 0; j < testCount; j++) {
                statement.executeQuery(sql.toString());
                latencyArray[j] = (end = System.currentTimeMillis()) - start;
                start = end;
            }
            Arrays.sort(latencyArray);
            long totalTime = 0l;
            for (int j = reduceCount; j < latencyArray.length - reduceCount; j++) {
                totalTime += latencyArray[j];
            }
            System.out.println(totalTime / (latencyArray.length - 2*reduceCount));
        }
    }
}
