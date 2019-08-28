import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class runDataOnDatabase {

    private static final String QUERY2_VALUE = "aRlr";
    private static final String QUERY3_VALUE1 = "gFF";
    private static final String QUERY3_VALUE2 = "699999.988079071";
    private static final String QUERY4_VALUE = "2000";

    public static void main(String[] args) {
        MysqlConnector mysqlConnector = new MysqlConnector();
        try {
            mysqlConnector.createTables();
            mysqlConnector.loadData(2);

            System.out.println("Query1:");
            String query1 = "SELECT COUNT(*) FROM PART LEFT JOIN PRODUCT ON P_PARTID=PR_PARTID";
            mysqlConnector.computeNum(query1);

            System.out.println("\nQuery2:");
            String query2 = "SELECT COUNT(*) FROM PART RIGHT JOIN PRODUCT ON P_PARTID=PR_PARTID" +
                    " AND TYPE ='" + QUERY2_VALUE + "'";
            mysqlConnector.computeNum(query2);
            query2 += " WHERE P_PARTID IS NULL";
            mysqlConnector.computeNum(query2);

            System.out.println("\nQuery3:");
            String query3InnerJoin= "SELECT COUNT(*) FROM PART JOIN PRODUCT ON P_PARTID=PR_PARTID" +
                    " AND SUPPLIER ='" + QUERY3_VALUE1 + "' AND TOTALSOLD>" + QUERY3_VALUE2;
            mysqlConnector.computeNum(query3InnerJoin);
            String query3LeftOuterNull = "SELECT COUNT(*) FROM PART LEFT JOIN PRODUCT ON P_PARTID=PR_PARTID" +
                    " AND TOTALSOLD>" + QUERY3_VALUE2 +" WHERE SUPPLIER ='" + QUERY3_VALUE1 + "' AND PR_PARTID IS NULL";
            mysqlConnector.computeNum(query3LeftOuterNull);
            String query3RightOuterNull = "SELECT COUNT(*) FROM PART RIGHT JOIN PRODUCT ON P_PARTID=PR_PARTID" +
                    " AND SUPPLIER ='" + QUERY3_VALUE1 + "' WHERE TOTALSOLD>" + QUERY3_VALUE2+" AND P_PARTID IS NULL";
            mysqlConnector.computeNum(query3RightOuterNull);

            System.out.println("\nQuery4:");
            String query4 = "SELECT COUNT(*) FROM PART JOIN PRODUCT ON P_PARTID=PR_PARTID" +
                    " WHERE STOCK<'" + QUERY4_VALUE + "'";
            mysqlConnector.computeNum(query4);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

class MysqlConnector {
    /**
     * JDBC 驱动名及数据库 URL
     */
    private Connection conn;

    MysqlConnector() {

        String dbUrl = "jdbc:mysql://10.11.6.121:13306/touchStoneTest?" +
                "useSSL=false&" +
                "allowPublicKeyRetrieval=true&" +
                "allowLoadLocalInfile=true&" +
                "serverTimezone=UTC";

        // 数据库的用户名与密码
        String user = "root";
        String pass = "root";

        try {
            conn = DriverManager.getConnection(dbUrl, user, pass);
            System.out.println("连接成功");
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("无法建立数据库连接");
            System.exit(-1);
        }
    }

    public void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void computeNum(String sql) {
        System.out.println(sql);
        try {
            ResultSet rs = conn.createStatement().executeQuery(sql);
            rs.next();
            int num = rs.getInt(1);
            System.out.println(num);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    private void executeSql(String sql) throws SQLException {
        conn.createStatement().execute(sql);
    }

    //表格相关操作


    void loadData(int threadNum) throws SQLException {
        for (int i = 0; i < threadNum; i++) {
            String sql = "load data CONCURRENT LOCAL INFILE 'data/part_" + i + ".txt' into table PART COLUMNS TERMINATED BY ',' ";
            executeSql(sql);
        }
        for (int i = 0; i < threadNum; i++) {
            String sql = "load data CONCURRENT LOCAL INFILE 'data/product_" + i + ".txt' into table PRODUCT COLUMNS TERMINATED BY ',' ";
            executeSql(sql);
        }
    }

    /**
     * 在本项目中表的命名都用t开头，因此我们从t0开始删除指定数量的表，
     * 来进行本次执行的初始化
     */
    void createTables() throws SQLException {
        String sql = "DROP TABLE IF EXISTS PRODUCT";
        executeSql(sql);
        sql = "DROP TABLE IF EXISTS PART";
        executeSql(sql);
        sql = "create table PART(P_PARTID int PRIMARY KEY,SUPPLIER VARCHAR(8),TYPE VARCHAR(8),STOCK INTEGER);";
        executeSql(sql);
        sql = "create table PRODUCT(PRODUCTID int PRIMARY KEY,PR_PARTID int,KIND VARCHAR(8)," +
                "TOTALSOLD INT,foreign key (PR_PARTID) references PART(P_PARTID));";
        executeSql(sql);
    }
}
