import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class runDataOnDatabase {
    public static void main(String[] args) {
        MysqlConnector mysqlConnector = new MysqlConnector();
        try {
            mysqlConnector.createTables();
            mysqlConnector.loadData(2);
            String sql1;
            sql1="select count(*) from R join S on R.R0=S.S1 where R.R1>700";
            mysqlConnector.computeNum(sql1);
            sql1="select count(*) from R join S on R.R0=S.S1 where R.R2>600 and S.S2<40000";
            mysqlConnector.computeNum(sql1);
            sql1="select count(*) from R left join S on R.R0=S.S1 where R.R2>600 and S.S0 is Null";
            mysqlConnector.computeNum(sql1);
            sql1="select count(*) from R join S on R.R0=S.S1 where R.R1<700";
            mysqlConnector.computeNum(sql1);
            sql1="select count(*) from R left join S on R.R0=S.S1 where R.R1<700 and S.S0 is Null";
            mysqlConnector.computeNum(sql1);
            sql1="select count(*) from R join S on R.R0=S.S1 and R.R1 where R.R1>564 and r3<'1995-1-6'";
            mysqlConnector.computeNum(sql1);
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


    void executeSql(String sql) throws SQLException {
        conn.createStatement().execute(sql);
    }

    //表格相关操作


    public void loadData(int threadNum) throws SQLException {
        for (int i = 0; i < threadNum; i++) {
            String sql = "load data CONCURRENT LOCAL INFILE 'data/r_" + i + ".txt' into table R COLUMNS TERMINATED BY ',' ";
            executeSql(sql);
        }
        for (int i = 0; i < threadNum; i++) {
            String sql = "load data CONCURRENT LOCAL INFILE 'data/s_" + i + ".txt' into table S COLUMNS TERMINATED BY ',' ";
            executeSql(sql);
        }
    }

    /**
     * 在本项目中表的命名都用t开头，因此我们从t0开始删除指定数量的表，
     * 来进行本次执行的初始化
     */
    public void createTables() throws SQLException {
        String sql = "DROP TABLE IF EXISTS S";
        executeSql(sql);
        sql = "DROP TABLE IF EXISTS R";
        executeSql(sql);
        sql = "create table R(R0 int PRIMARY KEY,R1 int,R2 int,R3 datetime,R4 VARCHAR(25));";
        executeSql(sql);
        sql = "create table S(S0 int PRIMARY KEY,S1 int,S2 int,foreign key (S1) references R(R0));";
        executeSql(sql);
    }
}
