import java.sql.*;

public class runDataOnDatabase {
    public static void main(String[] args) {
        MysqlConnector mysqlConnector=new MysqlConnector();
        try {
            mysqlConnector.createTables();
            mysqlConnector.loadData();
            mysqlConnector.executeRJoinS(1,700);
            mysqlConnector.executeRJoinS(2,600);
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

    public MysqlConnector() {

        String dbUrl = "jdbc:mysql://biui.me/runForTidb?useSSL=false&allowPublicKeyRetrieval=true";

        // 数据库的用户名与密码
        String user = "qswang";
        String pass = "Biui1227..";
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

    void executeRJoinS(int index , double value) throws SQLException {
        String sql="select count(*) from R left join S on R.R0=S.S1 where R.R"+index+">"+value+" and S.S0 is null";
        System.out.println(sql);
        ResultSet rs=conn.createStatement().executeQuery(sql);
        rs.next();
        int num=rs.getInt(1);
        sql="select count(*) from R where R.R"+index+">"+value;
        rs=conn.createStatement().executeQuery(sql);
        rs.next();
        int num2=rs.getInt(1);
        System.out.println(num+" "+num2);
    }

    void executeSql(String sql) throws SQLException {
        conn.createStatement().execute(sql);
    }

    //表格相关操作


    public void loadData() throws SQLException {
        String sql = "load data CONCURRENT LOCAL INFILE 'data/r_0.txt' into table R COLUMNS TERMINATED BY ',' ";
        executeSql(sql);
        sql = "load data CONCURRENT LOCAL INFILE 'data/s_0.txt' into table S COLUMNS TERMINATED BY ',' ";
        executeSql(sql);
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
        sql="create table R(R0 int PRIMARY KEY,R1 int,R2 int);";
        executeSql(sql);
        sql="create table S(S0 int PRIMARY KEY,S1 int,foreign key (S1) references R(R0));";
        executeSql(sql);
    }
}
