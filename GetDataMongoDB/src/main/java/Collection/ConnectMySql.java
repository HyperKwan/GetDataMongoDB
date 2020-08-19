package Collection;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * 连接mysql数据库
 */
public class ConnectMySql {
    private static Connection conn = null;
    public static Connection getCon() {
        try {
            Class.forName("com.mysql.jdbc.Driver"); //加载数据库连接驱动
            String user = "bip";
            String psw = "Bip@data5tgb123456";  //设置数据库的密码
            String url = "jdbc:mysql://10.254.3.11:3306/bip_ip_other?characterEncoding=utf-8&rewriteBatchedStatements=true";//解决写入乱码需要添加字符集
//            String url = "jdbc:mysql://10.254.3.11:3306/test?characterEncoding=utf-8&rewriteBatchedStatements=true";//解决写入乱码需要添加字符集
            conn = DriverManager.getConnection(url, user, psw);  //获取连接
        } catch (Exception e) {
            System.out.println("连接数据库失败");
            e.printStackTrace();
        }
        return conn;
    }


    public static Connection getConTest() {
        try {
            Class.forName("com.mysql.jdbc.Driver"); //加载数据库连接驱动
            String username = "root";
            String password = "Lbx@db123456";  //设置数据库的密码
            String url = "jdbc:mysql://10.249.0.25:3306/bip_ip_yjd?characterEncoding=utf-8&rewriteBatchedStatements=true";//解决写入乱码需要添加字符集
            conn = DriverManager.getConnection(url, username, password);  //获取连接
        } catch (Exception e) {
            System.out.println("连接数据库失败");
            e.printStackTrace();
        }
        return conn;
    }
}

