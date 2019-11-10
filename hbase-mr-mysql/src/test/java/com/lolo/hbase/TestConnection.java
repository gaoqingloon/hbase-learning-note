package com.lolo.hbase;

import java.sql.Connection;
import java.sql.DriverManager;

public class TestConnection {

    public static void main(String[] args) throws Exception {

        String className = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://hadoop104:3306/test?useSSL=false";
        String user = "root";
        String password = "123456";

        // 2. 加载驱动
        Class.forName(className);

        // 3. 获取连接
        Connection conn = DriverManager.getConnection(url, user, password);
        System.out.println(conn);

        /*
WARN: Establishing SSL connection without server's identity verification
 is not recommended. According to MySQL 5.5.45+, 5.6.26+ and 5.7.6+
 requirements SSL connection must be established by default if explicit
 option isn't set. For compliance with existing applications not using SSL
 the verifyServerCertificate property is set to 'false'. You need either to
 explicitly disable SSL by setting useSSL=false, or set useSSL=true and provide
 truststore for server certificate verification.
         */
    }
}
