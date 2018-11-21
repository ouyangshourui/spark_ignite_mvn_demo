package com.zhw.bigdata.ignite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class IgniteDriver {
    public static void main(String[] args)  throws  Exception{
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://172.172.240.122");
        String sql1 = "copy from \"/Users/ouyangshourui/code/ignite/src/main/resources/event_10001_user.csv\" INTO event_10001_user (_ftenancy_id , _fid , fsbabyage , fsbirthday , fuserlevel , fbabynum ,   fbbirthday ,fbirthday , fcreatordepartment ,fearliestregtime ,fmobile ,fsex ,fssex ,ftruename , fbbabyage ,  fbsex ,      fcityname , fcommunity ,fdistrictname ,fgestationage ,  fnickname , fpromoteactive , fprovincename ,  fcreator ,  fbirthday_next ,fmemberlevel ) format csv;";
        System.out.println(sql1);
        conn.createStatement().executeUpdate(sql1);

    }
}
