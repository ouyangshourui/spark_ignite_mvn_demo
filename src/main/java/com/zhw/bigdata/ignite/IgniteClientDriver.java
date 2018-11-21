package com.zhw.bigdata.ignite;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class IgniteClientDriver {
    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.ignite.IgniteJdbcDriver");
        Connection conn = DriverManager.getConnection("jdbc:ignite:cfg://" +
                "file:///Users/ouyangshourui/code/ignite/src/main/resources/client.xml");


        String sql1 = "COPY FROM \"file:////Users/ouyangshourui/code/ignite/src/main/resources/event_10001_user.csv\" INTO event_10001_user (_ftenancy_id , _fid , fsbabyage , fsbirthday , fuserlevel , fbabynum ,   fbbirthday ,fbirthday , fcreatordepartment ,                    fearliestregtime ,fmobile ,   fsex ,       fssex ,      ftruename , fbbabyage ,  fbsex ,      fcityname , fcommunity ,fdistrictname ,fgestationage ,  fnickname , fpromoteactive , fprovincename ,  fcreator ,  fbirthday_next ,fmemberlevel " + " ) FORMAT CSV;";
        conn.createStatement().execute(sql1);


    }
}
