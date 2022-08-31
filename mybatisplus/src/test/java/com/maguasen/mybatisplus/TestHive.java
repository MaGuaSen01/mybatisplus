package com.maguasen.mybatisplus;

import com.maguasen.mybatisplus.hive.RestfulTest;
import org.junit.jupiter.api.Test;

/**
 * @author 3golden
 * @Function
 * @date 2022/8/26 14:31
 */
public class TestHive {
    String http="http://10.0.7.59:12345";
    String token="3fdeafff1b25bf7947bfe24c6aadd471";
    String projectName="data-quality-v4.0";
    @Test
    public void test01() {
        String result = RestfulTest.get_status(http, token, projectName ,"" ,
                null , null ,"", "DAG/2022-08-16/3460-0-1661443201257" , null, null);
        System.out.println("result : " + result);
    }

}
