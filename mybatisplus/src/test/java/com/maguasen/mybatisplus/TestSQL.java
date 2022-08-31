package com.maguasen.mybatisplus;

import com.maguasen.mybatisplus.entity.User;
import com.maguasen.mybatisplus.mapper.UserMapper;
import com.maguasen.mybatisplus.service.ProductService;
import com.maguasen.mybatisplus.service.UserService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author 3golden
 * @Function
 * @date 2022/8/26 14:34
 */
@SpringBootTest
public class TestSQL {
    @Autowired
    private UserMapper userMapper;

    @Autowired
    private UserService userService;

    @Autowired
    private ProductService productService;

    @Test
    public void test01() {
        User user = new User();
        user.setName("zhangsan");
        user.setAge(29);
        user.setEmail("zhangsan@163.com");
        boolean isSave = userService.save(user);
        System.out.println("isSave:" + isSave);
    }

    @Test
    public void test02() {
        System.out.println(userService.getById(1L));
        System.out.println(productService.getById(1L));
    }
}
