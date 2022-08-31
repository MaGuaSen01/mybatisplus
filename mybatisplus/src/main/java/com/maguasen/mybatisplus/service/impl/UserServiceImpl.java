package com.maguasen.mybatisplus.service.impl;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.maguasen.mybatisplus.entity.User;
import com.maguasen.mybatisplus.mapper.UserMapper;
import com.maguasen.mybatisplus.service.UserService;
import org.springframework.stereotype.Service;

/**
 * @author
 * @version 1.0
 * @date 2022/8/16 20:16
 */
@DS("master")
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements UserService {

}
