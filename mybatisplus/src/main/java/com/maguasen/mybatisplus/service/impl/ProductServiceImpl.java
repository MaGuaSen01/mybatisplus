package com.maguasen.mybatisplus.service.impl;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.maguasen.mybatisplus.entity.Product;
import com.maguasen.mybatisplus.mapper.ProductMapper;
import com.maguasen.mybatisplus.service.ProductService;
import org.springframework.stereotype.Service;

/**
 * @author 3golden
 * @Function
 * @date 2022/8/29 11:25
 */
@DS("slave_1")
@Service
public class ProductServiceImpl extends ServiceImpl<ProductMapper, Product> implements ProductService {
}
