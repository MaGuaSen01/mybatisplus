package com.maguasen.mybatisplus.entity;

import lombok.Data;

/**
 * @author 3golden
 * @Function
 * @date 2022/8/21 23:57
 */
@Data
public class Product {

    private Long id;

    private String name;

    private Integer price;

    private Integer version;

}
