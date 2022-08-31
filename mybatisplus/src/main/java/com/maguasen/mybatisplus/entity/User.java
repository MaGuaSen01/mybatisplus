package com.maguasen.mybatisplus.entity;


import com.baomidou.mybatisplus.annotation.TableLogic;
import lombok.*;

import java.io.Serializable;

/**
 * @author xiaojie
 * @version 1.0
 * @description: TODO
 * @date 2022/8/15 22:12
 */
@Data
public class User implements Serializable {
    private Long id;

    private String name;

    private Integer age;

    private String email;

    @TableLogic
    private Integer isDelete;
}
