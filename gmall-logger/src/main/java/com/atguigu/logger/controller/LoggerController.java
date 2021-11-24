package com.atguigu.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author: xy_mono
 * @date: 2021/11/22
 * @description: 接收模拟器生成的数据，并对数据进行处理
 */

//@Controller   将对象的创建交给Spring容器  方法返回String,默认会当作跳转页面处理
//@RestController = @Controller +  @RequestBody  方法返回Object,底层会转换为json格式字符串进行响应
@RestController
@Slf4j
public class LoggerController {

    //Spring提供的对kafka的支持
    @Autowired   //将KafkaTemplate注入到Controller中
    KafkaTemplate kafkaTemplate;

    //    提供一个方法，处理模拟器生成的数据.但是类中有许多方法，如何知道是哪个方法，因此需要注解
    //     @RequestMapping("/applog") 作用是把applog请求交给方法进行处理
    //      @RequestBody 表示的是从请求体中获取数据
    //      mockLog是模拟器源码中的，代表模拟器日志
    @RequestMapping("/applog")
    public String applog(@RequestBody String mockLog) {
//        System.out.println(mockLog);
        //落盘
        log.info(mockLog);
        //根据日志的类型，发送到kafka的不同主题中去
        //将接收到的字符串数据转换为json对象
        JSONObject jsonObject = JSON.parseObject(mockLog);
        JSONObject startJson = jsonObject.getJSONObject("start");
        if(startJson != null){
            //启动日志
            kafkaTemplate.send("gmall_start_1122",mockLog);
        }else{
            //事件日志
            kafkaTemplate.send("gmall_event_1122",mockLog);
        }
        return "success";
    }
}
