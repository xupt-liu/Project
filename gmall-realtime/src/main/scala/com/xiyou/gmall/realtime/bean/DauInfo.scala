package com.xiyou.gmall.realtime.bean

/**
 * @author: xy_mono
 * @date: 2021/12/14
 * @description:样例类，用于封装日志数据，格式取决于处理后的日志格式
 */

case class DauInfo(
                    mid: String, //设备 id
                    uid: String, //用户 id
                    ar: String, //地区
                    ch: String, //渠道
                    vc: String, //版本
                    var dt: String, //日期
                    var hr: String, //小时
                    var mi: String, //分钟
                    ts: Long //时间戳
                  ) {}
