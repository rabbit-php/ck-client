<?php

namespace OneCk;

/**
 * Class Protocol
 * @package lzc
 */
class Protocol
{
    /**
     * 客户端含义
     */
    const CLIENT_HELLO  = 0;
    const CLIENT_QUERY  = 1;
    const CLIENT_DATA   = 2;
    const CLIENT_CANCEL = 3;
    const CLIENT_PING   = 4;

    /**
     * 服务端含义
     */
    const SERVER_HELLO         = 0; // 验证登录 获取服务器版本信息
    const SERVER_DATA          = 1; // 数据块
    const SERVER_EXCEPTION     = 2; // 异常信息
    const SERVER_PROGRESS      = 3; // 执行信息
    const SERVER_PONG          = 4; // 回复ping
    const SERVER_END_OF_STREAM = 5; // 发送信息结束标识
    const SERVER_PROFILE_INFO  = 6; // 分析信息
    const SERVER_TOTALS        = 7; // 总数
    const SERVER_EXTREMES      = 8; //
    const SERVER_TABLESSTATUSRESPONSE = 9;    /// Response to TableStatus.
    const SERVER_LOG                  = 10;   /// Query execution log.
    const SERVER_TABLECOLUMNS         = 11;   /// Columns' description for default values calculation
    const SERVER_PARTUUIDS            = 12;   /// List of unique parts ids.
    const SERVER_READTASKREQUEST      = 13;   /// String (UUID) describes a request for which next task is needed
    /// This is such an inverted logic, where server sends requests
    /// And client returns back response
    const SERVER_PROFILEEVENTS        = 14;   /// Packet with profile events from server.

    const COMPRESSION_DISABLE = 0;
    const COMPRESSION_ENABLE  = 1;
    const STAGES_COMPLETE     = 2;
}
