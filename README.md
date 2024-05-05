<p align="center">
<img src="https://github.com/xiaoxuxiansheng/goredis/blob/main/img/frame.png" />
<b>goredis: 基于 go 实现 redis</b>
<br/><br/>
</p>

## 📚 前言
笔者在学习 goredis 实现方案的过程中，在很大程度上借鉴了 godis 项目，在此特别致敬一下作者.
附上传送门：https://github.com/HDT3213/godis/

## 📖 简介
本着学习和实践的目标, 基于 100% 纯度 go 语言实现的 “低配仿制” redis ，实现到的功能点包括：
- tcp服务端搭建 
    - 基于go自带netpoller实现io多路复用
    - 还原redis数据解析协议
- 常规数据类型与操作指令支持
    - string——get/mget/set/mset
    - list——lpush/lpop/rpush/rpop/lrange
    - set——sadd/sismember/srem
    - hashmap——hset/hget/hdel
    - sortedset——zadd/zremzrangebyscore
- 数据持久化机制
    - appendonlyfile落盘与重写

## 💡 `goredis` 技术原理及源码实现
<a href="https://mp.weixin.qq.com/s?__biz=MzkxMjQzMjA0OQ==&mid=2247485070&idx=1&sn=e6fc425dee04746648dfb0bc4c3b2313">基于go实现redis之主干框架</a> <br/><br/>
<a href="https://mp.weixin.qq.com/s?__biz=MzkxMjQzMjA0OQ==&mid=2247485081&idx=1&sn=68319157e3e6cd3133738ceda4e41872">基于go实现redis之指令分发</a> <br/><br/>
<a href="https://mp.weixin.qq.com/s?__biz=MzkxMjQzMjA0OQ==&mid=2247485098&idx=1&sn=72a09e4d91fa3c5937cd56086ba7a80a">基于go实现redis之存储引擎</a> <br/><br/>
<a href="https://mp.weixin.qq.com/s?__biz=MzkxMjQzMjA0OQ==&mid=2247485113&idx=1&sn=4a7945138c43c6ec41a2f933e61642c8">基于go实现redis之数据持久化</a> <br/><br/>

## 💻 核心架构
<b>服务端与指令分发层</b>
<img src="https://github.com/xiaoxuxiansheng/goredis/blob/main/img/logic.png" />
<b>数据存储引擎层</b>
<img src="https://github.com/xiaoxuxiansheng/goredis/blob/main/img/database.png" />
