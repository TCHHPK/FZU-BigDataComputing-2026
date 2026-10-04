# 大数据计算第01次作业

## 一、实验环境

- 操作系统：macOS
- Java：Temurin JDK 17
- Hadoop：3.4.2
- 运行模式：单机伪分布式
- 计算框架：MapReduce
- 文件系统：HDFS

---

## 二、题目一：统计每个年份收藏量最大的商品

### 1. 题目描述

现有电商网站数据文件 `favorite.txt`，每条记录包含：

- 用户 ID
- 商品 ID
- 收藏日期

要求使用 MapReduce 统计每个年份收藏量最大的商品。

数据格式：

```text
用户ID 商品ID 收藏日期
```

示例：

```text
10001 100001 2010-04-01 09:15:22
10001 100005 2010-04-02 10:42:13
```

### 2. 实现思路

该题使用两个 MapReduce Job 完成。

#### Job 1：统计每年每个商品的收藏次数

Mapper 读取每条收藏记录，提取年份和商品 ID。

Mapper 输出：

```text
<年份, 商品ID>    1
```

Reducer 对相同年份和商品 ID 的记录进行累加，得到：

```text
年份    商品ID    收藏次数
```

对应程序：

```text
FavoriteCount.java
```

#### Job 2：找出每个年份收藏量最大的商品

第二个 Job 读取第一个 Job 的统计结果。

Mapper 以年份作为 Key，以商品 ID 和收藏次数作为 Value：

```text
年份    商品ID,收藏次数
```

Reducer 对同一年的所有商品进行比较，找出最大收藏次数。

如果多个商品收藏次数相同且都为最大值，则全部输出。

对应程序：

```text
FavoriteMax.java
```

### 3. 第一阶段结果

部分结果如下：

```text
2010    100001    11
2010    100002    10
2010    100003    10
...
2011    100001    4
2011    100002    3
2011    100003    4
...
```

完整结果保存在：

```text
outputs/favorite/count_result.txt
```

### 4. 最终结果

```text
2010    100001    11
2011    100015    4
2011    100014    4
2011    100013    4
2011    100012    4
2011    100011    4
2011    100010    4
2011    100008    4
2011    100007    4
2011    100005    4
2011    100004    4
2011    100003    4
2011    100001    4
```

因此：

- 2010 年收藏量最大的商品为 `100001`，收藏次数为 `11`
- 2011 年最大收藏次数为 `4`，存在多个并列商品

最终结果保存在：

```text
outputs/favorite/max_result.txt
```

---

## 三、题目二：统计每门课程的最高分及对应学生

### 1. 题目描述

现有成绩文件 `score.txt`。

每条记录包含：

- 课程名称
- 学生姓名
- 多次考试成绩

不同学生的考试次数不固定。

数据格式：

```text
课程名称,学生姓名,成绩1,成绩2,...
```

示例：

```text
math,liujialing,85,86,41,75,93,42,85,75
english,huangxiaoming,85,86,41,75,93,42,85
```

要求统计每门课程的最高分以及取得最高分的学生。

### 2. 实现思路

该题使用一个 MapReduce Job 完成。

#### Mapper

Mapper 读取每一行，解析课程名称、学生姓名以及所有考试成绩。

首先求出该学生在该课程中的个人最高分。

例如：

```text
math,liujialing,85,86,41,75,93,42,85,75
```

得到：

```text
liujialing    93
```

Mapper 输出：

```text
课程名称    学生姓名,个人最高分
```

#### Reducer

Reducer 接收同一课程下所有学生的个人最高分，比较得到该课程的最高分及对应学生。

如果存在并列最高分，则可以同时输出所有最高分学生。

对应程序：

```text
ScoreMax.java
```

### 3. 最终结果

```text
algorithm    huangzitao    93
computer     chenyu        93
english      xuemei        94
math         wanghao       95
```

最终结果保存在：

```text
outputs/score/max_score_result.txt
```

---

## 四、项目目录

```text
第01次作业
├── FavoriteMax
│   ├── classes
│   ├── favorite.txt
│   ├── FavoriteCount.jar
│   └── src
│       ├── FavoriteCount.java
│       └── FavoriteMax.java
├── outputs
│   ├── favorite
│   │   ├── count_result.txt
│   │   └── max_result.txt
│   └── score
│       └── max_score_result.txt
└── ScoreMax
    ├── classes
    ├── score.txt
    ├── ScoreMax.jar
    └── src
        └── ScoreMax.java
```

---

## 五、实验总结

本次实验完成了 Hadoop 单机伪分布式环境的搭建，并使用 Java MapReduce 完成了两个数据统计任务。

第一题通过两个 MapReduce Job 实现了“先统计每年每个商品收藏次数，再求每年最大值”的处理流程。

第二题通过 Mapper 先求每个学生在一门课程中的个人最高分，再由 Reducer 比较得到课程最高分及对应学生。

通过本次实验进一步理解了 Map、Shuffle、Reduce 三个阶段的数据处理流程，以及 HDFS 与 YARN 在 MapReduce 程序运行过程中的作用。
