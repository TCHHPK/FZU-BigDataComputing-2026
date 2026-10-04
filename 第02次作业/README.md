# 第02次作业

本次作业主要使用 PySpark 的 RDD 算子完成数据处理，包括 `map`、`filter`、`reduce`、`sortBy`、`union`、`zipWithIndex`、`foreach` 等操作。

## 题目一：人员信息处理

创建人员信息 RDD，并完成以下操作：

1. 创建 `person_list`
2. 将每个人年龄变为原来的 3 倍
3. 过滤年龄小于 20 的人员
4. 过滤女性
5. 计算所有人的总年龄
6. 按年龄升序排序
7. 按年龄降序排序
8. 合并输出姓名、性别和年龄
9. 将年龄为奇数和偶数的人员分开

程序文件：

```text
code/task1.py
```

---

## 题目二：整数列表 RDD 操作

创建整数列表：

```text
6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
16, 17, 18, 19, 20, 1, 2, 3, 4, 5
```

完成以下操作：

1. 创建 RDD
2. 使用 `map` 和 `foreach` 遍历
3. 分离奇数和偶数
4. 所有元素加 10，生成 `list2`
5. 提取 `list2` 中的偶数，生成 `list3`
6. 计算 `list3` 数据总和
7. 对 `list3` 进行倒序排序
8. 对 `list3` 进行反转

程序文件：

```text
code/task2.py
```

---

## 题目三：多文件整数排序

读取以下三个文件：

```text
data/file1.txt
data/file2.txt
data/file3.txt
```

将所有整数合并后进行升序排序，并使用 `zipWithIndex` 为每个数字生成排序位次。

输出格式：

```text
位次    数值
```

示例结果：

```text
1	1
2	4
3	5
4	12
5	16
6	25
7	33
8	37
9	39
10	40
11	45
```

程序文件：

```text
code/task3.py
```

程序同时将结果保存到：

```text
output_task3/
```

---

## 题目四：学生成绩统计

读取学生成绩文件：

```text
data/score.txt
```

使用 RDD 算子分别统计：

- 全部学生各门课程的平均成绩、最低成绩、最高成绩
- 男生各门课程的平均成绩、最低成绩、最高成绩
- 女生各门课程的平均成绩、最低成绩、最高成绩

测试数据包含：

```text
Math
English
Physics
```

程序文件：

```text
code/task4.py
```

---

## 项目结构

```text
第02次作业/
├── README.md
├── code/
│   ├── task1.py
│   ├── task2.py
│   ├── task3.py
│   └── task4.py
├── data/
│   ├── file1.txt
│   ├── file2.txt
│   ├── file3.txt
│   └── score.txt
├── output_task3/
└── screenshots/
```

## 运行方式

在第02次作业目录下执行：

```bash
spark-submit code/task1.py
spark-submit code/task2.py
spark-submit code/task3.py
spark-submit code/task4.py
```
