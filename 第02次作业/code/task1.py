from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Task1").setMaster("local[*]")
sc = SparkContext(conf=conf)

person_list = [
    ("xiaoming1", "男", 18),
    ("xiaohua1", "女", 20),
    ("xiaomeng", "男", 18),
    ("xiaoli1", "女", 19),
    ("xiaoming2", "男", 22),
    ("xiaoli2", "女", 17),
    ("xiaoming4", "男", 28)
]

rdd = sc.parallelize(person_list)

print("1. 原始数据：")
print(rdd.collect())

# (2) 年龄变成原来的3倍
rdd_age3 = rdd.map(lambda x: (x[0], x[1], x[2] * 3))
print("\n2. 年龄变成3倍：")
print(rdd_age3.collect())

# (3) 过滤年龄小于20的
rdd_age20 = rdd.filter(lambda x: x[2] >= 20)
print("\n3. 过滤年龄小于20的：")
print(rdd_age20.collect())

# (4) 把性别为女的过滤掉
rdd_male = rdd.filter(lambda x: x[1] != "女")
print("\n4. 过滤女性：")
print(rdd_male.collect())

# (5) 计算所有人的总年龄
total_age = rdd.map(lambda x: x[2]).reduce(lambda x, y: x + y)
print("\n5. 所有人的总年龄：")
print(total_age)

# (6) 按年龄从小到大排序
rdd_asc = rdd.sortBy(lambda x: x[2], ascending=True)
print("\n6. 按年龄从小到大排序：")
print(rdd_asc.collect())

# (7) 按年龄从大到小排序
rdd_desc = rdd.sortBy(lambda x: x[2], ascending=False)
print("\n7. 按年龄从大到小排序：")
print(rdd_desc.collect())

# (8) 姓名、性别、年龄合成一个输出
rdd_info = rdd.map(lambda x: f"{x[0]} {x[1]} {x[2]}")
print("\n8. 姓名性别年龄合成输出：")
for item in rdd_info.collect():
    print(item)

# (9) 奇数年龄和偶数年龄分开
rdd_odd = rdd.filter(lambda x: x[2] % 2 != 0)
rdd_even = rdd.filter(lambda x: x[2] % 2 == 0)

print("\n9. 年龄为奇数的人：")
print(rdd_odd.collect())

print("年龄为偶数的人：")
print(rdd_even.collect())

sc.stop()