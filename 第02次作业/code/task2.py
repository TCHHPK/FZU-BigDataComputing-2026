from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Task2").setMaster("local[*]")
sc = SparkContext(conf=conf)

# (1) 创建列表
data = [6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        16, 17, 18, 19, 20, 1, 2, 3, 4, 5]

rdd = sc.parallelize(data)

print("1. 原始列表：")
print(rdd.collect())

# (2) 使用 map 方法和 foreach 方法遍历
print("\n2. 使用 map 遍历：")
map_result = rdd.map(lambda x: x)
print(map_result.collect())

print("使用 foreach 遍历：")
rdd.foreach(lambda x: print(x))

# (3) 奇数偶数分开打印
odd_rdd = rdd.filter(lambda x: x % 2 != 0)
even_rdd = rdd.filter(lambda x: x % 2 == 0)

print("\n3. 奇数：")
print(odd_rdd.collect())

print("偶数：")
print(even_rdd.collect())

# (4) 每个数据 +10 产生 list2
list2 = rdd.map(lambda x: x + 10)

print("\n4. list2：")
print(list2.collect())

# (5) 取出 list2 中的偶数产生 list3
list3 = list2.filter(lambda x: x % 2 == 0)

print("\n5. list3：")
print(list3.collect())

# (6) 计算 list3 总和
total = list3.reduce(lambda x, y: x + y)

print("\n6. list3 总和：")
print(total)

# (7) list3 倒序排序
list3_desc = list3.sortBy(lambda x: x, ascending=False)

print("\n7. list3 倒序排序：")
print(list3_desc.collect())

# (8) list3 反转
list3_reverse = list3.collect()[::-1]

print("\n8. list3 反转：")
print(list3_reverse)

sc.stop()