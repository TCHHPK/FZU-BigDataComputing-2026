from pyspark import SparkConf, SparkContext
from pathlib import Path

conf = SparkConf().setAppName("Task4").setMaster("local[*]")
sc = SparkContext(conf=conf)

base_dir = Path(__file__).resolve().parent.parent
score_path = f"file://{base_dir / 'data' / 'score.txt'}"

rdd = sc.textFile(score_path)

# 读取表头
header = rdd.first().split()
courses = header[2:]

# 去掉表头
data_rdd = rdd.filter(lambda line: not line.startswith("Id"))

# 解析数据
student_rdd = data_rdd.map(lambda line: line.split()) \
                      .map(lambda x: (x[1], list(map(float, x[2:]))))

def calc_stats(input_rdd):
    result = []

    for i, course in enumerate(courses):
        score_rdd = input_rdd.map(lambda x: x[1][i])

        total = score_rdd.reduce(lambda a, b: a + b)
        count = score_rdd.count()
        avg = total / count
        min_score = score_rdd.min()
        max_score = score_rdd.max()

        result.append((course, avg, min_score, max_score))

    return result


# 全部学生
all_stats = calc_stats(student_rdd)

# 男生
male_rdd = student_rdd.filter(lambda x: x[0] == "male")
male_stats = calc_stats(male_rdd)

# 女生
female_rdd = student_rdd.filter(lambda x: x[0] == "female")
female_stats = calc_stats(female_rdd)


print("\ncourse    average   min   max")
for course, avg, min_score, max_score in all_stats:
    print(f"{course:<10}{avg:8.2f}{min_score:8.2f}{max_score:8.2f}")

print("\ncourse    average   min   max (males)")
for course, avg, min_score, max_score in male_stats:
    print(f"{course:<10}{avg:8.2f}{min_score:8.2f}{max_score:8.2f}")

print("\ncourse    average   min   max (females)")
for course, avg, min_score, max_score in female_stats:
    print(f"{course:<10}{avg:8.2f}{min_score:8.2f}{max_score:8.2f}")

sc.stop()