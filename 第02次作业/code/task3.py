from pyspark import SparkConf, SparkContext
from pathlib import Path

conf = SparkConf().setAppName("Task3").setMaster("local[*]")
sc = SparkContext(conf=conf)

base_dir = Path(__file__).resolve().parent.parent
data_dir = base_dir / "data"

file1_path = f"file://{data_dir / 'file1.txt'}"
file2_path = f"file://{data_dir / 'file2.txt'}"
file3_path = f"file://{data_dir / 'file3.txt'}"

file1 = sc.textFile(file1_path)
file2 = sc.textFile(file2_path)
file3 = sc.textFile(file3_path)

rdd = file1.union(file2).union(file3)

number_rdd = (
    rdd.filter(lambda x: x.strip() != "")
       .map(lambda x: int(x.strip()))
)

sorted_rdd = number_rdd.sortBy(
    lambda x: x,
    ascending=True
)

result_rdd = (
    sorted_rdd.zipWithIndex()
              .map(lambda x: (x[1] + 1, x[0]))
)
output_dir = base_dir / "output_task3"

if output_dir.exists():
    import shutil
    shutil.rmtree(output_dir)

result_rdd.map(lambda x: f"{x[0]}\t{x[1]}") \
          .coalesce(1) \
          .saveAsTextFile(f"file://{output_dir}")
print("\n========== 排序结果 ==========")

for rank, value in result_rdd.collect():
    print(f"{rank}\t{value}")

print("==============================\n")

sc.stop()