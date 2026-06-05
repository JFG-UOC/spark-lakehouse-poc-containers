import random
import sys
from operator import add

from pyspark.sql import SparkSession


def sample_point(_: int) -> int:
    x = random.random()
    y = random.random()
    return 1 if x * x + y * y <= 1.0 else 0


def main() -> None:
    slices = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    points_per_slice = int(sys.argv[2]) if len(sys.argv) > 2 else 100000
    total_points = slices * points_per_slice

    spark = (
        SparkSession.builder
        .appName("Spark Pi Python Test")
        .getOrCreate()
    )

    sc = spark.sparkContext

    print("=" * 80)
    print("Spark Pi Python Test")
    print(f"Application ID: {sc.applicationId}")
    print(f"Master: {sc.master}")
    print(f"Default parallelism: {sc.defaultParallelism}")
    print(f"Slices: {slices}")
    print(f"Points per slice: {points_per_slice}")
    print(f"Total points: {total_points}")
    print("=" * 80)

    count = (
        sc.parallelize(range(total_points), slices)
        .map(sample_point)
        .reduce(add)
    )

    pi = 4.0 * count / total_points

    print("=" * 80)
    print(f"Points inside circle: {count}")
    print(f"Estimated Pi: {pi}")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()