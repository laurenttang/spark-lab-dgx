"""spark101 / avg_temperature  (lesson 0718 "AverTemperature_By_steps")

Get the average temperature per day from (big) weather data, step by step,
exactly as in class: filter bad records -> (day, temp) -> (day, (temp, 1))
-> reduceByKey -> sum / count.
"""
from lab_common import COURSE, Report, get_spark


def is_good(record):
    try:
        int(record.split(",")[10])
    except (ValueError, IndexError):
        return False
    return True


if __name__ == "__main__":
    spark = get_spark("spark101 avg_temperature")
    sc = spark.sparkContext
    rep = Report("每日平均溫度（NOAA 2007/01 逐時觀測）")

    with rep.step("load + filter good records"):
        records = sc.textFile(f"{COURSE}/spark101/avg_temperature/data/200701hourly_small.txt")
        good_records = records.filter(is_good)
        n_all, n_good = records.count(), good_records.count()

    with rep.step("map -> reduceByKey -> mapValues"):
        day_temp = good_records.map(lambda x: (x.split(",")[1], int(x.split(",")[10])))
        temp1 = day_temp.mapValues(lambda x: (x, 1))
        temp2 = temp1.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
        result = temp2.mapValues(lambda pair: pair[0] / pair[1])
        result_list = sorted(result.collect())

    rep.metric("原始筆數", n_all)
    rep.metric("有效筆數", n_good)
    rep.metric("被濾掉（缺值）", n_all - n_good)
    rep.metric("天數", len(result_list))
    rep.chart("每日平均溫度 (°F)", "line",
              [f"{d[4:6]}/{d[6:]}" for d, _ in result_list],
              {"平均溫度": [round(t, 2) for _, t in result_list]})
    rep.table("day, avg_temp", [(d, round(t, 3)) for d, t in result_list], ["day", "avg_temp"])
    rep.save()
    spark.stop()
