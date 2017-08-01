# :rocket: [Window functions][1] :facepunch:
---
# pyspark.sql.Window
> over开窗

- Window类下的方法
    - Window.orderBy
    - Window.partitionBy
    - Window.rangeBetween: 逻辑窗口，从列值上控制窗口的尺寸
    - Window.rowsBetween: 物理窗口，从行数上控制窗口的尺寸
    - windowSpec结构（升序理解）
        - 前面：Window.unboundedPreceding起点
        - 当前：Window.currentRow
        - 后面：Window.unboundedFollowing终点

```
windowSpec = row_number().over(Window.partitionBy(col1).orderBy(col2))
# Defines a Window Specification with a ROW frame.
windowSpec.rowsBetween(start, end)
# Defines a Window Specification with a RANGE frame.
windowSpec.rangeBetween(start, end)
```
```
select url,rate,row_number() over(partition by url order by rate desc) as r from window_test;  
```

---
# Ranking functions
- rank: 遇到排名并列时，下一列排名跳空(1,2,2,4)
- dense_rank: 连续排名
- percent_rank: 介于0和1直接的小数形式表示，计算方法是(rank - 1) / (n-1)，其中rank为行的序号，n为组的行数（归一化）
- row_number
- ntile(n): 组内分成n份
```
# ntile(5) OVER (PARTITION BY 1 ORDER BY id）
spark.range(10).select(ntile(5).over(Window.partitionBy(lit(1)).orderBy('id'))).show()
+--------+
|ntile(5)|
+--------+
|       1|
|       1|
|       2|
|       2|
|       3|
|       3|
|       4|
|       4|
|       5|
|       5|
+--------+
```

- first(col, ignorenulls=False): FIRST_VALUE(hive)
- last(col, ignorenulls=False): LAST_VALUE(hive)
---
# Analytic functions
- cume_dist: 累计分布，返回低于当前行的行的百分数
```
# cume_dist() OVER (PARTITION BY 1 ORDER BY id
spark.range(10).select(cume_dist().over(Window.partitionBy(lit(1)).orderBy('id'))).show()
+---------+
|cume_dist|
+---------+
|      0.1|
|      0.2|
|      0.3|
|      0.4|
|      0.5|
|      0.6|
|      0.7|
|      0.8|
|      0.9|
|      1.0|
+---------+
```
- lag(col, count=1, default=None): 窗口网上滑动
- lead(col, count=1, default=None): 窗口往下滑动
> 第一个参数为列名，第二个参数为往上/下第n行（可选，默认为1），第三个参数为默认值（当往上/下第n行为NULL时候，取默认值，如不指定，则为NULL）

---
# Aggregate functions
```
# 广播
spark.range(10).withColumn('new', expr("id - mean(id) OVER()")).show()
+---+----+
| id| new|
+---+----+
|  0|-4.5|
|  1|-3.5|
|  2|-2.5|
|  3|-1.5|
|  4|-0.5|
|  5| 0.5|
|  6| 1.5|
|  7| 2.5|
|  8| 3.5|
|  9| 4.5|
+---+----+
```


  [1]: http://lxw1234.com/archives/tag/hive-window-functions
