# Lab4

## JAR包执行方式说明

任务一：

```bash
hadoop jar KNN-1.0-SNAPSHOT.jar KNN <k> <tarin> <test> <output>
```

OJ平台使用示例

```bash
yarn jar KNN-1.0-SNAPSHOT.jar KNN 3 /data/exp4/iris_train.csv /data/exp4/iris_test.csv /user/2023stu_13/lab4-1out
```

任务二：

```bash
hadoop jar KNN-1.0-SNAPSHOT.jar WKNN <k> <tarin> <test> <output>
```

OJ平台使用示例

```bash
yarn jar KNN-1.0-SNAPSHOT.jar WKNN 3 /data/exp4/iris_train.csv /data/exp4/iris_test.csv /user/2023stu_13/lab4-2out
```
