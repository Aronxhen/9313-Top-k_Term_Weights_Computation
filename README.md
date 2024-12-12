# 9313 Top-k Term Weights Computation 大规模文本数据分析

### 项目组成：
- MapReduce 文件夹 
  - project1.py (MR 源代码)
  - 其他测试文件
- DataFrame and RDD 文件夹 
  - project2_rdd.py (RDD 源代码)
  - project2_df.py （DF 源代码）
  - 其他测试文件
 
### 测试项目指令：
1.配置hdfs环境  
2.运行测试指令  
`python3 project1.py -r hadoop input_file -o hdfs_output --jobconf myjob.settings.k=2 --jobconf mapreduce.job.reduces=2`
参数：
input_file 输入文件
hdfs_output 输出文件，可改为本地
myjob.settings.k top-k参数
mapreduce.job.reduces reducer数量
