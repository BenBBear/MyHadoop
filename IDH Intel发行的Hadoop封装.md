# IDH Intel发行的Hadoop封装

### 优势

- IDH的优点主要表现在        
    - 大数据复杂分析和管理能力        
        跨数据中心 HBase 大表        
        HBase 全文检索和准实时查询        
        分布式 R 语言支持            
    - 大数据高速即时分析能力            
        高性能 HBase 查询汇总引擎            
        HBase 的 HiveQL 支持            
        自动 HDFS 热点数据复制倍数增加            
        高级集群负载均衡能力            
    - 大数据平台管理能力        
        图形化安装配置部署工具                
        集中式图形化监控报警功能            
        SmartTuner 动态集群优化工具            

### 使用的优化

- 针对MapReduce进行参数优化及作业剖析来动态优化集群提高性能

- MapReduce作业剖析提供以下信息：
    - 任务完成需要的时间
    - 对于每个任务：
        - 创建任务所耗时间
        - Map 任务所耗时间
        - Reduce 任务所耗时间
        - Cleanup 任务所耗时间
    - 对于作业中的具体任务：
        - 任务完成所需时间
        - 任务在哪个节点上运行
        - 任务是Map 任务还是Reduce 任务
        - 如果是Map 任务，则Map 任务需要多长时间完成
        - 如果是Reduce 任务，则Reduce 任务需要多长时间完成
        - 报告阶段需要多长时间完成
        - 如果任务包含Shuffle 阶段，则Shuffle 阶段需要多长时间完成
        - 如果任务包含Sort 阶段，则Sort 阶段需要多长时间完成
        - 如果任务包含Spill 阶段，则Spill 阶段需要多长时间完成
        - 这是否是一个Cleanup 任务，如果是，Cleanup 任务需要多长时间完成

- 使用可统计的机器学习方法来获得一系列MapReduce 参数推荐值。工具主要使用以下参数：
    - Map slots、 Reduce Slots、job compression 和map compression 的完成时间
    - Reducers、io.sort.mb、job compression 和map compression 的完成时间

- 使用某种搜索运算法则来获得参数空间。运算法则根据以下方式工作：
    1. 运算法则选择将用于运行MapReduce 应用程序的某一系列属性值。这一系列属性值称为一次迭代。MapReduce 应用程序用这些参数来运行的次数成为一次尝试。迭代可以为多次，每次迭代有不同的一系列属性值，每次迭代有多次尝试。
    2. 在一个或多个尝试后，运算法则将动态地运用一个迭代的尝试结果来反应性地生成一个会返回更好的完成时间的新迭代。
    3. 根据多次迭代和尝试，运算法则无须实际运行尝试，就能模拟其他迭代的尝试结果。智能优化可模拟系统探测参数空间，以节省资源开销和实际运行成百上千次尝试所需的时间。
    4. 在运行多次迭代和尝试后，运算法则将返回一个最好的配置。最好的配置是能达到最佳完成时间的一系列属性值：Reducers、 io.sort.mb、Map slots、Reduce Slots、job compression 和map compression。

### 不足

    只支持Hadoop1.x