# Hadoop项目流程类与关键行号映射

本文档基于HADOOP_PROJECT_FLOW_DOC.md，系统地标注了Hadoop项目中各主要流程的相关类和关键代码位置。

## 1. HDFS相关流程

### 1.1 HDFS启动流程

#### NameNode启动流程
- **核心类**：`org.apache.hadoop.hdfs.server.namenode.NameNode`
- **主要方法**：
  - `main(String[] args)`：程序入口点
  - `createNameNode(String argv[], Configuration conf)`：创建NameNode实例
  - `initialize(Configuration conf)`：初始化NameNode服务
  - `startCommonServices(Configuration conf)`：启动公共服务

#### DataNode启动流程
- **核心类**：`org.apache.hadoop.hdfs.server.datanode.DataNode`
- **主要方法**：
  - `main(String[] args)`：程序入口点
  - `runDatanodeDaemon(DatanodeContext context)`：运行DataNode守护进程
  - `startDataNode(Configuration conf, SecureResources resources)`：启动DataNode
  - `initIpcServer()`：初始化IPC服务器
  - `initStorage(NamespaceInfo nsInfo)`：初始化数据存储

### 1.2 HDFS文件读写流程

#### 文件写入流程
- **核心类**：
  - `org.apache.hadoop.hdfs.DFSClient`
  - `org.apache.hadoop.hdfs.server.namenode.FSNamesystem`
  - `org.apache.hadoop.hdfs.server.datanode.DataNode`
- **主要方法**：
  - `DFSClient.create(String src, FsPermission permission, boolean overwrite, boolean createParent, short replication, long blockSize, Progressable progress)`：创建文件
  - `DataNode.createBlockOutputStream(DFSOutputStream dfsOutput, LocatedBlock locatedBlock)`：创建数据块输出流
  - `DataNode.transferData(OutputRamDisk, Token, long, String, String)`：传输数据

#### 文件读取流程
- **核心类**：
  - `org.apache.hadoop.hdfs.DFSClient`
  - `org.apache.hadoop.hdfs.server.namenode.FSNamesystem`
  - `org.apache.hadoop.hdfs.server.datanode.DataNode`
- **主要方法**：
  - `DFSClient.open(String src, int bufferSize)`：打开文件
  - `DataNode.transferTo(long blockId, long generationStamp, String target, OutputStream out, long offset, long length, boolean verifyChecksum)`：传输数据块

### 1.3 数据节点存储管理
- **核心类**：`org.apache.hadoop.hdfs.server.datanode.DataNode`
- **主要方法**：
  - `initStorage(NamespaceInfo nsInfo)`：初始化数据存储
  - `parseChangedVolumes(String[] oldDirs, String[] newDirs)`：解析更改的存储卷
  - `getXferAddress()`：获取数据传输地址
  - `getInfoAddr()`：获取信息服务器地址

## 2. YARN相关流程

### 2.1 YARN启动流程

#### ResourceManager启动流程
- **核心类**：`org.apache.hadoop.yarn.server.resourcemanager.ResourceManager`
- **主要方法**：
  - `main(String[] args)`：程序入口点
  - `serviceInit(Configuration conf)`：初始化服务
  - `serviceStart()`：启动服务
  - `createAndStartActiveServices()`：创建并启动活动服务

#### NodeManager启动流程
- **核心类**：`org.apache.hadoop.yarn.server.nodemanager.NodeManager`
- **主要方法**：
  - `main(String[] args)`：程序入口点
  - `serviceInit(Configuration conf)`：初始化服务
  - `serviceStart()`：启动服务
  - `initializeAndStartComponents()`：初始化并启动组件

### 2.2 资源分配与调度流程

#### 资源请求与分配
- **核心类**：
  - `org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler`
  - `org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer`
- **主要方法**：
  - `allocate(ApplicationAttemptId appAttemptId, List<ResourceRequest> ask, List<ContainerId> release, float progress)`：分配资源
  - `allocateContainers(ApplicationAttemptId applicationAttemptId, List<ResourceRequest> resourceRequests)`：分配容器

#### Container生命周期管理
- **核心类**：`org.apache.hadoop.yarn.server.nodemanager.container.Container`
- **主要方法**：
  - `start()`：启动容器
  - `signalContainer(Signal signal)`：向容器发送信号
  - `exit(int exitCode)`：容器退出处理

## 3. MapReduce相关流程

### 3.1 MapReduce作业提交与执行

#### 作业提交流程
- **核心类**：
  - `org.apache.hadoop.mapreduce.Job`
  - `org.apache.hadoop.mapreduce.v2.app.MRAppMaster`
- **主要方法**：
  - `Job.submit()`：提交作业
  - `MRAppMaster.serviceInit(Configuration conf)`：初始化AppMaster
  - `MRAppMaster.serviceStart()`：启动AppMaster

#### Map任务执行流程
- **核心类**：`org.apache.hadoop.mapred.MapTask`
- **主要方法**：
  - `run(JobConf job, TaskUmbilicalProtocol umbilical)`：运行Map任务
  - `runNewMapper(JobConf job, TaskSplitIndex splitIndex, TaskUmbilicalProtocol umbilical, TaskReporter reporter)`：运行新的Mapper
  - `MapOutputBuffer.sortAndSpill()`：排序和溢出

#### Reduce任务执行流程
- **核心类**：`org.apache.hadoop.mapred.ReduceTask`
- **主要方法**：
  - `run(JobConf job, TaskUmbilicalProtocol umbilical)`：运行Reduce任务
  - `runNewReducer(JobConf job, final TaskUmbilicalProtocol umbilical, final int initPhase)`：运行新的Reducer
  - `ShuffleSchedulerImpl.scheduleShuffle(ShuffleClientMetrics metrics)`：调度Shuffle阶段

## 4. 组件交互与通信机制

### 4.1 RPC通信机制
- **核心类**：
  - `org.apache.hadoop.ipc.Server`
  - `org.apache.hadoop.ipc.Client`
- **主要方法**：
  - `Server.constructRpcServer(Class<?> protocolClass, Object protocolImpl, Configuration conf, String bindAddress, int port)`：构建RPC服务器
  - `Client.call(RPC.Call call, InetSocketAddress addr, ConnectionId remoteId)`：发起RPC调用

### 4.2 心跳机制
- **核心类**：
  - `org.apache.hadoop.hdfs.server.namenode.HeartbeatManager`
  - `org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl`
- **主要方法**：
  - `HeartbeatManager.processHeartbeat(DatanodeRegistration dnR, StorageReport[] reports, long dnCacheCapacity, long dnCacheUsed, int xmitsInProgress, int xceiverCount)`：处理心跳
  - `RMNodeImpl.nodeHeartbeat(ContainerStatus[] containerStatuses, ResourceReport resourceReport, NodeHealthStatus nodeHealthStatus)`：节点心跳处理

## 5. 配置管理流程

### 5.1 配置加载机制
- **核心类**：`org.apache.hadoop.conf.Configuration`
- **主要方法**：
  - `Configuration.addResource(Resource resource)`：添加资源
  - `Configuration.get(String name)`：获取配置项
  - `Configuration.set(String name, String value)`：设置配置项
  - `GenericOptionsParser.parseGeneralOptions(String[] args)`：解析通用选项

## 6. 守护进程管理

### 6.1 守护进程启动机制
- **核心脚本**：
  - `hadoop-daemon.sh`
  - `yarn-daemon.sh`
  - `hadoop-functions.sh`
- **关键函数**：
  - `hadoop_start_daemon`：启动守护进程
  - `hadoop_rotate_log`：日志轮转
  - `hadoop_wait_for_process_to_die`：等待进程终止

### 6.2 服务生命周期管理
- **核心类**：
  - `org.apache.hadoop.service.AbstractService`
  - `org.apache.hadoop.service.CompositeService`
- **主要方法**：
  - `AbstractService.init(Configuration conf)`：初始化服务
  - `AbstractService.start()`：启动服务
  - `AbstractService.stop()`：停止服务
  - `CompositeService.addService(Service service)`：添加子服务

## 7. 高级特性

### 7.1 HDFS高可用（HA）机制
- **核心类**：
  - `org.apache.hadoop.hdfs.server.namenode.ha.ActiveStandbyElector`
  - `org.apache.hadoop.hdfs.tools.zkfc.ZKFailoverController`
- **主要方法**：
  - `ActiveStandbyElector.joinElection(boolean checkExists)`：加入选举
  - `ZKFailoverController.doFenceOldActive(NameNodeProxies.ProxyAndInfo<HAProtocol> prevActiveProxy)`：隔离旧的Active节点

### 7.2 YARN高级调度器
- **核心类**：
  - `org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler`
  - `org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler`
- **主要方法**：
  - `CapacityScheduler.serviceInit(Configuration conf)`：初始化容量调度器
  - `FairScheduler.serviceInit(Configuration conf)`：初始化公平调度器
  - `AbstractYarnScheduler.allocateContainers(ApplicationAttemptId applicationAttemptId, List<ResourceRequest> resourceRequests)`：分配容器

### 7.3 容器运行时隔离
- **核心类**：
  - `org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor`
  - `org.apache.hadoop.yarn.server.nodemanager.docker.DockerContainerExecutor`
- **主要方法**：
  - `LinuxContainerExecutor.launchContainer(Container container, ContainerLaunchContext containerLaunchContext, ContainerState containerState)`：启动容器
  - `DockerContainerExecutor.startContainer(Container container, ContainerLaunchContext containerLaunchContext)`：启动Docker容器

## 8. 调试与测试工具

### 8.1 测试集群工具
- **核心类**：
  - `org.apache.hadoop.hdfs.MiniDFSCluster`
  - `org.apache.hadoop.yarn.server.MiniYARNCluster`
- **主要方法**：
  - `MiniDFSCluster.Builder.build()`：构建迷你DFS集群
  - `MiniYARNCluster.start()`：启动迷你YARN集群

## 9. 配置文件关键位置

### 9.1 核心配置文件
- **core-site.xml**：通用配置，如默认文件系统URI
- **hdfs-site.xml**：HDFS特定配置，如副本数、块大小
- **yarn-site.xml**：YARN特定配置，如调度器、资源管理
- **mapred-site.xml**：MapReduce特定配置，如框架名称
- **capacity-scheduler.xml**：容量调度器配置
- **fair-scheduler.xml**：公平调度器配置
- **log4j2.properties**：日志配置

## 10. 关键日志文件

### 10.1 守护进程日志
- **NameNode日志**：`logs/hadoop-*-namenode-*.log`
- **DataNode日志**：`logs/hadoop-*-datanode-*.log`
- **ResourceManager日志**：`logs/yarn-*-resourcemanager-*.log`
- **NodeManager日志**：`logs/yarn-*-nodemanager-*.log`
- **作业历史日志**：`logs/mapred-*-historyserver-*.log`