# 本地启动Hadoop并进行调试指南

## 环境准备

1. **系统要求**
   - Unix系统（macOS或Linux）
   - JDK 1.8或更高版本
   - Maven 3.3或更高版本
   - SSH（用于免密登录）

2. **环境变量配置**
   ```bash
   # 在hadoop-env.sh中设置JAVA_HOME
   export JAVA_HOME=/path/to/your/jdk
   
   # 设置语言环境
   export LANG=en_US.UTF-8
   ```

## 构建Hadoop

在开始前，建议先构建Hadoop项目：

```bash
# 在项目根目录执行构建命令
mvn clean package -DskipTests
```

## 本地单节点设置

### 1. 配置核心文件

**core-site.xml配置**（位于`etc/hadoop/core-site.xml`）：

```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
```

**hdfs-site.xml配置**（位于`etc/hadoop/hdfs-site.xml`）：

```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
```

**mapreduce-site.xml配置**（位于`etc/hadoop/mapreduce-site.xml`）：

```xml
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
</configuration>
```

**yarn-site.xml配置**（位于`etc/hadoop/yarn-site.xml`）：

```xml
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>localhost</value>
    </property>
</configuration>
```

### 2. 设置SSH免密登录

**步骤1：确保SSH服务已启动**

在MacOS系统上：
```bash
# 检查SSH服务状态
sudo systemsetup -getremotelogin

# 如果未启动，启用SSH服务
sudo systemsetup -setremotelogin on
```

在Linux系统上：
```bash
# 检查SSH服务状态
systemctl status sshd

# 如果未启动，启用并启动SSH服务
sudo systemctl enable sshd
sudo systemctl start sshd
```

**步骤2：生成SSH密钥对并配置免密登录**

```bash
# 生成SSH密钥对（如果尚未生成）
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa

# 将公钥添加到授权列表
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

# 确保密钥文件权限正确
chmod 0700 ~/.ssh
chmod 0600 ~/.ssh/id_rsa
chmod 0600 ~/.ssh/authorized_keys

# 验证SSH配置
ssh localhost
```

**步骤3：解决MacOS特有的SSH配置问题**

如果仍然遇到权限问题，请检查MacOS的SSH配置：

```bash
# 编辑SSH配置文件
sudo nano /etc/ssh/sshd_config

# 确保以下配置项未被注释并设置正确
# PubkeyAuthentication yes
# AuthorizedKeysFile .ssh/authorized_keys .ssh/authorized_keys2

# 重启SSH服务
sudo launchctl stop com.openssh.sshd
sudo launchctl start com.openssh.sshd
```

**步骤4：测试SSH连接**

再次测试SSH连接，如果成功则无需密码即可登录：
```bash
ssh localhost
```

**如果仍然失败，检查以下几点**：
1. 确认`~/.ssh`目录和文件的所有权正确：
   ```bash
   chown -R $USER:$USER ~/.ssh
   ```

2. 检查`/etc/hosts`文件中是否包含localhost的正确映射：
   ```bash
   grep localhost /etc/hosts
   # 应包含类似以下行：
   # 127.0.0.1       localhost
   # ::1             localhost
   ```

3. 临时关闭防火墙测试：
   ```bash
   # MacOS
   sudo /usr/libexec/ApplicationFirewall/socketfilterfw --setglobalstate off
   
   # Linux
   sudo systemctl stop firewalld
   ```

### 3. 格式化HDFS

```bash
bin/hdfs namenode -format
```

#### 如何判断HDFS格式化是否成功

执行格式化命令后，可以通过以下方式判断是否成功：

1. **查看命令输出**：成功格式化后，最后几行输出应包含类似以下内容：
   ```
   INFO common.Storage: Storage directory /tmp/hadoop-username/dfs/name has been successfully formatted.
   INFO namenode.FSImageFormatProtobuf: Saving image file /tmp/hadoop-username/dfs/name/current/fsimage.ckpt_0000000000000000000 using no compression
   INFO namenode.NNStorageRetentionManager: Going to retain 1 images with txid >= 0
   INFO namenode.NameNode: SHUTDOWN_MSG:
   /************************************************************
   SHUTDOWN_MSG: Shutting down NameNode at hostname/ip-address
   ************************************************************/
   ```

2. **检查存储目录**：确认格式化后的目录是否创建成功
   ```bash
   # 默认存储位置（可在core-site.xml中通过hadoop.tmp.dir配置）
   ls -la /tmp/hadoop-${USER}/dfs/name
   # 应包含current目录和文件
   ```

3. **注意**：格式化命令执行完成后，NameNode进程会自动关闭（如日志中所示的"Shutting down NameNode"），这是正常现象，不表示失败。格式化只是准备文件系统，真正的NameNode服务需要通过`start-dfs.sh`命令启动。

### 4. 启动HDFS

```bash
sbin/start-dfs.sh
```

启动后，可以通过浏览器访问NameNode Web界面：http://localhost:9870/

### 5. 启动YARN（可选）

```bash
sbin/start-yarn.sh
```

启动后，可以通过浏览器访问ResourceManager Web界面：http://localhost:8088/

## 配置调试环境

### 1. 设置Java调试端口

在`hadoop-env.sh`文件中，为需要调试的组件添加调试参数：

```bash
# 为NameNode设置调试端口
export HDFS_NAMENODE_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"

# 为DataNode设置调试端口
export HDFS_DATANODE_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006"

# 为ResourceManager设置调试端口
export YARN_RESOURCEMANAGER_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5007"

# 为NodeManager设置调试端口
export YARN_NODEMANAGER_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5008"
```

> 注意：
> - `suspend=y`表示JVM会在启动时等待调试器连接
> - `suspend=n`表示JVM会直接启动，不等待调试器连接
> - 每个组件应使用不同的端口号

### 2. 配置日志级别

可以通过以下方式配置或调整日志级别：

**静态配置（在hadoop-env.sh中设置）**：

```bash
# 设置默认日志级别
export HADOOP_ROOT_LOGGER=DEBUG,console

# 设置特定组件的日志级别
export HDFS_NAMENODE_OPTS="-Dhadoop.security.logger=DEBUG,RFAS"
```

**动态调整日志级别（使用daemonlog命令）**：

```bash
# 获取特定组件的日志级别
hadoop daemonlog -getlevel localhost:9870 org.apache.hadoop.hdfs.server.namenode.NameNode

# 设置特定组件的日志级别
hadoop daemonlog -setlevel localhost:9870 org.apache.hadoop.hdfs.server.namenode.NameNode DEBUG
```

## 使用IDE进行调试

以IntelliJ IDEA为例：

1. 在IDE中打开Hadoop项目
2. 创建一个新的远程调试配置：
   - 点击"Run" > "Edit Configurations..."
   - 点击"+" > "Remote JVM Debug"
   - 设置名称（如"Hadoop NameNode Debug"）
   - 设置主机为"localhost"
   - 设置端口为之前在hadoop-env.sh中配置的端口（如5005）
   - 点击"Apply"保存配置
3. 启动Hadoop组件（使用修改后的hadoop-env.sh）
4. 在IDE中点击"Debug"按钮开始调试
5. 在代码中设置断点

## 测试Hadoop安装

可以通过运行一个简单的MapReduce作业来验证安装是否成功：

```bash
# 创建HDFS目录
bin/hdfs dfs -mkdir -p /user/$USER

# 上传测试数据
bin/hdfs dfs -put etc/hadoop/*.xml input

# 运行MapReduce示例作业
bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.1.jar grep input output 'dfs[a-z.]+'

# 查看结果
bin/hdfs dfs -cat output/*
```

## 停止Hadoop

```bash
# 停止YARN（如果已启动）
sbin/stop-yarn.sh

# 停止HDFS
sbin/stop-dfs.sh
```

## 常见问题排查

1. **端口冲突**：检查是否有其他应用程序占用了Hadoop所需的端口
2. **权限问题**：确保HDFS目录有正确的权限设置
3. **日志分析**：检查`logs`目录下的日志文件以获取详细错误信息
4. **内存不足**：如果遇到内存溢出问题，可以在hadoop-env.sh中增加JVM堆大小
   ```bash
   export HADOOP_HEAPSIZE_MAX=4g
   ```

## 启动失败排查指南

如果在执行`start-dfs.sh`或`start-yarn.sh`时遇到类似以下错误：

```
Starting namenodes on [macbook-pro]
macbook-pro: wohaocai@macbook-pro: Permission denied (publickey,password,keyboard-interactive).
Starting datanodes
localhost: wohaocai@localhost: Permission denied (publickey,password,keyboard-interactive).
```

请按照以下步骤进行详细排查：

### 1. SSH权限问题深度排查

**检查SSH服务状态和配置**

```bash
# 检查SSH服务是否正常运行
# MacOS
sudo systemsetup -getremotelogin

# Linux
sudo systemctl status sshd

# 检查SSH配置是否允许公钥认证
sudo grep -i PubkeyAuthentication /etc/ssh/sshd_config
# 应显示：PubkeyAuthentication yes

# 检查AuthorizedKeysFile配置
sudo grep -i AuthorizedKeysFile /etc/ssh/sshd_config
# 应显示：AuthorizedKeysFile .ssh/authorized_keys .ssh/authorized_keys2
```

**验证密钥文件和权限**

```bash
# 检查密钥文件是否存在
ls -la ~/.ssh/
# 应包含id_rsa（私钥）和id_rsa.pub（公钥）

# 检查密钥文件权限是否正确
ls -la ~/.ssh/
# 确保：
# - ~/.ssh 权限为700 (drwx------)
# - ~/.ssh/id_rsa 权限为600 (-rw-------)
# - ~/.ssh/authorized_keys 权限为600 (-rw-------)

# 验证公钥是否正确添加到授权列表
cat ~/.ssh/id_rsa.pub
cat ~/.ssh/authorized_keys
# 确认输出内容一致
```

**测试SSH连接的详细信息**

```bash
# 使用-vv选项获取详细的SSH连接日志
ssh -vv localhost

# 检查连接过程中的错误信息，重点关注：
# - 密钥是否被正确读取
# - 授权文件是否被访问
# - 是否有权限被拒绝的具体原因
```

### 2. 手动启动组件进行测试

如果通过脚本启动失败，可以尝试手动启动各个组件以定位问题：

```bash
# 手动启动NameNode
# 首先停止所有Hadoop服务
./sbin/stop-all.sh

# 手动启动NameNode
./bin/hdfs --daemon start namenode

# 检查NameNode是否启动成功
jps
# 应该能看到NameNode进程

# 检查NameNode日志
cat logs/hadoop-*-namenode-*.log

# 如果NameNode启动成功，再手动启动DataNode
./bin/hdfs --daemon start datanode

# 检查DataNode是否启动成功
jps
# 应该能看到DataNode进程

# 检查DataNode日志
cat logs/hadoop-*-datanode-*.log

### 3. start-dfs.sh执行后没有找到进程的特别排查

如果执行`start-dfs.sh`命令后没有显示明显错误（如没有权限被拒绝的提示），但使用`jps`或`ps -ef|grep namenode`命令找不到NameNode进程，可能是以下原因：

#### 特别注意：fs.defaultFS配置问题

一个常见的错误是`fs.defaultFS`配置使用了无效的`file:///`格式。请确保在`core-site.xml`中使用正确的HDFS协议配置：

```bash
# 检查当前的fs.defaultFS配置
grep -A 3 fs.defaultFS etc/hadoop/core-site.xml

# 正确的配置应为：
# <property>
#   <name>fs.defaultFS</name>
#   <value>hdfs://localhost:9000</value>
# </property>

# 如果发现配置错误，请修改core-site.xml文件
nano etc/hadoop/core-site.xml

# 修改后需要重新格式化HDFS（注意：这会删除所有数据）
./sbin/stop-all.sh
rm -rf /tmp/hadoop-${USER}
./bin/hdfs namenode -format
./sbin/start-dfs.sh

这个错误通常会导致NameNode无法正常启动，但不会在启动脚本中显示明确的错误信息。通过检查日志文件可以发现相关错误提示。

#### 排查步骤：

1. **检查日志文件**：这是最重要的排查步骤
   ```bash
   # 检查NameNode日志（重点关注ERROR和FATAL级别信息）
   cat logs/hadoop-*-namenode-*.log | grep -i error
   cat logs/hadoop-*-namenode-*.log | grep -i fatal
   
   # 检查DataNode日志
   cat logs/hadoop-*-datanode-*.log | grep -i error
   
   # 检查启动脚本日志
   cat logs/hadoop-*-start-dfs.sh-*.log
   ```

2. **检查Hadoop临时目录权限**：
   ```bash
   # 查看core-site.xml中配置的临时目录
   grep hadoop.tmp.dir etc/hadoop/core-site.xml
   
   # 默认临时目录（如果未配置）
   TMP_DIR="/tmp/hadoop-${USER}"
   # 或使用配置中的目录
   # TMP_DIR=$(grep -oP 'hadoop.tmp.dir">\K[^<]+' etc/hadoop/core-site.xml)
   
   # 检查临时目录权限
   ls -la "$TMP_DIR"
   
   # 如果权限不正确，修复权限
   sudo chown -R ${USER}:${USER} "$TMP_DIR"
   chmod -R 755 "$TMP_DIR"
   ```

3. **关于"Unable to load native-hadoop library"警告**：
   这个警告通常不会影响Hadoop的基本功能，它表示系统无法加载Hadoop的本地库（用于性能优化），系统会自动使用纯Java实现替代。如果需要解决这个警告，可以：
   ```bash
   # 1. 确认是否编译了本地库
   ls -la lib/native/
   
   # 2. 检查HADOOP_OPTS环境变量配置
   echo $HADOOP_OPTS
   
   # 3. 可以在hadoop-env.sh中添加以下配置来指定本地库路径
   # export HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=$HADOOP_HOME/lib/native"
   ```

4. **检查Java版本兼容性**：
   ```bash
   # 检查当前使用的Java版本
   java -version
   
   # 检查Hadoop要求的Java版本
   grep -A 5 "Java" BUILDING.txt
   
   # 确保JAVA_HOME设置正确
   echo $JAVA_HOME
   ```

5. **检查主机名解析问题**：
   ```bash
   # 检查主机名配置
   hostname
   
   # 检查/etc/hosts文件中是否包含主机名的正确映射
   grep $(hostname) /etc/hosts
   # 应包含类似以下行：
   # 127.0.0.1       $(hostname)
   ```

6. **检查端口占用**：
   ```bash
   # 检查NameNode使用的端口（默认为9870和9000）
   lsof -i :9870
   lsof -i :9000
   
   # 如果端口被占用，可以在core-site.xml中修改端口配置
   # <property>
   #   <name>dfs.namenode.http-address</name>
   #   <value>localhost:9871</value>
   # </property>
   # <property>
   #   <name>fs.defaultFS</name>
   #   <value>hdfs://localhost:9001</value>
   # </property>
   ```

7. **重新格式化HDFS（注意：这会删除所有数据）**：
   ```bash
   # 停止所有Hadoop服务
   ./sbin/stop-all.sh
   
   # 删除临时目录
   rm -rf /tmp/hadoop-${USER}
   
   # 重新格式化HDFS
   ./bin/hdfs namenode -format
   
   # 再次尝试启动HDFS
   ./sbin/start-dfs.sh
   
   # 检查进程
   jps
   ```

### 3. 配置文件和环境变量检查

```bash
# 检查Hadoop配置文件的所有权和权限
ls -la etc/hadoop/

# 确保JAVA_HOME设置正确
echo $JAVA_HOME
# 应该指向有效的JDK安装目录

# 检查hadoop-env.sh中的配置
cat etc/hadoop/hadoop-env.sh | grep -v '^#' | grep -v '^$'

# 检查Hadoop环境变量配置
printenv | grep HADOOP_
```

### 4. 主机名和网络配置问题

```bash
# 检查主机名配置
hostname

# 检查/etc/hosts文件配置
cat /etc/hosts
# 确保包含以下行：
# 127.0.0.1       localhost
# ::1             localhost
# 127.0.0.1       $(hostname)

# 测试主机名解析
ping -c 3 $(hostname)
```

### 5. 防火墙和安全软件检查

```bash
# 检查防火墙状态
# MacOS
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --getglobalstate

# Linux
sudo systemctl status firewalld || sudo systemctl status ufw

# 临时关闭防火墙进行测试
# MacOS
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --setglobalstate off

# Linux
sudo systemctl stop firewalld || sudo ufw disable
```

### 6. 特殊情况：MacOS的系统完整性保护(SIP)影响

在某些MacOS版本上，系统完整性保护可能会影响SSH配置：

```bash
# 检查SIP状态
csrutil status

# 如果需要，可以在恢复模式下禁用SIP（高级用户操作）
# 但通常不建议这样做，更好的方法是调整SSH配置以适应SIP
```

### 7. 使用Hadoop的详细日志进行故障排查

```bash
# 设置详细日志级别
# 方法1：修改hadoop-env.sh
# export HADOOP_ROOT_LOGGER=DEBUG,console

# 方法2：临时在命令行设置
HADOOP_ROOT_LOGGER=DEBUG,console ./sbin/start-dfs.sh

# 查看详细日志输出
```

通过以上详细的排查步骤，您应该能够解决在本地启动Hadoop过程中遇到的SSH权限问题和其他常见启动故障。如果问题仍然存在，可以考虑重新构建Hadoop或尝试使用Docker环境来隔离可能的系统配置问题。