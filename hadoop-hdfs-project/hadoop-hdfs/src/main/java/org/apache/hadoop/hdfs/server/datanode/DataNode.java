/**
 * * * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE 文件
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this 文件
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this 文件 except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * HTTP（超文本传输协议）（超文本传输协议）:// www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;


import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurableBase;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.net.DomainPeerServer;
import org.apache.hadoop.hdfs.net.TcpPeerServer;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.DatanodeInfoBuilder;
import org.apache.hadoop.hdfs.protocol.datatransfer.*;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferServer;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.ClientDatanodeProtocolService;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DNTransferAckProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocol.proto.InterDatanodeProtocolProtos.InterDatanodeProtocolService;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.ReconfigurationProtocolService;
import org.apache.hadoop.hdfs.protocolPB.*;
import org.apache.hadoop.hdfs.security.token.block.*;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier.AccessMode;
import org.apache.hadoop.hdfs.server.common.DataNodeLockManager.LockLevel;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.MetricsLoggerTask;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.datanode.SecureDataNodeStarter.SecureResources;
import org.apache.hadoop.hdfs.server.datanode.checker.DatasetVolumeChecker;
import org.apache.hadoop.hdfs.server.datanode.checker.StorageLocationChecker;
import org.apache.hadoop.hdfs.server.datanode.erasurecode.ErasureCodingWorker;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.AddBlockPoolException;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.BlockPoolSlice;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeDiskMetrics;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodePeerMetrics;
import org.apache.hadoop.hdfs.server.datanode.web.DatanodeHttpServer;
import org.apache.hadoop.hdfs.server.diskbalancer.DiskBalancerConstants;
import org.apache.hadoop.hdfs.server.diskbalancer.DiskBalancerException;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.hadoop.thirdparty.protobuf.BlockingService;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.hadoop.tracing.Tracer;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.Timer;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.eclipse.jetty.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.management.ObjectName;
import javax.net.SocketFactory;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.*;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage.*;
import static org.apache.hadoop.util.ExitUtil.terminate;
import static org.apache.hadoop.util.Preconditions.checkNotNull;
import static org.apache.hadoop.util.Time.now;

/**
 * ********************************************************
 * DataNode（数据节点）是一个类（和程序），用于存储DFS部署中的一组数据块。
 * 单个部署可以有一个或多个DataNode。每个DataNode定期与单个NameNode通信。
 * 它还不时与客户端代码和其他DataNode通信。
 *
 * DataNode存储一系列命名的数据块。DataNode允许客户端代码读取这些数据块，
 * 或写入新的数据块数据。DataNode还可能根据来自NameNode的指令，
 * 删除数据块或在DataNode之间复制数据块。
 *
 * DataNode只维护一个关键表：
 *   数据块{@literal ->} 字节流（大小不超过BLOCK_SIZE）
 *
 * 此信息存储在本地磁盘上。DataNode在启动时向NameNode报告表的内容，
 * 之后也会定期报告。
 *
 * DataNode的生命周期是一个不断向NameNode请求任务的循环。
 * NameNode不能直接连接到DataNode；NameNode只是从DataNode调用的函数中返回值。
 *
 * DataNode维护一个开放的服务器套接字，以便客户端代码或其他DataNode可以读写数据。
 * 此服务器的主机/端口会报告给NameNode，然后NameNode会将该信息发送给可能感兴趣的客户端或其他DataNode。
 * ********************************************************
 */
@InterfaceAudience.Private
public class DataNode extends ReconfigurableBase
        implements InterDatanodeProtocol, ClientDatanodeProtocol,
        DataNodeMXBean, ReconfigurationProtocol {
    public static final Logger LOG = LoggerFactory.getLogger(DataNode.class);

    static {
        HdfsConfiguration.init();
    }

    public static final String DN_CLIENTTRACE_FORMAT =
            "src: %s" +      // src IP
                    ", dest: %s" +   // dst IP
                    ", volume: %s" + // volume
                    ", bytes: %s" +  // byte count
                    ", op: %s" +     // operation
                    ", cliID: %s" +  // DFSClient id
                    ", offset: %s" + // offset
                    ", srvID: %s" +  // DatanodeRegistration
                    ", blockid: %s" + // block id
                    ", duration(ns): %s";  // duration time

    static final Logger CLIENT_TRACE_LOG =
            LoggerFactory.getLogger(DataNode.class.getName() + ".clienttrace");

    private static final String USAGE =
            "Usage: hdfs datanode [-regular | -rollback | -rollingupgrade rollback" +
                    " ]\n" +
                    "    -regular                 : Normal DataNode startup (default).\n" +
                    "    -rollback                : Rollback a standard or rolling upgrade.\n" +
                    "    -rollingupgrade rollback : Rollback a rolling upgrade operation.\n" +
                    "  Refer to HDFS documentation for the difference between standard\n" +
                    "  and rolling upgrades.";

    static final int CURRENT_BLOCK_FORMAT_VERSION = 1;
    public static final int MAX_VOLUME_FAILURE_TOLERATED_LIMIT = -1;
    public static final String MAX_VOLUME_FAILURES_TOLERATED_MSG =
            "should be greater than or equal to -1";

    /** A list of property that are reconfigurable at runtime. */
    private static final List<String> RECONFIGURABLE_PROPERTIES =
            Collections.unmodifiableList(
                    Arrays.asList(
                            DFS_DATANODE_DATA_DIR_KEY,
                            DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
                            DFS_BLOCKREPORT_INTERVAL_MSEC_KEY,
                            DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY,
                            DFS_BLOCKREPORT_INITIAL_DELAY_KEY,
                            DFS_DATANODE_MAX_RECEIVER_THREADS_KEY,
                            DFS_CACHEREPORT_INTERVAL_MSEC_KEY,
                            DFS_DATANODE_PEER_STATS_ENABLED_KEY,
                            DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_KEY,
                            DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_KEY,
                            DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_KEY,
                            DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY,
                            DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY,
                            DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_KEY,
                            DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_KEY,
                            DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_KEY,
                            FS_DU_INTERVAL_KEY,
                            FS_GETSPACEUSED_JITTER_KEY,
                            FS_GETSPACEUSED_CLASSNAME,
                            DFS_DISK_BALANCER_ENABLED,
                            DFS_DISK_BALANCER_PLAN_VALID_INTERVAL,
                            DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY,
                            DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY,
                            DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY,
                            DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY));

    public static final String METRICS_LOG_NAME = "DataNodeMetricsLog";

    private static final String DATANODE_HTRACE_PREFIX = "datanode.htrace.";
    private final FileIoProvider fileIoProvider;

    private static final String NETWORK_ERRORS = "networkErrors";

    /**
     * Use {@link NetUtils#createSocketAddr(String)} instead.
     */
    @Deprecated
    public static InetSocketAddress createSocketAddr(String target) {
        return NetUtils.createSocketAddr(target);
    }

    volatile boolean shouldRun = true;
    volatile boolean shutdownForUpgrade = false;
    private boolean shutdownInProgress = false;
    private BlockPoolManager blockPoolManager;
    volatile FsDatasetSpi<? extends FsVolumeSpi> data = null;
    private String clusterId = null;

    final AtomicInteger xmitsInProgress = new AtomicInteger();
    Daemon dataXceiverServer = null;
    DataXceiverServer xserver = null;
    Daemon localDataXceiverServer = null;
    ShortCircuitRegistry shortCircuitRegistry = null;
    ThreadGroup threadGroup = null;
    private DNConf dnConf;
    private volatile boolean heartbeatsDisabledForTests = false;
    private volatile boolean ibrDisabledForTests = false;
    private volatile boolean cacheReportsDisabledForTests = false;
    private DataStorage storage = null;

    private DatanodeHttpServer httpServer = null;
    private int infoPort;
    private int infoSecurePort;

    DataNodeMetrics metrics;
    @Nullable
    private volatile DataNodePeerMetrics peerMetrics;
    private volatile DataNodeDiskMetrics diskMetrics;
    private InetSocketAddress streamingAddr;

    private LoadingCache<String, Map<String, Long>> datanodeNetworkCounts;

    private String hostName;
    private DatanodeID id;

    final private String fileDescriptorPassingDisabledReason;
    boolean isBlockTokenEnabled;
    BlockPoolTokenSecretManager blockPoolTokenSecretManager;
    private boolean hasAnyBlockPoolRegistered = false;

    private BlockScanner blockScanner;
    private DirectoryScanner directoryScanner = null;

    /** Activated plug-ins. */
    private List<ServicePlugin> plugins;

    // For InterDataNodeProtocol
    public RPC.Server ipcServer;

    private JvmPauseMonitor pauseMonitor;

    private SecureResources secureResources = null;
    // dataDirs must be accessed while holding the DataNode lock.
    private List<StorageLocation> dataDirs;
    private final String confVersion;
    private final long maxNumberOfBlocksToLog;
    private final boolean pipelineSupportECN;
    private final boolean pipelineSupportSlownode;

    private final List<String> usersWithLocalPathAccess;
    private final boolean connectToDnViaHostname;
    ReadaheadPool readaheadPool;
    SaslDataTransferClient saslClient;
    SaslDataTransferServer saslServer;
    private ObjectName dataNodeInfoBeanName;
    // Test verification only
    private volatile long lastDiskErrorCheck;
    private String supergroup;
    private boolean isPermissionEnabled;
    private String dnUserName = null;
    private BlockRecoveryWorker blockRecoveryWorker;
    private ErasureCodingWorker ecWorker;
    private final Tracer tracer;
    private static final int NUM_CORES = Runtime.getRuntime()
            .availableProcessors();
    private final double congestionRatio;
    private DiskBalancer diskBalancer;
    private DataSetLockManager dataSetLockManager;

    private final ExecutorService xferService;

    @Nullable
    private final StorageLocationChecker storageLocationChecker;

    private final DatasetVolumeChecker volumeChecker;

    private final SocketFactory socketFactory;

    private static Tracer createTracer(Configuration conf) {
        return new Tracer.Builder("DataNode").
                conf(TraceUtils.wrapHadoopConf(DATANODE_HTRACE_PREFIX, conf)).
                build();
    }

    private long[] oobTimeouts;
    /** timeout value of each OOB type */

    private ScheduledThreadPoolExecutor metricsLoggerTimer;

    private long startTime = 0;

    private DataTransferThrottler ecReconstuctReadThrottler;
    private DataTransferThrottler ecReconstuctWriteThrottler;

    /**
     * 创建一个用于测试目的的虚拟DataNode。
     */
    @VisibleForTesting
    @InterfaceAudience.LimitedPrivate("HDFS")
    DataNode(final Configuration conf) throws DiskErrorException {
        super(conf);
        this.tracer = createTracer(conf);
        this.fileIoProvider = new FileIoProvider(conf, this);
        this.fileDescriptorPassingDisabledReason = null;
        this.maxNumberOfBlocksToLog = 0;
        this.confVersion = null;
        this.usersWithLocalPathAccess = null;
        this.connectToDnViaHostname = false;
        this.blockScanner = new BlockScanner(this, this.getConf());
        this.pipelineSupportECN = false;
        this.pipelineSupportSlownode = false;
        this.socketFactory = NetUtils.getDefaultSocketFactory(conf);
        this.dnConf = new DNConf(this);
        this.dataSetLockManager = new DataSetLockManager(conf);
        initOOBTimeout();
        storageLocationChecker = null;
        volumeChecker = new DatasetVolumeChecker(conf, new Timer());
        this.xferService =
                HadoopExecutors.newCachedThreadPool(new Daemon.DaemonFactory());
        double congestionRationTmp = conf.getDouble(DFSConfigKeys.DFS_PIPELINE_CONGESTION_RATIO,
                DFSConfigKeys.DFS_PIPELINE_CONGESTION_RATIO_DEFAULT);
        this.congestionRatio = congestionRationTmp > 0 ?
                congestionRationTmp : DFSConfigKeys.DFS_PIPELINE_CONGESTION_RATIO_DEFAULT;
    }

    /**
     * 给定配置、数据目录数组和NameNode代理来创建DataNode。
     */
    DataNode(final Configuration conf,
             final List<StorageLocation> dataDirs,
             final StorageLocationChecker storageLocationChecker,
             final SecureResources resources) throws IOException {
        super(conf);
        this.tracer = createTracer(conf);
        this.fileIoProvider = new FileIoProvider(conf, this);
        this.dataSetLockManager = new DataSetLockManager(conf);
        this.blockScanner = new BlockScanner(this);
        this.lastDiskErrorCheck = 0;
        //todo     // dfs.namenode.max-num-blocks-to-log : 1000
        this.maxNumberOfBlocksToLog = conf.getLong(DFS_MAX_NUM_BLOCKS_TO_LOG_KEY,
                DFS_MAX_NUM_BLOCKS_TO_LOG_DEFAULT);

        this.usersWithLocalPathAccess = Arrays.asList(
                conf.getTrimmedStrings(DFSConfigKeys.DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY));
        //todo     // dfs.datanode.use.datanode.hostname : false
        this.connectToDnViaHostname = conf.getBoolean(
                DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME,
                DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT);
        //todo     // dfs.permissions.superusergroup : supergroup
        this.supergroup = conf.get(DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY,
                DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
        //todo     // dfs.permissions.enabled : true
        this.isPermissionEnabled = conf.getBoolean(
                DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY,
                DFSConfigKeys.DFS_PERMISSIONS_ENABLED_DEFAULT);
        //todo     // dfs.pipeline.ecn : false
        this.pipelineSupportECN = conf.getBoolean(
                DFSConfigKeys.DFS_PIPELINE_ECN_ENABLED,
                DFSConfigKeys.DFS_PIPELINE_ECN_ENABLED_DEFAULT);
        this.pipelineSupportSlownode = conf.getBoolean(
                DFSConfigKeys.DFS_PIPELINE_SLOWNODE_ENABLED,
                DFSConfigKeys.DFS_PIPELINE_SLOWNODE_ENABLED_DEFAULT);
        //todo     // 配置文件版本 :
        confVersion = "core-" +
                conf.get("hadoop.common.configuration.version", "UNSPECIFIED") +
                ",hdfs-" +
                conf.get("hadoop.hdfs.configuration.version", "UNSPECIFIED");
        //todo     // 构建 DatasetVolumeChecker
        this.volumeChecker = new DatasetVolumeChecker(conf, new Timer());
        this.xferService =
                HadoopExecutors.newCachedThreadPool(new Daemon.DaemonFactory());

        // Determine whether we should try to pass file descriptors to clients.
        //todo     // 确定是否应尝试将文件描述符传递给客户端 : false
        if (conf.getBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY,
                HdfsClientConfigKeys.Read.ShortCircuit.DEFAULT)) {
            String reason = DomainSocket.getLoadingFailureReason();
            if (reason != null) {
                LOG.warn("File descriptor passing is disabled because {}", reason);
                this.fileDescriptorPassingDisabledReason = reason;
            } else {
                LOG.info("File descriptor passing is enabled.");
                this.fileDescriptorPassingDisabledReason = null;
            }
        } else {
            this.fileDescriptorPassingDisabledReason =
                    "File descriptor passing was not configured.";
            LOG.debug(this.fileDescriptorPassingDisabledReason);
        }
        //todo     // 构建socketFactory
        this.socketFactory = NetUtils.getDefaultSocketFactory(conf);

        try {
            hostName = getHostName(conf);
            LOG.info("Configured hostname is {}", hostName);
            //todo       // 启动 DataNode
            startDataNode(dataDirs, resources);
        } catch (IOException ie) {
            shutdown();
            throw ie;
        }
        final int dncCacheMaxSize =
                conf.getInt(DFS_DATANODE_NETWORK_COUNTS_CACHE_MAX_SIZE_KEY,
                        DFS_DATANODE_NETWORK_COUNTS_CACHE_MAX_SIZE_DEFAULT);
        datanodeNetworkCounts =
                CacheBuilder.newBuilder()
                        .maximumSize(dncCacheMaxSize)
                        .build(new CacheLoader<String, Map<String, Long>>() {
                            @Override
                            public Map<String, Long> load(String key) {
                                final Map<String, Long> ret = new ConcurrentHashMap<>();
                                ret.put(NETWORK_ERRORS, 0L);
                                return ret;
                            }
                        });
        //todo     // 从配置中获取每个OOB类型的超时值
        initOOBTimeout();
        this.storageLocationChecker = storageLocationChecker;
        long ecReconstuctReadBandwidth = conf.getLongBytes(
                DFSConfigKeys.DFS_DATANODE_EC_RECONSTRUCT_READ_BANDWIDTHPERSEC_KEY,
                DFSConfigKeys.DFS_DATANODE_EC_RECONSTRUCT_READ_BANDWIDTHPERSEC_DEFAULT);
        long ecReconstuctWriteBandwidth = conf.getLongBytes(
                DFSConfigKeys.DFS_DATANODE_EC_RECONSTRUCT_WRITE_BANDWIDTHPERSEC_KEY,
                DFSConfigKeys.DFS_DATANODE_EC_RECONSTRUCT_WRITE_BANDWIDTHPERSEC_DEFAULT);
        this.ecReconstuctReadThrottler = ecReconstuctReadBandwidth > 0 ?
                new DataTransferThrottler(100, ecReconstuctReadBandwidth) : null;
        this.ecReconstuctWriteThrottler = ecReconstuctWriteBandwidth > 0 ?
                new DataTransferThrottler(100, ecReconstuctWriteBandwidth) : null;
        double congestionRationTmp = conf.getDouble(DFSConfigKeys.DFS_PIPELINE_CONGESTION_RATIO,
                DFSConfigKeys.DFS_PIPELINE_CONGESTION_RATIO_DEFAULT);
        this.congestionRatio = congestionRationTmp > 0 ?
                congestionRationTmp : DFSConfigKeys.DFS_PIPELINE_CONGESTION_RATIO_DEFAULT;
    }

    @Override  // ReconfigurableBase
    protected Configuration getNewConf() {
        return new HdfsConfiguration();
    }

    /**
     * {@inheritDoc }.
     */
    @Override
    public String reconfigurePropertyImpl(String property, String newVal)
            throws ReconfigurationException {
        switch (property) {
            case DFS_DATANODE_DATA_DIR_KEY: {
                IOException rootException = null;
                try {
                    LOG.info("Reconfiguring {} to {}", property, newVal);
                    this.refreshVolumes(newVal);
                    return getConf().get(DFS_DATANODE_DATA_DIR_KEY);
                } catch (IOException e) {
                    rootException = e;
                } finally {
                    // Send a full block report to let NN acknowledge the volume changes.
                    try {
                        triggerBlockReport(
                                new BlockReportOptions.Factory().setIncremental(false).build());
                    } catch (IOException e) {
                        LOG.warn("Exception while sending the block report after refreshing"
                                + " volumes {} to {}", property, newVal, e);
                        if (rootException == null) {
                            rootException = e;
                        }
                    } finally {
                        if (rootException != null) {
                            throw new ReconfigurationException(property, newVal,
                                    getConf().get(property), rootException);
                        }
                    }
                }
                break;
            }
            case DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY: {
                ReconfigurationException rootException = null;
                try {
                    LOG.info("Reconfiguring {} to {}", property, newVal);
                    int movers;
                    if (newVal == null) {
                        // set to default
                        movers = DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT;
                    } else {
                        movers = Integer.parseInt(newVal);
                        if (movers <= 0) {
                            rootException = new ReconfigurationException(
                                    property,
                                    newVal,
                                    getConf().get(property),
                                    new IllegalArgumentException(
                                            "balancer max concurrent movers must be larger than 0"));
                        }
                    }
                    boolean success = xserver.updateBalancerMaxConcurrentMovers(movers);
                    if (!success) {
                        rootException = new ReconfigurationException(
                                property,
                                newVal,
                                getConf().get(property),
                                new IllegalArgumentException(
                                        "Could not modify concurrent moves thread count"));
                    }
                    return Integer.toString(movers);
                } catch (NumberFormatException nfe) {
                    rootException = new ReconfigurationException(
                            property, newVal, getConf().get(property), nfe);
                } finally {
                    if (rootException != null) {
                        LOG.warn(String.format(
                                "Exception in updating balancer max concurrent movers %s to %s",
                                property, newVal), rootException);
                        throw rootException;
                    }
                }
                break;
            }
            case DFS_BLOCKREPORT_INTERVAL_MSEC_KEY:
            case DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY:
            case DFS_BLOCKREPORT_INITIAL_DELAY_KEY:
                return reconfBlockReportParameters(property, newVal);
            case DFS_DATANODE_MAX_RECEIVER_THREADS_KEY:
            case DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY:
            case DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY:
            case DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY:
                return reconfDataXceiverParameters(property, newVal);
            case DFS_CACHEREPORT_INTERVAL_MSEC_KEY:
                return reconfCacheReportParameters(property, newVal);
            case DFS_DATANODE_PEER_STATS_ENABLED_KEY:
            case DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_KEY:
            case DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_KEY:
            case DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_KEY:
                return reconfSlowPeerParameters(property, newVal);
            case DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY:
            case DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY:
            case DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_KEY:
            case DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_KEY:
            case DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_KEY:
                return reconfSlowDiskParameters(property, newVal);
            case FS_DU_INTERVAL_KEY:
            case FS_GETSPACEUSED_JITTER_KEY:
            case FS_GETSPACEUSED_CLASSNAME:
                return reconfDfsUsageParameters(property, newVal);
            case DFS_DISK_BALANCER_ENABLED:
            case DFS_DISK_BALANCER_PLAN_VALID_INTERVAL:
                return reconfDiskBalancerParameters(property, newVal);
            case DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY:
                return reconfSlowIoWarningThresholdParameters(property, newVal);
            default:
                break;
        }
        throw new ReconfigurationException(
                property, newVal, getConf().get(property));
    }

    private String reconfDataXceiverParameters(String property, String newVal)
            throws ReconfigurationException {
        String result = null;
        try {
            LOG.info("Reconfiguring {} to {}", property, newVal);
            if (property.equals(DFS_DATANODE_MAX_RECEIVER_THREADS_KEY)) {
                Preconditions.checkNotNull(getXferServer(), "DataXceiverServer has not been initialized.");
                int threads = (newVal == null ? DFS_DATANODE_MAX_RECEIVER_THREADS_DEFAULT :
                        Integer.parseInt(newVal));
                result = Integer.toString(threads);
                getXferServer().setMaxXceiverCount(threads);
            } else if (property.equals(DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY)) {
                Preconditions.checkNotNull(getXferServer(), "DataXceiverServer has not been initialized.");
                long bandwidthPerSec = (newVal == null ?
                        DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_DEFAULT : Long.parseLong(newVal));
                DataTransferThrottler transferThrottler = null;
                if (bandwidthPerSec > 0) {
                    transferThrottler = new DataTransferThrottler(bandwidthPerSec);
                } else {
                    bandwidthPerSec = 0;
                }
                result = Long.toString(bandwidthPerSec);
                getXferServer().setTransferThrottler(transferThrottler);
            } else if (property.equals(DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY)) {
                Preconditions.checkNotNull(getXferServer(), "DataXceiverServer has not been initialized.");
                long bandwidthPerSec = (newVal == null ? DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_DEFAULT :
                        Long.parseLong(newVal));
                DataTransferThrottler writeThrottler = null;
                if (bandwidthPerSec > 0) {
                    writeThrottler = new DataTransferThrottler(bandwidthPerSec);
                } else {
                    bandwidthPerSec = 0;
                }
                result = Long.toString(bandwidthPerSec);
                getXferServer().setWriteThrottler(writeThrottler);
            } else if (property.equals(DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY)) {
                Preconditions.checkNotNull(getXferServer(), "DataXceiverServer has not been initialized.");
                long bandwidthPerSec = (newVal == null ? DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_DEFAULT :
                        Long.parseLong(newVal));
                DataTransferThrottler readThrottler = null;
                if (bandwidthPerSec > 0) {
                    readThrottler = new DataTransferThrottler(bandwidthPerSec);
                } else {
                    bandwidthPerSec = 0;
                }
                result = Long.toString(bandwidthPerSec);
                getXferServer().setReadThrottler(readThrottler);
            }
            LOG.info("RECONFIGURE* changed {} to {}", property, newVal);
            return result;
        } catch (IllegalArgumentException e) {
            throw new ReconfigurationException(property, newVal, getConf().get(property), e);
        }
    }

    private String reconfCacheReportParameters(String property, String newVal)
            throws ReconfigurationException {
        String result;
        try {
            LOG.info("Reconfiguring {} to {}", property, newVal);
            Preconditions.checkNotNull(dnConf, "DNConf has not been initialized.");
            long reportInterval = (newVal == null ? DFS_CACHEREPORT_INTERVAL_MSEC_DEFAULT :
                    Long.parseLong(newVal));
            result = Long.toString(reportInterval);
            dnConf.setCacheReportInterval(reportInterval);
            LOG.info("RECONFIGURE* changed {} to {}", property, newVal);
            return result;
        } catch (IllegalArgumentException e) {
            throw new ReconfigurationException(property, newVal, getConf().get(property), e);
        }
    }

    private String reconfBlockReportParameters(String property, String newVal)
            throws ReconfigurationException {
        String result = null;
        try {
            LOG.info("Reconfiguring {} to {}", property, newVal);
            if (property.equals(DFS_BLOCKREPORT_INTERVAL_MSEC_KEY)) {
                Preconditions.checkNotNull(dnConf, "DNConf has not been initialized.");
                long intervalMs = newVal == null ? DFS_BLOCKREPORT_INTERVAL_MSEC_DEFAULT :
                        Long.parseLong(newVal);
                result = Long.toString(intervalMs);
                dnConf.setBlockReportInterval(intervalMs);
                for (BPOfferService bpos : blockPoolManager.getAllNamenodeThreads()) {
                    if (bpos != null) {
                        for (BPServiceActor actor : bpos.getBPServiceActors()) {
                            actor.getScheduler().setBlockReportIntervalMs(intervalMs);
                        }
                    }
                }
            } else if (property.equals(DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY)) {
                Preconditions.checkNotNull(dnConf, "DNConf has not been initialized.");
                long threshold = newVal == null ? DFS_BLOCKREPORT_SPLIT_THRESHOLD_DEFAULT :
                        Long.parseLong(newVal);
                result = Long.toString(threshold);
                dnConf.setBlockReportSplitThreshold(threshold);
            } else if (property.equals(DFS_BLOCKREPORT_INITIAL_DELAY_KEY)) {
                Preconditions.checkNotNull(dnConf, "DNConf has not been initialized.");
                int initialDelay = newVal == null ? DFS_BLOCKREPORT_INITIAL_DELAY_DEFAULT :
                        Integer.parseInt(newVal);
                result = Integer.toString(initialDelay);
                dnConf.setInitBRDelayMs(result);
            }
            LOG.info("RECONFIGURE* changed {} to {}", property, newVal);
            return result;
        } catch (IllegalArgumentException e) {
            throw new ReconfigurationException(property, newVal, getConf().get(property), e);
        }
    }

    private String reconfSlowPeerParameters(String property, String newVal)
            throws ReconfigurationException {
        String result = null;
        try {
            LOG.info("Reconfiguring {} to {}", property, newVal);
            if (property.equals(DFS_DATANODE_PEER_STATS_ENABLED_KEY)) {
                Preconditions.checkNotNull(dnConf, "DNConf has not been initialized.");
                if (newVal != null && !newVal.equalsIgnoreCase("true")
                        && !newVal.equalsIgnoreCase("false")) {
                    throw new IllegalArgumentException("Not a valid Boolean value for " + property +
                            " in reconfSlowPeerParameters");
                }
                boolean enable = (newVal == null ? DFS_DATANODE_PEER_STATS_ENABLED_DEFAULT :
                        Boolean.parseBoolean(newVal));
                result = Boolean.toString(enable);
                dnConf.setPeerStatsEnabled(enable);
                if (enable) {
                    // Create if it doesn't exist, overwrite if it does.
                    peerMetrics = DataNodePeerMetrics.create(getDisplayName(), getConf());
                }
            } else if (property.equals(DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_KEY)) {
                Preconditions.checkNotNull(peerMetrics, "DataNode peer stats may be disabled.");
                long minNodes = (newVal == null ? DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_DEFAULT :
                        Long.parseLong(newVal));
                result = Long.toString(minNodes);
                peerMetrics.setMinOutlierDetectionNodes(minNodes);
            } else if (property.equals(DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_KEY)) {
                Preconditions.checkNotNull(peerMetrics, "DataNode peer stats may be disabled.");
                long threshold = (newVal == null ? DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_DEFAULT :
                        Long.parseLong(newVal));
                result = Long.toString(threshold);
                peerMetrics.setLowThresholdMs(threshold);
            } else if (property.equals(DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_KEY)) {
                Preconditions.checkNotNull(peerMetrics, "DataNode peer stats may be disabled.");
                long minSamples = (newVal == null ?
                        DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_DEFAULT :
                        Long.parseLong(newVal));
                result = Long.toString(minSamples);
                peerMetrics.setMinOutlierDetectionSamples(minSamples);
            }
            LOG.info("RECONFIGURE* changed {} to {}", property, newVal);
            return result;
        } catch (IllegalArgumentException e) {
            throw new ReconfigurationException(property, newVal, getConf().get(property), e);
        }
    }

    private String reconfSlowDiskParameters(String property, String newVal)
            throws ReconfigurationException {
        String result = null;
        try {
            LOG.info("Reconfiguring {} to {}", property, newVal);
            if (property.equals(DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY)) {
                checkNotNull(dnConf, "DNConf has not been initialized.");
                String reportInterval = (newVal == null ? DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_DEFAULT :
                        newVal);
                result = reportInterval;
                dnConf.setOutliersReportIntervalMs(reportInterval);
                for (BPOfferService bpos : blockPoolManager.getAllNamenodeThreads()) {
                    if (bpos != null) {
                        for (BPServiceActor actor : bpos.getBPServiceActors()) {
                            actor.getScheduler().setOutliersReportIntervalMs(
                                    dnConf.outliersReportIntervalMs);
                        }
                    }
                }
            } else if (property.equals(DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY)) {
                checkNotNull(dnConf, "DNConf has not been initialized.");
                int samplingPercentage = (newVal == null ?
                        DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_DEFAULT :
                        Integer.parseInt(newVal));
                result = Integer.toString(samplingPercentage);
                dnConf.setFileIoProfilingSamplingPercentage(samplingPercentage);
                if (fileIoProvider != null) {
                    fileIoProvider.getProfilingEventHook().setSampleRangeMax(samplingPercentage);
                }
                if (samplingPercentage > 0 && diskMetrics == null) {
                    diskMetrics = new DataNodeDiskMetrics(this,
                            dnConf.outliersReportIntervalMs, getConf());
                } else if (samplingPercentage <= 0 && diskMetrics != null) {
                    diskMetrics.shutdownAndWait();
                }
            } else if (property.equals(DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_KEY)) {
                checkNotNull(diskMetrics, "DataNode disk stats may be disabled.");
                long minDisks = (newVal == null ? DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_DEFAULT :
                        Long.parseLong(newVal));
                result = Long.toString(minDisks);
                diskMetrics.setMinOutlierDetectionDisks(minDisks);
            } else if (property.equals(DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_KEY)) {
                checkNotNull(diskMetrics, "DataNode disk stats may be disabled.");
                long threshold = (newVal == null ? DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_DEFAULT :
                        Long.parseLong(newVal));
                result = Long.toString(threshold);
                diskMetrics.setLowThresholdMs(threshold);
            } else if (property.equals(DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_KEY)) {
                checkNotNull(diskMetrics, "DataNode disk stats may be disabled.");
                int maxSlowDisksToExclude = (newVal == null ?
                        DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_DEFAULT : Integer.parseInt(newVal));
                result = Integer.toString(maxSlowDisksToExclude);
                diskMetrics.setMaxSlowDisksToExclude(maxSlowDisksToExclude);
            }
            LOG.info("RECONFIGURE* changed {} to {}", property, newVal);
            return result;
        } catch (IllegalArgumentException e) {
            throw new ReconfigurationException(property, newVal, getConf().get(property), e);
        }
    }

    private String reconfDfsUsageParameters(String property, String newVal)
            throws ReconfigurationException {
        String result = null;
        try {
            LOG.info("Reconfiguring {} to {}", property, newVal);
            if (data == null) {
                LOG.debug("FsDatasetSpi has not been initialized.");
                throw new IOException("FsDatasetSpi has not been initialized");
            }
            if (property.equals(FS_DU_INTERVAL_KEY)) {
                long interval = (newVal == null ? FS_DU_INTERVAL_DEFAULT :
                        Long.parseLong(newVal));
                result = Long.toString(interval);
                List<FsVolumeImpl> volumeList = data.getVolumeList();
                for (FsVolumeImpl fsVolume : volumeList) {
                    Map<String, BlockPoolSlice> blockPoolSlices = fsVolume.getBlockPoolSlices();
                    for (BlockPoolSlice value : blockPoolSlices.values()) {
                        value.updateDfsUsageConfig(interval, null, null);
                    }
                }
            } else if (property.equals(FS_GETSPACEUSED_JITTER_KEY)) {
                long jitter = (newVal == null ? FS_GETSPACEUSED_JITTER_DEFAULT :
                        Long.parseLong(newVal));
                result = Long.toString(jitter);
                List<FsVolumeImpl> volumeList = data.getVolumeList();
                for (FsVolumeImpl fsVolume : volumeList) {
                    Map<String, BlockPoolSlice> blockPoolSlices = fsVolume.getBlockPoolSlices();
                    for (BlockPoolSlice value : blockPoolSlices.values()) {
                        value.updateDfsUsageConfig(null, jitter, null);
                    }
                }
            } else if (property.equals(FS_GETSPACEUSED_CLASSNAME)) {
                Class<? extends GetSpaceUsed> klass;
                if (newVal == null) {
                    if (Shell.WINDOWS) {
                        klass = DU.class;
                    } else {
                        klass = WindowsGetSpaceUsed.class;
                    }
                } else {
                    klass = Class.forName(newVal).asSubclass(GetSpaceUsed.class);
                }
                result = klass.getName();
                List<FsVolumeImpl> volumeList = data.getVolumeList();
                for (FsVolumeImpl fsVolume : volumeList) {
                    Map<String, BlockPoolSlice> blockPoolSlices = fsVolume.getBlockPoolSlices();
                    for (BlockPoolSlice value : blockPoolSlices.values()) {
                        value.updateDfsUsageConfig(null, null, klass);
                    }
                }
            }
            LOG.info("RECONFIGURE* changed {} to {}", property, newVal);
            return result;
        } catch (IllegalArgumentException | IOException | ClassNotFoundException e) {
            throw new ReconfigurationException(property, newVal, getConf().get(property), e);
        }
    }

    private String reconfDiskBalancerParameters(String property, String newVal)
            throws ReconfigurationException {
        String result = null;
        try {
            LOG.info("Reconfiguring {} to {}", property, newVal);
            if (property.equals(DFS_DISK_BALANCER_ENABLED)) {
                if (newVal != null && !newVal.equalsIgnoreCase("true")
                        && !newVal.equalsIgnoreCase("false")) {
                    throw new IllegalArgumentException("Not a valid Boolean value for " + property);
                }
                boolean enable = (newVal == null ? DFS_DISK_BALANCER_ENABLED_DEFAULT :
                        Boolean.parseBoolean(newVal));
                getDiskBalancer().setDiskBalancerEnabled(enable);
                result = Boolean.toString(enable);
            } else if (property.equals(DFS_DISK_BALANCER_PLAN_VALID_INTERVAL)) {
                if (newVal == null) {
                    // set to default
                    long defaultInterval = getConf().getTimeDuration(
                            DFS_DISK_BALANCER_PLAN_VALID_INTERVAL,
                            DFS_DISK_BALANCER_PLAN_VALID_INTERVAL_DEFAULT,
                            TimeUnit.MILLISECONDS);
                    getDiskBalancer().setPlanValidityInterval(defaultInterval);
                    result = DFS_DISK_BALANCER_PLAN_VALID_INTERVAL_DEFAULT;
                } else {
                    long newInterval = getConf()
                            .getTimeDurationHelper(DFS_DISK_BALANCER_PLAN_VALID_INTERVAL,
                                    newVal, TimeUnit.MILLISECONDS);
                    getDiskBalancer().setPlanValidityInterval(newInterval);
                    result = newVal;
                }
            }
            LOG.info("RECONFIGURE* changed {} to {}", property, result);
            return result;
        } catch (IllegalArgumentException | IOException e) {
            throw new ReconfigurationException(property, newVal, getConf().get(property), e);
        }
    }

    private String reconfSlowIoWarningThresholdParameters(String property, String newVal)
            throws ReconfigurationException {
        String result;
        try {
            LOG.info("Reconfiguring {} to {}", property, newVal);
            Preconditions.checkNotNull(dnConf, "DNConf has not been initialized.");
            long slowIoWarningThreshold = (newVal == null ?
                    DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_DEFAULT :
                    Long.parseLong(newVal));
            result = Long.toString(slowIoWarningThreshold);
            dnConf.setDatanodeSlowIoWarningThresholdMs(slowIoWarningThreshold);
            LOG.info("RECONFIGURE* changed {} to {}", property, newVal);
            return result;
        } catch (IllegalArgumentException e) {
            throw new ReconfigurationException(property, newVal, getConf().get(property), e);
        }
    }

    /** * Get a list of the keys of the re-configurable properties in 配置. */
    @Override // Reconfigurable
    public Collection<String> getReconfigurableProperties() {
        return RECONFIGURABLE_PROPERTIES;
    }

    /**
 * * * The ECN bit for the 数据节点. The 数据节点 should 返回:
     * <ul>
     *   <li>ECN.DISABLED when ECN is disabled.</li>
     *   <li>ECN.SUPPORTED when ECN is enabled but the DN still has capacity.</li>
     *   <li>ECN.CONGESTED when ECN is enabled and the DN is congested.</li>
     * </ul>
 */
    public PipelineAck.ECN getECN() {
        if (!pipelineSupportECN) {
            return PipelineAck.ECN.DISABLED;
        }
        double load = ManagementFactory.getOperatingSystemMXBean()
                .getSystemLoadAverage();
        return load > NUM_CORES * congestionRatio ? PipelineAck.ECN.CONGESTED :
                PipelineAck.ECN.SUPPORTED;
    }

    /**
 * * * The SLOW bit for the 数据节点 of the specific BlockPool.
     * The 数据节点 should 返回:
     * <ul>
     *   <li>SLOW.DISABLED when SLOW is disabled
     *   <li>SLOW.NORMAL when SLOW is enabled and DN is not slownode.</li>
     *   <li>SLOW.SLOW when SLOW is enabled and DN is slownode.</li>
     * </ul>
 */
    public PipelineAck.SLOW getSLOWByBlockPoolId(String bpId) {
        if (!pipelineSupportSlownode) {
            return PipelineAck.SLOW.DISABLED;
        }
        return isSlownodeByBlockPoolId(bpId) ? PipelineAck.SLOW.SLOW :
                PipelineAck.SLOW.NORMAL;
    }

    public FileIoProvider getFileIoProvider() {
        return fileIoProvider;
    }

    /** * Contains the StorageLocations for changed data volumes. */
    @VisibleForTesting
    static class ChangedVolumes {
        /** The 存储 locations of the newly added volumes. */
        List<StorageLocation> newLocations = Lists.newArrayList();
        /** The 存储 locations of the volumes that are removed. */
        List<StorageLocation> deactivateLocations = Lists.newArrayList();
        /** The unchanged locations that existed in the old 配置. */
        List<StorageLocation> unchangedLocations = Lists.newArrayList();
    }

    /**
     * 解析配置中的DFS_DATANODE_DATA_DIR新值，以检测存储卷的变化情况
     * 
     * 此方法用于在DataNode运行时动态刷新存储卷配置时，识别新增、移除或保持不变的存储卷
     * 通过比较新旧配置中的存储位置信息，确定需要添加的新卷和需要停用的旧卷
     * 
     * @param newVolumes 以逗号分隔的字符串，指定新的数据存储卷配置
     * @return 包含新增、停用和未变更存储卷信息的ChangedVolumes对象
     * @throws IOException 如果配置中未指定任何目录，或者尝试更改现有目录的存储类型
     */
    @VisibleForTesting
    ChangedVolumes parseChangedVolumes(String newVolumes) throws IOException {
        Configuration conf = new Configuration();
        conf.set(DFS_DATANODE_DATA_DIR_KEY, newVolumes);
        List<StorageLocation> newStorageLocations = getStorageLocations(conf);

        if (newStorageLocations.isEmpty()) {
            throw new IOException("No directory is specified.");
        }

        // Use the existing 存储 locations from the current conf
        // to detect new 存储 additions or removals.
        Map<String, StorageLocation> existingStorageLocations = new HashMap<>();
        for (StorageLocation loc : getStorageLocations(getConf())) {
            existingStorageLocations.put(loc.getNormalizedUri().toString(), loc);
        }

        ChangedVolumes results = new ChangedVolumes();
        results.newLocations.addAll(newStorageLocations);

        for (Iterator<Storage.StorageDirectory> it = storage.dirIterator();
             it.hasNext(); ) {
            Storage.StorageDirectory dir = it.next();
            boolean found = false;
            for (Iterator<StorageLocation> newLocationItr =
                 results.newLocations.iterator(); newLocationItr.hasNext(); ) {
                StorageLocation newLocation = newLocationItr.next();
                if (newLocation.matchesStorageDirectory(dir)) {
                    StorageLocation oldLocation = existingStorageLocations.get(
                            newLocation.getNormalizedUri().toString());
                    if (oldLocation != null &&
                            oldLocation.getStorageType() != newLocation.getStorageType()) {
                        throw new IOException("Changing storage type is not allowed.");
                    }
                    // Update the unchanged locations as this location
                    // from the new conf is really not a new one.
                    newLocationItr.remove();
                    results.unchangedLocations.add(newLocation);
                    found = true;
                    break;
                }
            }

            // New conf doesn't have the 存储 location which available in
            // the current 存储 locations. Add to the deactivateLocations list.
            if (!found) {
                LOG.info("Deactivation request received for active volume: {}",
                        dir.getRoot());
                results.deactivateLocations.add(
                        StorageLocation.parse(dir.getRoot().toString()));
            }
        }

        // Use the failed storage locations from the current conf
        // to detect removals in the new conf.
        if (getFSDataset().getNumFailedVolumes() > 0) {
            for (String failedStorageLocation : getFSDataset()
                    .getVolumeFailureSummary().getFailedStorageLocations()) {
                boolean found = false;
                for (Iterator<StorageLocation> newLocationItr =
                     results.newLocations.iterator(); newLocationItr.hasNext(); ) {
                    StorageLocation newLocation = newLocationItr.next();
                    if (newLocation.toString().equals(
                            failedStorageLocation)) {
                        // The failed storage is being re-added. DataNode#refreshVolumes()
                        // will take care of re-assessing it.
                        found = true;
                        break;
                    }
                }

                // New conf doesn't have this failed storage location.
                // Add to the deactivate locations list.
                if (!found) {
                    LOG.info("Deactivation request received for failed volume: {}",
                            failedStorageLocation);
                    results.deactivateLocations.add(StorageLocation.parse(
                            failedStorageLocation));
                }
            }
        }

        validateVolumesWithSameDiskTiering(results);

        return results;
    }

    /**
 * * * Check conflict with same 磁盘 tiering feature
     * and 抛出 异常.
     *
     * TODO: We can add feature to
     *   allow refreshing 卷 with capacity ratio,
     *   and solve the case of replacing 卷 on same mount.
 */
    private void validateVolumesWithSameDiskTiering(ChangedVolumes
                                                            changedVolumes) throws IOException {
        if (dnConf.getConf().getBoolean(DFS_DATANODE_ALLOW_SAME_DISK_TIERING,
                DFS_DATANODE_ALLOW_SAME_DISK_TIERING_DEFAULT)
                && data.getMountVolumeMap() != null) {
            // Check if mount already exist.
            for (StorageLocation location : changedVolumes.newLocations) {
                if (StorageType.allowSameDiskTiering(location.getStorageType())) {
                    File dir = new File(location.getUri());
                    // Get the first parent dir that exists to check 磁盘 mount point.
                    while (!dir.exists()) {
                        dir = dir.getParentFile();
                        if (dir == null) {
                            throw new IOException("Invalid path: "
                                    + location + ": directory does not exist");
                        }
                    }
                    DF df = new DF(dir, dnConf.getConf());
                    String mount = df.getMount();
                    if (data.getMountVolumeMap().hasMount(mount)) {
                        String errMsg = "Disk mount " + mount
                                + " already has volume, when trying to add "
                                + location + ". Please try removing mounts first"
                                + " or restart datanode.";
                        LOG.error(errMsg);
                        throw new IOException(errMsg);
                    }
                }
            }
        }
    }

    /**
 * * * Attempts to reload data volumes with new 配置.
     * @param newVolumes a comma separated string that specifies the data volumes.
     * @抛出 IOException on error. If an IOException is thrown, some new volumes
     * may have been successfully added and removed.
 */
    private void refreshVolumes(String newVolumes) throws IOException {
        // Add volumes for each 命名空间
        final List<NamespaceInfo> nsInfos = Lists.newArrayList();
        for (BPOfferService bpos : blockPoolManager.getAllNamenodeThreads()) {
            nsInfos.add(bpos.getNamespaceInfo());
        }
        synchronized (this) {
            Configuration conf = getConf();
            conf.set(DFS_DATANODE_DATA_DIR_KEY, newVolumes);
            ExecutorService service = null;
            int numOldDataDirs = dataDirs.size();
            ChangedVolumes changedVolumes = parseChangedVolumes(newVolumes);
            StringBuilder errorMessageBuilder = new StringBuilder();
            List<String> effectiveVolumes = Lists.newArrayList();
            for (StorageLocation sl : changedVolumes.unchangedLocations) {
                effectiveVolumes.add(sl.toString());
            }

            try {
                if (numOldDataDirs + getFSDataset().getNumFailedVolumes()
                        + changedVolumes.newLocations.size()
                        - changedVolumes.deactivateLocations.size() <= 0) {
                    throw new IOException("Attempt to remove all volumes.");
                }
                if (!changedVolumes.newLocations.isEmpty()) {
                    LOG.info("Adding new volumes: {}",
                            Joiner.on(",").join(changedVolumes.newLocations));

                    service = Executors
                            .newFixedThreadPool(changedVolumes.newLocations.size());
                    List<Future<IOException>> exceptions = Lists.newArrayList();

                    checkStorageState("refreshVolumes");
                    for (final StorageLocation location : changedVolumes.newLocations) {
                        exceptions.add(service.submit(new Callable<IOException>() {
                            @Override
                            public IOException call() {
                                try {
                                    data.addVolume(location, nsInfos);
                                } catch (IOException e) {
                                    return e;
                                }
                                return null;
                            }
                        }));
                    }

                    for (int i = 0; i < changedVolumes.newLocations.size(); i++) {
                        StorageLocation volume = changedVolumes.newLocations.get(i);
                        Future<IOException> ioExceptionFuture = exceptions.get(i);
                        try {
                            IOException ioe = ioExceptionFuture.get();
                            if (ioe != null) {
                                errorMessageBuilder.append(
                                        String.format("FAILED TO ADD: %s: %s%n",
                                                volume, ioe.getMessage()));
                                LOG.error("Failed to add volume: {}", volume, ioe);
                            } else {
                                effectiveVolumes.add(volume.toString());
                                LOG.info("Successfully added volume: {}", volume);
                            }
                        } catch (Exception e) {
                            errorMessageBuilder.append(
                                    String.format("FAILED to ADD: %s: %s%n", volume,
                                            e.toString()));
                            LOG.error("Failed to add volume: {}", volume, e);
                        }
                    }
                }

                try {
                    removeVolumes(changedVolumes.deactivateLocations);
                } catch (IOException e) {
                    errorMessageBuilder.append(e.getMessage());
                    LOG.error("Failed to remove volume", e);
                }

                if (errorMessageBuilder.length() > 0) {
                    throw new IOException(errorMessageBuilder.toString());
                }
            } finally {
                if (service != null) {
                    service.shutdown();
                }
                conf.set(DFS_DATANODE_DATA_DIR_KEY,
                        Joiner.on(",").join(effectiveVolumes));
                dataDirs = getStorageLocations(conf);
            }
        }
    }

    /**
 * * * Remove volumes from 数据节点.
     * See {@link #removeVolumes(Collection, boolean)} for details.
     *
     * @param locations the StorageLocations of the volumes to be removed.
     * @抛出 IOException
 */
    private void removeVolumes(final Collection<StorageLocation> locations)
            throws IOException {
        if (locations.isEmpty()) {
            return;
        }
        removeVolumes(locations, true);
    }

    /**
 * * * Remove volumes from 数据节点.
     *
     * It does three things:
     * <li>
     *   <ul>Remove volumes and 数据块 info from FsDataset.</ul>
     *   <ul>Remove volumes from DataStorage.</ul>
     *   <ul>Reset 配置 DATA_DIR and {@link #dataDirs} to represent
     *   active volumes.</ul>
     * </li>
     * @param storageLocations the absolute 路径 of volumes.
     * @param clearFailure if true, clears the failure information related to the
     *                     volumes.
     * @抛出 IOException
 */
    private synchronized void removeVolumes(
            final Collection<StorageLocation> storageLocations, boolean clearFailure)
            throws IOException {
        if (storageLocations.isEmpty()) {
            return;
        }

        LOG.info(String.format("Deactivating volumes (clear failure=%b): %s",
                clearFailure, Joiner.on(",").join(storageLocations)));

        IOException ioe = null;
        checkStorageState("removeVolumes");
        // Remove volumes and 数据块 infos from FsDataset.
        data.removeVolumes(storageLocations, clearFailure);

        // Remove volumes from DataStorage.
        try {
            storage.removeVolumes(storageLocations);
        } catch (IOException e) {
            ioe = e;
        }

        // Set 配置 and dataDirs to reflect 卷 changes.
        for (Iterator<StorageLocation> it = dataDirs.iterator(); it.hasNext(); ) {
            StorageLocation loc = it.next();
            if (storageLocations.contains(loc)) {
                it.remove();
            }
        }
        getConf().set(DFS_DATANODE_DATA_DIR_KEY, Joiner.on(",").join(dataDirs));

        if (ioe != null) {
            throw ioe;
        }
    }

    private synchronized void setClusterId(final String nsCid, final String bpid
    ) throws IOException {
        if (clusterId != null && !clusterId.equals(nsCid)) {
            throw new IOException("Cluster IDs not matched: dn cid=" + clusterId
                    + " but ns cid=" + nsCid + "; bpid=" + bpid);
        }
        // else
        clusterId = nsCid;
    }

    /**
 * * * Returns the hostname for this 数据节点. If the hostname is not
     * explicitly configured in the given config, then it is determined
     * via the DNS 类.
     *
     * @param config 配置
     * @返回 the hostname (NB: may not be a FQDN)
     * @抛出 UnknownHostException if the dfs.数据节点.dns.接口
     *    option is used and the hostname can not be determined
 */
    private static String getHostName(Configuration config)
            throws UnknownHostException {
        String name = config.get(DFS_DATANODE_HOST_NAME_KEY);
        if (name == null) {
            String dnsInterface = config.get(
                    CommonConfigurationKeys.HADOOP_SECURITY_DNS_INTERFACE_KEY);
            String nameServer = config.get(
                    CommonConfigurationKeys.HADOOP_SECURITY_DNS_NAMESERVER_KEY);
            boolean fallbackToHosts = false;

            if (dnsInterface == null) {
                // 尝试 the legacy 配置 keys.
                dnsInterface = config.get(DFS_DATANODE_DNS_INTERFACE_KEY);
                nameServer = config.get(DFS_DATANODE_DNS_NAMESERVER_KEY);
            } else {
                // If HADOOP_SECURITY_DNS_* is set then also attempt hosts 文件
                // resolution if DNS fails. We will not use hosts 文件 resolution
                // by default to avoid breaking existing clusters.
                fallbackToHosts = true;
            }

            name = DNS.getDefaultHost(dnsInterface, nameServer, fallbackToHosts);
        }
        return name;
    }

    /**
 * * * @see DFSUtil#getHttpPolicy(org.apache.hadoop.conf.配置)
     * for information related to the different 配置 options and
     * HTTP（超文本传输协议）（超文本传输协议） Policy is decided.
 */
    private void startInfoServer()
            throws IOException {
        // SecureDataNodeStarter will bind the privileged port to the 通道 if
        // the DN is started by JSVC, pass it along.
        ServerSocketChannel httpServerChannel = secureResources != null ?
                secureResources.getHttpServerChannel() : null;

        httpServer = new DatanodeHttpServer(getConf(), this, httpServerChannel);
        httpServer.start();
        if (httpServer.getHttpAddress() != null) {
            infoPort = httpServer.getHttpAddress().getPort();
        }
        if (httpServer.getHttpsAddress() != null) {
            infoSecurePort = httpServer.getHttpsAddress().getPort();
        }
    }

    private void startPlugins(Configuration conf) {
        try {
            plugins = conf.getInstances(DFS_DATANODE_PLUGINS_KEY,
                    ServicePlugin.class);
        } catch (RuntimeException e) {
            String pluginsValue = conf.get(DFS_DATANODE_PLUGINS_KEY);
            LOG.error("Unable to load DataNode plugins. " +
                            "Specified list of plugins: {}",
                    pluginsValue, e);
            throw e;
        }
        for (ServicePlugin p : plugins) {
            try {
                p.start(this);
                LOG.info("Started plug-in {}", p);
            } catch (Throwable t) {
                LOG.warn("ServicePlugin {} could not be started", p, t);
            }
        }
    }

    private void initIpcServer() throws IOException {
        InetSocketAddress ipcAddr = NetUtils.createSocketAddr(
                getConf().getTrimmed(DFS_DATANODE_IPC_ADDRESS_KEY));

        // 添加数据节点实现的所有远程过程调用（RPC）协议
        RPC.setProtocolEngine(getConf(), ClientDatanodeProtocolPB.class,
                ProtobufRpcEngine2.class);
        ClientDatanodeProtocolServerSideTranslatorPB clientDatanodeProtocolXlator =
                new ClientDatanodeProtocolServerSideTranslatorPB(this);
        BlockingService service = ClientDatanodeProtocolService
                .newReflectiveBlockingService(clientDatanodeProtocolXlator);
        ipcServer = new RPC.Builder(getConf())
                .setProtocol(ClientDatanodeProtocolPB.class)
                .setInstance(service)
                .setBindAddress(ipcAddr.getHostName())
                .setPort(ipcAddr.getPort())
                .setNumHandlers(
                        getConf().getInt(DFS_DATANODE_HANDLER_COUNT_KEY,
                                DFS_DATANODE_HANDLER_COUNT_DEFAULT)).setVerbose(false)
                .setSecretManager(blockPoolTokenSecretManager).build();

        ReconfigurationProtocolServerSideTranslatorPB reconfigurationProtocolXlator
                = new ReconfigurationProtocolServerSideTranslatorPB(this);
        service = ReconfigurationProtocolService
                .newReflectiveBlockingService(reconfigurationProtocolXlator);
        DFSUtil.addInternalPBProtocol(getConf(), ReconfigurationProtocolPB.class, service,
                ipcServer);

        InterDatanodeProtocolServerSideTranslatorPB interDatanodeProtocolXlator =
                new InterDatanodeProtocolServerSideTranslatorPB(this);
        service = InterDatanodeProtocolService
                .newReflectiveBlockingService(interDatanodeProtocolXlator);
        DFSUtil.addInternalPBProtocol(getConf(), InterDatanodeProtocolPB.class, service,
                ipcServer);

        LOG.info("Opened IPC server at {}", ipcServer.getListenerAddress());

        // set service-level authorization security policy
        if (getConf().getBoolean(
                CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
            ipcServer.refreshServiceAcl(getConf(), new HDFSPolicyProvider());
        }
    }

    /** Check whether the current user is in the superuser group. */
    private void checkSuperuserPrivilege() throws IOException, AccessControlException {
        if (!isPermissionEnabled) {
            return;
        }
        // 尝试 to get the ugi in the RPC（远程过程调用） call.
        UserGroupInformation callerUgi = Server.getRemoteUser();
        if (callerUgi == null) {
            // This is not from RPC（远程过程调用）.
            callerUgi = UserGroupInformation.getCurrentUser();
        }

        // Is this by the DN user itself?
        assert dnUserName != null;
        if (callerUgi.getUserName().equals(dnUserName)) {
            return;
        }

        // Is the user a member of the super group?
        if (callerUgi.getGroupsSet().contains(supergroup)) {
            return;
        }
        // Not a superuser.
        throw new AccessControlException();
    }

    private void shutdownPeriodicScanners() {
        shutdownDirectoryScanner();
        blockScanner.removeAllVolumeScanners();
    }

    /** * See {@link DirectoryScanner} */
    private synchronized void initDirectoryScanner(Configuration conf) {
        if (directoryScanner != null) {
            return;
        }
        String reason = null;
        if (conf.getTimeDuration(DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY,
                DFS_DATANODE_DIRECTORYSCAN_INTERVAL_DEFAULT, TimeUnit.SECONDS) < 0) {
            reason = "verification is turned off by configuration";
        } else if ("SimulatedFSDataset".equals(data.getClass().getSimpleName())) {
            reason = "verifcation is not supported by SimulatedFSDataset";
        }
        if (reason == null) {
            directoryScanner = new DirectoryScanner(data, conf);
            directoryScanner.start();
        } else {
            LOG.warn("Periodic Directory Tree Verification scan " +
                            "is disabled because {}",
                    reason);
        }
    }

    private synchronized void shutdownDirectoryScanner() {
        if (directoryScanner != null) {
            directoryScanner.shutdown();
        }
    }

    /**
 * * * Initilizes {@link DiskBalancer}.
     * @param  data - FSDataSet
     * @param conf - Config
 */
    private void initDiskBalancer(FsDatasetSpi data,
                                  Configuration conf) {
        if (this.diskBalancer != null) {
            return;
        }

        DiskBalancer.BlockMover mover = new DiskBalancer.DiskBalancerMover(data,
                conf);
        this.diskBalancer = new DiskBalancer(getDatanodeUuid(), conf, mover);
    }

    /** * Shutdown 磁盘 均衡器. */
    private void shutdownDiskBalancer() {
        if (this.diskBalancer != null) {
            this.diskBalancer.shutdown();
            this.diskBalancer = null;
        }
    }

    private void initDataXceiver() throws IOException {
        // find free port or use privileged port provided
        TcpPeerServer tcpPeerServer;
        if (secureResources != null) {
            tcpPeerServer = new TcpPeerServer(secureResources);
        } else {
            int backlogLength = getConf().getInt(
                    CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_KEY,
                    CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_DEFAULT);
            tcpPeerServer = new TcpPeerServer(dnConf.socketWriteTimeout,
                    DataNode.getStreamingAddr(getConf()), backlogLength);
        }
        if (dnConf.getTransferSocketRecvBufferSize() > 0) {
            tcpPeerServer.setReceiveBufferSize(
                    dnConf.getTransferSocketRecvBufferSize());
        }
        streamingAddr = tcpPeerServer.getStreamingAddr();
        LOG.info("Opened streaming server at {}", streamingAddr);
        this.threadGroup = new ThreadGroup("dataXceiverServer");
        xserver = new DataXceiverServer(tcpPeerServer, getConf(), this);
        this.dataXceiverServer = new Daemon(threadGroup, xserver);
        this.threadGroup.setDaemon(true); // auto destroy when empty

        if (getConf().getBoolean(
                HdfsClientConfigKeys.Read.ShortCircuit.KEY,
                HdfsClientConfigKeys.Read.ShortCircuit.DEFAULT) ||
                getConf().getBoolean(
                        HdfsClientConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC,
                        HdfsClientConfigKeys
                                .DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC_DEFAULT)) {
            DomainPeerServer domainPeerServer =
                    getDomainPeerServer(getConf(), streamingAddr.getPort());
            if (domainPeerServer != null) {
                this.localDataXceiverServer = new Daemon(threadGroup,
                        new DataXceiverServer(domainPeerServer, getConf(), this));
                LOG.info("Listening on UNIX domain socket: {}",
                        domainPeerServer.getBindPath());
            }
        }
        this.shortCircuitRegistry = new ShortCircuitRegistry(getConf());
    }

    private static DomainPeerServer getDomainPeerServer(Configuration conf,
                                                        int port) throws IOException {
        String domainSocketPath =
                conf.getTrimmed(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
                        DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_DEFAULT);
        if (domainSocketPath.isEmpty()) {
            if (conf.getBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY,
                    HdfsClientConfigKeys.Read.ShortCircuit.DEFAULT) &&
                    (!conf.getBoolean(HdfsClientConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL,
                            HdfsClientConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL_DEFAULT))) {
                LOG.warn("Although short-circuit local reads are configured, " +
                                "they are disabled because you didn't configure {}",
                        DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY);
            }
            return null;
        }
        if (DomainSocket.getLoadingFailureReason() != null) {
            throw new RuntimeException("Although a UNIX domain socket " +
                    "path is configured as " + domainSocketPath + ", we cannot " +
                    "start a localDataXceiverServer because " +
                    DomainSocket.getLoadingFailureReason());
        }
        DomainPeerServer domainPeerServer =
                new DomainPeerServer(domainSocketPath, port);
        int recvBufferSize = conf.getInt(
                DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY,
                DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT);
        if (recvBufferSize > 0) {
            domainPeerServer.setReceiveBufferSize(recvBufferSize);
        }
        return domainPeerServer;
    }

    // calls specific to BP
    public void notifyNamenodeReceivedBlock(ExtendedBlock block, String delHint,
                                            String storageUuid, boolean isOnTransientStorage) {
        BPOfferService bpos = blockPoolManager.get(block.getBlockPoolId());
        if (bpos != null) {
            bpos.notifyNamenodeReceivedBlock(block, delHint, storageUuid,
                    isOnTransientStorage);
        } else {
            LOG.error("Cannot find BPOfferService for reporting block received " +
                    "for bpid={}", block.getBlockPoolId());
        }
    }

    /**
     * 通知NameNode当前DataNode正在接收一个数据块
     * 
     * 当DataNode开始接收客户端或其他DataNode发送的数据块时，调用此方法通知对应的NameNode
     * 这是块创建过程中的重要步骤，让NameNode能够跟踪块的复制状态
     * 
     * @param block 正在接收的块，包含块ID、块池ID和生成戳等信息
     * @param storageUuid 存储该块的存储卷UUID
     */
    protected void notifyNamenodeReceivingBlock(
            ExtendedBlock block, String storageUuid) {
        // 根据块池ID获取对应的BPOfferService，用于与NameNode通信
        BPOfferService bpos = blockPoolManager.get(block.getBlockPoolId());
        if (bpos != null) {
            // 通知BPOfferService转发块接收信息给NameNode
            bpos.notifyNamenodeReceivingBlock(block, storageUuid);
        } else {
            // 如果找不到对应的BPOfferService，记录错误日志
            LOG.error("Cannot find BPOfferService for reporting block receiving " +
                    "for bpid={}", block.getBlockPoolId());
        }
    }

    /**
     * 通知NameNode当前DataNode已删除指定的数据块
     * 
     * 当DataNode根据NameNode的指令删除一个数据块后，调用此方法通知NameNode块已成功删除
     * 这确保了NameNode的元数据与实际存储在DataNode上的块信息保持一致
     * 
     * @param block 已删除的块，包含块ID、块池ID和生成戳等信息
     * @param storageUuid 存储该块的存储卷UUID
     */
    public void notifyNamenodeDeletedBlock(ExtendedBlock block, String storageUuid) {
        // 根据块池ID获取对应的BPOfferService，用于与NameNode通信
        BPOfferService bpos = blockPoolManager.get(block.getBlockPoolId());
        if (bpos != null) {
            // 通知BPOfferService转发块删除信息给NameNode
            bpos.notifyNamenodeDeletedBlock(block, storageUuid);
        } else {
            // 如果找不到对应的BPOfferService，记录错误日志
            LOG.error("Cannot find BPOfferService for reporting block deleted for bpid="
                    + block.getBlockPoolId());
        }
    }

    /**
     * 报告当前DataNode上托管的损坏块给NameNode
     * 
     * 当DataNode检测到本地存储的数据块损坏时，调用此方法向NameNode报告
     * NameNode收到报告后会启动块恢复或重新复制流程，确保数据的完整性
     * 
     * @param block 损坏的块，包含块ID、块池ID和生成戳等信息
     * @throws IOException 如果报告过程中发生IO异常
     */
    public void reportBadBlocks(ExtendedBlock block) throws IOException {
        // 获取存储该块的卷
        FsVolumeSpi volume = getFSDataset().getVolume(block);
        if (volume == null) {
            // 如果找不到对应的存储卷，记录警告日志并返回
            LOG.warn("Cannot find FsVolumeSpi to report bad block: {}", block);
            return;
        }
        // 调用重载方法报告损坏块
        reportBadBlocks(block, volume);
    }

    /**
     * 报告当前DataNode上托管的损坏块给NameNode（带存储卷信息）
     * 
     * 这是实际执行损坏块报告的核心方法，提供了更详细的存储卷信息
     * 
     * @param block 损坏的块，包含块ID、块池ID和生成戳等信息
     * @param volume 存储该块的卷，不能为空
     * @throws IOException 如果报告过程中发生IO异常
     */
    public void reportBadBlocks(ExtendedBlock block, FsVolumeSpi volume)
            throws IOException {
        // 获取与该块相关联的BPOfferService
        BPOfferService bpos = getBPOSForBlock(block);
        // 通过BPOfferService向NameNode报告损坏块，并提供存储ID和存储类型信息
        bpos.reportBadBlocks(
                block, volume.getStorageID(), volume.getStorageType());
    }

    /**
     * 报告远程DataNode上的损坏块给NameNode
     * 
     * 当DataNode从其他DataNode接收数据时检测到块损坏，可以通过此方法报告
     * 这有助于NameNode及时了解整个集群中数据块的健康状态
     * 
     * @param srcDataNode 托管损坏块的远程DataNode信息
     * @param block 损坏的块本身
     * @throws IOException 如果报告过程中发生IO异常
     */
    public void reportRemoteBadBlock(DatanodeInfo srcDataNode, ExtendedBlock block)
            throws IOException {
        // 获取与该块相关联的BPOfferService
        BPOfferService bpos = getBPOSForBlock(block);
        // 通过BPOfferService向NameNode报告远程损坏块
        bpos.reportRemoteBadBlock(srcDataNode, block);
    }

    public void reportCorruptedBlocks(
            DFSUtilClient.CorruptedBlocks corruptedBlocks) throws IOException {
        Map<ExtendedBlock, Set<DatanodeInfo>> corruptionMap =
                corruptedBlocks.getCorruptionMap();
        if (corruptionMap != null) {
            for (Map.Entry<ExtendedBlock, Set<DatanodeInfo>> entry :
                    corruptionMap.entrySet()) {
                for (DatanodeInfo dnInfo : entry.getValue()) {
                    reportRemoteBadBlock(dnInfo, entry.getKey());
                }
            }
        }
    }

    /**
     * Return the BPOfferService instance corresponding to the given block.
     * @return the BPOS
     * @throws IOException if no such BPOS can be found
     */
    private BPOfferService getBPOSForBlock(ExtendedBlock block)
            throws IOException {
        Preconditions.checkNotNull(block);
        BPOfferService bpos = blockPoolManager.get(block.getBlockPoolId());
        if (bpos == null) {
            throw new IOException("cannot locate OfferService thread for bp=" +
                    block.getBlockPoolId());
        }
        return bpos;
    }

    // used only for testing
    @VisibleForTesting
    public void setHeartbeatsDisabledForTests(
            boolean heartbeatsDisabledForTests) {
        this.heartbeatsDisabledForTests = heartbeatsDisabledForTests;
    }

    @VisibleForTesting
    boolean areHeartbeatsDisabledForTests() {
        return this.heartbeatsDisabledForTests;
    }

    @VisibleForTesting
    void setIBRDisabledForTest(boolean disabled) {
        this.ibrDisabledForTests = disabled;
    }

    @VisibleForTesting
    boolean areIBRDisabledForTests() {
        return this.ibrDisabledForTests;
    }

    void setCacheReportsDisabledForTest(boolean disabled) {
        this.cacheReportsDisabledForTests = disabled;
    }

    @VisibleForTesting
    boolean areCacheReportsDisabledForTests() {
        return this.cacheReportsDisabledForTests;
    }

    /**
     * 使用指定的配置启动数据节点
     * 
     * 如果配置中的DFS_DATANODE_FSDATASET_FACTORY_KEY属性已设置，
     * 则会创建一个基于模拟存储的数据节点
     *
     * @param dataDirectories - 仅用于非模拟存储数据节点
     * @throws IOException 如果启动过程中发生IO异常
     */
    void startDataNode(List<StorageLocation> dataDirectories,
                       SecureResources resources
    ) throws IOException {

        // 为DataNode中所有块池(BPs)设置全局配置
        this.secureResources = resources;
        synchronized (this) {
            this.dataDirs = dataDirectories;
        }
        this.dnConf = new DNConf(this);
        // 检查安全配置
        checkSecureConfig(dnConf, getConf(), resources);

        if (dnConf.maxLockedMemory > 0) {
            if (!NativeIO.POSIX.getCacheManipulator().verifyCanMlock()) {
                throw new RuntimeException(String.format(
                        "Cannot start datanode because the configured max locked memory" +
                                " size (%s) is greater than zero and native code is not available.",
                        DFS_DATANODE_MAX_LOCKED_MEMORY_KEY));
            }
            if (Path.WINDOWS) {
                NativeIO.Windows.extendWorkingSetSize(dnConf.maxLockedMemory);
            } else {
                long ulimit = NativeIO.POSIX.getCacheManipulator().getMemlockLimit();
                if (dnConf.maxLockedMemory > ulimit) {
                    throw new RuntimeException(String.format(
                            "Cannot start datanode because the configured max locked memory" +
                                    " size (%s) of %d bytes is more than the datanode's available" +
                                    " RLIMIT_MEMLOCK ulimit of %d bytes.",
                            DFS_DATANODE_MAX_LOCKED_MEMORY_KEY,
                            dnConf.maxLockedMemory,
                            ulimit));
                }
            }
        }
        LOG.info("Starting DataNode with maxLockedMemory = {}",
                dnConf.maxLockedMemory);

        int volFailuresTolerated = dnConf.getVolFailuresTolerated();
        int volsConfigured = dnConf.getVolsConfigured();
        if (volFailuresTolerated < MAX_VOLUME_FAILURE_TOLERATED_LIMIT
                || volFailuresTolerated >= volsConfigured) {
            throw new HadoopIllegalArgumentException("Invalid value configured for "
                    + "dfs.datanode.failed.volumes.tolerated - " + volFailuresTolerated
                    + ". Value configured is either less than -1 or >= "
                    + "to the number of configured volumes (" + volsConfigured + ").");
        }
        // 构建数据存储组件
        storage = new DataStorage();

        // global DN settings
        registerMXBean();
        // 初始化数据接收器服务器
        initDataXceiver();
        // 启动DataNode HTTP服务器
        startInfoServer();
        // 启动JVM暂停监控器
        pauseMonitor = new JvmPauseMonitor();
        pauseMonitor.init(getConf());
        pauseMonitor.start();

        // BlockPoolTokenSecretManager is required to create ipc server.
        this.blockPoolTokenSecretManager = new BlockPoolTokenSecretManager();

        // Login is done by now. Set the DN user name.
        dnUserName = UserGroupInformation.getCurrentUser().getUserName();
        LOG.info("dnUserName = {}", dnUserName);
        LOG.info("supergroup = {}", supergroup);
        initIpcServer();

        metrics = DataNodeMetrics.create(getConf(), getDisplayName());
        peerMetrics = dnConf.peerStatsEnabled ?
                DataNodePeerMetrics.create(getDisplayName(), getConf()) : null;
        metrics.getJvmMetrics().setPauseMonitor(pauseMonitor);

        ecWorker = new ErasureCodingWorker(getConf(), this);
        // 初始化块恢复工作器
        blockRecoveryWorker = new BlockRecoveryWorker(this);

        blockPoolManager = new BlockPoolManager(this);
        // 根据配置刷新NameNode连接
        blockPoolManager.refreshNamenodes(getConf());

        // Create the ReadaheadPool from the DataNode context so we can
        // exit without having to explicitly shutdown its thread pool.
        readaheadPool = ReadaheadPool.getInstance();
        saslClient = new SaslDataTransferClient(dnConf.getConf(),
                dnConf.saslPropsResolver, dnConf.trustedChannelResolver);
        saslServer = new SaslDataTransferServer(dnConf, blockPoolTokenSecretManager);
        startMetricsLogger();

        if (dnConf.diskStatsEnabled) {
            diskMetrics = new DataNodeDiskMetrics(this,
                    dnConf.outliersReportIntervalMs, getConf());
        }
    }

    /**
     * Checks if the DataNode has a secure configuration if security is enabled.
     * There are 2 possible configurations that are considered secure:
     * 1. The server has bound to privileged ports for RPC and HTTP via
     *   SecureDataNodeStarter.
     * 2. The configuration enables SASL on DataTransferProtocol and HTTPS (no
     *   plain HTTP) for the HTTP server.  The SASL handshake guarantees
     *   authentication of the RPC server before a client transmits a secret, such
     *   as a block access token.  Similarly, SSL guarantees authentication of the
     *   HTTP server before a client transmits a secret, such as a delegation
     *   token.
     * It is not possible to run with both privileged ports and SASL on
     * DataTransferProtocol.  For backwards-compatibility, the connection logic
     * must check if the target port is a privileged port, and if so, skip the
     * SASL handshake.
     *
     * @param dnConf DNConf to check
     * @param conf Configuration to check
     * @param resources SecuredResources obtained for DataNode
     * @throws RuntimeException if security enabled, but configuration is insecure
     */
    private static void checkSecureConfig(DNConf dnConf, Configuration conf,
                                          SecureResources resources) throws RuntimeException {
        if (!UserGroupInformation.isSecurityEnabled()) {
            return;
        }

        // Abort out of inconsistent state if Kerberos is enabled
        // but block access tokens are not enabled.
        boolean isEnabled = conf.getBoolean(
                DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY,
                DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_DEFAULT);
        if (!isEnabled) {
            String errMessage = "Security is enabled but block access tokens " +
                    "(via " + DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY + ") " +
                    "aren't enabled. This may cause issues " +
                    "when clients attempt to connect to a DataNode. Aborting DataNode";
            throw new RuntimeException(errMessage);
        }

        if (dnConf.getIgnoreSecurePortsForTesting()) {
            return;
        }

        if (resources != null) {
            final boolean httpSecured = resources.isHttpPortPrivileged()
                    || DFSUtil.getHttpPolicy(conf) == HttpConfig.Policy.HTTPS_ONLY;
            final boolean rpcSecured = resources.isRpcPortPrivileged()
                    || resources.isSaslEnabled();

            // Allow secure 数据节点 to startup if:
            // 1. HTTP（超文本传输协议） is secure.
            // 2. RPC（远程过程调用） is secure
            if (rpcSecured && httpSecured) {
                return;
            }
        } else {
            // Handle cases when SecureDataNodeStarter#getSecureResources is not
            // invoked
            SaslPropertiesResolver saslPropsResolver = dnConf.getSaslPropsResolver();
            if (saslPropsResolver != null &&
                    DFSUtil.getHttpPolicy(conf) == HttpConfig.Policy.HTTPS_ONLY) {
                return;
            }
        }

        throw new RuntimeException("Cannot start secure DataNode due to incorrect "
                + "config. See https:// cwiki.apache.org/confluence/display/HADOOP/"
                + "Secure+DataNode for details.");
    }

    public static String generateUuid() {
        return UUID.randomUUID().toString();
    }

    public SaslDataTransferClient getSaslClient() {
        return saslClient;
    }

    /**
 * * * Verify that the DatanodeUuid has been initialized. If this is a new
     * 数据节点 then we generate a new 数据节点 Uuid and persist it to 磁盘.
     *
     * @抛出 IOException
 */
    synchronized void checkDatanodeUuid() throws IOException {
        if (storage.getDatanodeUuid() == null) {
            storage.setDatanodeUuid(generateUuid());
            storage.writeAll();
            LOG.info("Generated and persisted new Datanode UUID {}",
                    storage.getDatanodeUuid());
        }
    }

    /**
 * * * Create a DatanodeRegistration for a specific 数据块 pool.
     * @param nsInfo the 命名空间 info from the first part of the NN handshake
 */
    DatanodeRegistration createBPRegistration(NamespaceInfo nsInfo) {
        StorageInfo storageInfo = storage.getBPStorage(nsInfo.getBlockPoolID());
        if (storageInfo == null) {
            // it's null in the case of SimulatedDataSet
            storageInfo = new StorageInfo(
                    DataNodeLayoutVersion.getCurrentLayoutVersion(),
                    nsInfo.getNamespaceID(), nsInfo.clusterID, nsInfo.getCTime(),
                    NodeType.DATA_NODE);
        }

        DatanodeID dnId = new DatanodeID(
                streamingAddr.getAddress().getHostAddress(), hostName,
                storage.getDatanodeUuid(), getXferPort(), getInfoPort(),
                infoSecurePort, getIpcPort());
        return new DatanodeRegistration(dnId, storageInfo,
                new ExportedBlockKeys(), VersionInfo.getVersion());
    }

    /**
 * * Check that the registration returned from a 名称节点 is consistent
     * with the information in the 存储. If the 存储 is fresh/unformatted,
     * sets the 存储 ID based on this registration.
     * Also updates the 数据块 pool's 状态 in the secret manager.
 */
    synchronized void bpRegistrationSucceeded(DatanodeRegistration bpRegistration,
                                              String blockPoolId) throws IOException {
        id = bpRegistration;

        if (!storage.getDatanodeUuid().equals(bpRegistration.getDatanodeUuid())) {
            throw new IOException("Inconsistent Datanode IDs. Name-node returned "
                    + bpRegistration.getDatanodeUuid()
                    + ". Expecting " + storage.getDatanodeUuid());
        }

        registerBlockPoolWithSecretManager(bpRegistration, blockPoolId);
    }

    /**
 * * * After the 数据块 pool has contacted the NN, registers that 数据块 pool
     * with the secret manager, updating it with the secrets provided by the NN.
     * @抛出 IOException on error
 */
    private synchronized void registerBlockPoolWithSecretManager(
            DatanodeRegistration bpRegistration, String blockPoolId) throws IOException {
        ExportedBlockKeys keys = bpRegistration.getExportedKeys();
        if (!hasAnyBlockPoolRegistered) {
            hasAnyBlockPoolRegistered = true;
            isBlockTokenEnabled = keys.isBlockTokenEnabled();
        } else {
            if (isBlockTokenEnabled != keys.isBlockTokenEnabled()) {
                throw new RuntimeException("Inconsistent configuration of block access"
                        + " tokens. Either all block pools must be configured to use block"
                        + " tokens, or none may be.");
            }
        }
        if (!isBlockTokenEnabled) return;

        if (!blockPoolTokenSecretManager.isBlockPoolRegistered(blockPoolId)) {
            long blockKeyUpdateInterval = keys.getKeyUpdateInterval();
            long blockTokenLifetime = keys.getTokenLifetime();
            LOG.info("Block token params received from NN: " +
                            "for block pool {} keyUpdateInterval={} min(s), " +
                            "tokenLifetime={} min(s)",
                    blockPoolId, blockKeyUpdateInterval / (60 * 1000),
                    blockTokenLifetime / (60 * 1000));
            final boolean enableProtobuf = getConf().getBoolean(
                    DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_PROTOBUF_ENABLE,
                    DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_PROTOBUF_ENABLE_DEFAULT);
            final BlockTokenSecretManager secretMgr =
                    new BlockTokenSecretManager(0, blockTokenLifetime, blockPoolId,
                            dnConf.encryptionAlgorithm, enableProtobuf);
            blockPoolTokenSecretManager.addBlockPool(blockPoolId, secretMgr);
        }
    }

    /** * Remove the given 数据块 pool from the 数据块 scanner, dataset, and 存储. */
    void shutdownBlockPool(BPOfferService bpos) {
        blockPoolManager.remove(bpos);
        if (bpos.hasBlockPoolId()) {
            // Possible that this is shutting down before successfully
            // registering anywhere. If that's the case, we wouldn't have
            // a 数据块 pool id
            String bpId = bpos.getBlockPoolId();

            if (blockScanner.hasAnyRegisteredScanner()) {
                blockScanner.disableBlockPoolId(bpId);
            }

            if (data != null) {
                data.shutdownBlockPool(bpId);
            }

            if (storage != null) {
                storage.removeBlockPoolStorage(bpId);
            }
        }

    }

    /**
 * * * One of the 数据块 Pools has successfully connected to its NN.
     * This initializes the local 存储 for that 数据块 pool,
     * checks consistency of the NN's cluster ID, etc.
     *
     * If this is the first 数据块 pool to register, this also initializes
     * the 数据节点-scoped 存储.
     *
     * @param bpos 数据块 pool offer service
     * @抛出 IOException if the NN is inconsistent with the local 存储.
 */
    void initBlockPool(BPOfferService bpos) throws IOException {
        NamespaceInfo nsInfo = bpos.getNamespaceInfo();
        if (nsInfo == null) {
            throw new IOException("NamespaceInfo not found: Block pool " + bpos
                    + " should have retrieved namespace info before initBlockPool.");
        }

        setClusterId(nsInfo.clusterID, nsInfo.getBlockPoolID());

        // Register the new block pool with the BP manager.
        blockPoolManager.addBlockPool(bpos);

        // In the case that this is the first block pool to connect, initialize
        // the dataset, block scanners, etc.
        initStorage(nsInfo);

        try {
            data.addBlockPool(nsInfo.getBlockPoolID(), getConf());
        } catch (AddBlockPoolException e) {
            handleAddBlockPoolError(e);
        }
        // HDFS-14993: check disk after add the block pool info.
        checkDiskError();

        blockScanner.enableBlockPoolId(bpos.getBlockPoolId());
        initDirectoryScanner(getConf());
        initDiskBalancer(data, getConf());
    }

    /**
     * Handles an AddBlockPoolException object thrown from
     * {@link org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeList#
     * addBlockPool}. Will ensure that all volumes that encounted a
     * AddBlockPoolException are removed from the DataNode and marked as failed
     * volumes in the same way as a runtime volume failure.
     *
     * @param e this exception is a container for all IOException objects caught
     *          in FsVolumeList#addBlockPool.
     */
    private void handleAddBlockPoolError(AddBlockPoolException e)
            throws IOException {
        Map<FsVolumeSpi, IOException> unhealthyDataDirs =
                e.getFailingVolumes();
        if (unhealthyDataDirs != null && !unhealthyDataDirs.isEmpty()) {
            handleVolumeFailures(unhealthyDataDirs.keySet());
        } else {
            LOG.debug("HandleAddBlockPoolError called with empty exception list");
        }
    }

    List<BPOfferService> getAllBpOs() {
        return blockPoolManager.getAllNamenodeThreads();
    }

    BPOfferService getBPOfferService(String bpid) {
        return blockPoolManager.get(bpid);
    }

    public int getBpOsCount() {
        return blockPoolManager.getAllNamenodeThreads().size();
    }

    /**
     * 初始化数据存储组件({@link #data})。初始化过程只执行一次，即在与第一个名称节点完成握手时进行。
     */
    private void initStorage(final NamespaceInfo nsInfo) throws IOException {
        final FsDatasetSpi.Factory<? extends FsDatasetSpi<?>> factory
                = FsDatasetSpi.Factory.getFactory(getConf());

        if (!factory.isSimulated()) {
            final StartupOption startOpt = getStartupOption(getConf());
            if (startOpt == null) {
                throw new IOException("Startup option not set.");
            }
            final String bpid = nsInfo.getBlockPoolID();
            //read storage info, lock data dirs and transition fs state if necessary
            synchronized (this) {
                storage.recoverTransitionRead(this, nsInfo, dataDirs, startOpt);
            }
            final StorageInfo bpStorage = storage.getBPStorage(bpid);
            LOG.info("Setting up storage: nsid={};bpid={};lv={};" +
                            "nsInfo={};dnuuid={}",
                    bpStorage.getNamespaceID(), bpid, storage.getLayoutVersion(),
                    nsInfo, storage.getDatanodeUuid());
        }

        // If this is a newly formatted DataNode then assign a new DatanodeUuid.
        checkDatanodeUuid();

        synchronized (this) {
            if (data == null) {
                data = factory.newInstance(this, storage, getConf());
            }
        }
    }

    /**
     * 确定超文本传输协议（HTTP）服务器的有效地址
     */
    public static InetSocketAddress getInfoAddr(Configuration conf) {
        return NetUtils.createSocketAddr(conf.getTrimmed(DFS_DATANODE_HTTP_ADDRESS_KEY,
                DFS_DATANODE_HTTP_ADDRESS_DEFAULT));
    }

    private void registerMXBean() {
        dataNodeInfoBeanName = MBeans.register("DataNode", "DataNodeInfo", this);
    }

    @VisibleForTesting
    public DataXceiverServer getXferServer() {
        return xserver;
    }

    @VisibleForTesting
    public int getXferPort() {
        return streamingAddr.getPort();
    }

    @VisibleForTesting
    public SaslDataTransferServer getSaslServer() {
        return saslServer;
    }

    /**
     * 返回用于日志记录或显示的名称
     * @return 用于日志记录或显示的名称
     */
    public String getDisplayName() {
        return hostName + ":" + getXferPort();
    }

    /**
     * 获取数据传输的套接字地址
     * 
     * 注意：数据节点可以在流式传输地址上执行数据传输，但客户端会被分配IPC IP地址进行数据传输，
     * 这两者可能是不同的地址。
     * 
     * @return 数据传输的套接字地址
     */
    public InetSocketAddress getXferAddress() {
        return streamingAddr;
    }

    /**
     * 获取数据节点的IPC端口
     * @return 数据节点的IPC端口号
     */
    public int getIpcPort() {
        return ipcServer.getListenerAddress().getPort();
    }

    /**
     * 通过块池ID获取BP注册信息
     * @param bpid 块池ID
     * @return 块池注册对象
     * @throws IOException 当获取注册信息失败时抛出异常
     */
    @VisibleForTesting
    public DatanodeRegistration getDNRegistrationForBP(String bpid)
            throws IOException {
        DataNodeFaultInjector.get().noRegistration();
        BPOfferService bpos = blockPoolManager.get(bpid);
        if (bpos == null || bpos.bpRegistration == null) {
            throw new IOException("cannot find BPOfferService for bpid=" + bpid);
        }
        return bpos.bpRegistration;
    }

    /**
     * Creates either NIO or regular depending on socketWriteTimeout.
     */
    public Socket newSocket() throws IOException {
        return socketFactory.createSocket();
    }

    /**
     * Connect to the NN. This is separated out for easier testing.
     */
    DatanodeProtocolClientSideTranslatorPB connectToNN(
            InetSocketAddress nnAddr) throws IOException {
        return new DatanodeProtocolClientSideTranslatorPB(nnAddr, getConf());
    }

    /**
     * Connect to the NN for the lifeline protocol. This is separated out for
     * easier testing.
     *
     * @param lifelineNnAddr address of lifeline RPC server
     * @return lifeline RPC proxy
     */
    DatanodeLifelineProtocolClientSideTranslatorPB connectToLifelineNN(
            InetSocketAddress lifelineNnAddr) throws IOException {
        return new DatanodeLifelineProtocolClientSideTranslatorPB(lifelineNnAddr,
                getConf());
    }

    public static InterDatanodeProtocol createInterDataNodeProtocolProxy(
            DatanodeID datanodeid, final Configuration conf, final int socketTimeout,
            final boolean connectToDnViaHostname) throws IOException {
        final String dnAddr = datanodeid.getIpcAddr(connectToDnViaHostname);
        final InetSocketAddress addr = NetUtils.createSocketAddr(dnAddr);
        LOG.debug("Connecting to datanode {} addr={}",
                dnAddr, addr);
        final UserGroupInformation loginUgi = UserGroupInformation.getLoginUser();
        try {
            return loginUgi
                    .doAs(new PrivilegedExceptionAction<InterDatanodeProtocol>() {
                        @Override
                        public InterDatanodeProtocol run() throws IOException {
                            return new InterDatanodeProtocolTranslatorPB(addr, loginUgi,
                                    conf, NetUtils.getDefaultSocketFactory(conf), socketTimeout);
                        }
                    });
        } catch (InterruptedException ie) {
            throw new IOException(ie.getMessage());
        }
    }

    public DataNodeMetrics getMetrics() {
        return metrics;
    }

    public DataNodeDiskMetrics getDiskMetrics() {
        return diskMetrics;
    }

    public DataNodePeerMetrics getPeerMetrics() {
        return peerMetrics;
    }

    /** Ensure the authentication method is kerberos */
    private void checkKerberosAuthMethod(String msg) throws IOException {
        // User invoking the call must be same as the datanode user
        if (!UserGroupInformation.isSecurityEnabled()) {
            return;
        }
        if (UserGroupInformation.getCurrentUser().getAuthenticationMethod() !=
                AuthenticationMethod.KERBEROS) {
            throw new AccessControlException("Error in " + msg
                    + "Only kerberos based authentication is allowed.");
        }
    }

    private void checkBlockLocalPathAccess() throws IOException {
        checkKerberosAuthMethod("getBlockLocalPathInfo()");
        String currentUser = UserGroupInformation.getCurrentUser().getShortUserName();
        if (!usersWithLocalPathAccess.contains(currentUser)) {
            throw new AccessControlException(
                    "Can't continue with getBlockLocalPathInfo() "
                            + "authorization. The user " + currentUser
                            + " is not configured in "
                            + DFSConfigKeys.DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY);
        }
    }

    public long getMaxNumberOfBlocksToLog() {
        return maxNumberOfBlocksToLog;
    }

    @Override
    public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock block,
                                                    Token<BlockTokenIdentifier> token) throws IOException {
        checkBlockLocalPathAccess();
        checkBlockToken(block, token, BlockTokenIdentifier.AccessMode.READ);
        checkStorageState("getBlockLocalPathInfo");
        BlockLocalPathInfo info = data.getBlockLocalPathInfo(block);
        if (info != null) {
            LOG.trace("getBlockLocalPathInfo successful " +
                            "block={} blockfile {} metafile {}",
                    block, info.getBlockPath(), info.getMetaPath());
        } else {
            LOG.trace("getBlockLocalPathInfo for block={} " +
                    "returning null", block);
        }

        metrics.incrBlocksGetLocalPathInfo();
        return info;
    }

    @InterfaceAudience.LimitedPrivate("HDFS")
    static public class ShortCircuitFdsUnsupportedException extends IOException {
        private static final long serialVersionUID = 1L;

        public ShortCircuitFdsUnsupportedException(String msg) {
            super(msg);
        }
    }

    @InterfaceAudience.LimitedPrivate("HDFS")
    static public class ShortCircuitFdsVersionException extends IOException {
        private static final long serialVersionUID = 1L;

        public ShortCircuitFdsVersionException(String msg) {
            super(msg);
        }
    }

    FileInputStream[] requestShortCircuitFdsForRead(final ExtendedBlock blk,
                                                    final Token<BlockTokenIdentifier> token, int maxVersion)
            throws ShortCircuitFdsUnsupportedException,
            ShortCircuitFdsVersionException, IOException {
        if (fileDescriptorPassingDisabledReason != null) {
            throw new ShortCircuitFdsUnsupportedException(
                    fileDescriptorPassingDisabledReason);
        }
        int blkVersion = CURRENT_BLOCK_FORMAT_VERSION;
        if (maxVersion < blkVersion) {
            throw new ShortCircuitFdsVersionException("Your client is too old " +
                    "to read this block!  Its format version is " +
                    blkVersion + ", but the highest format version you can read is " +
                    maxVersion);
        }
        metrics.incrBlocksGetLocalPathInfo();
        FileInputStream fis[] = new FileInputStream[2];

        try {
            checkStorageState("requestShortCircuitFdsForRead");
            fis[0] = (FileInputStream) data.getBlockInputStream(blk, 0);
            fis[1] = DatanodeUtil.getMetaDataInputStream(blk, data);
        } catch (ClassCastException e) {
            LOG.debug("requestShortCircuitFdsForRead failed", e);
            throw new ShortCircuitFdsUnsupportedException("This DataNode's " +
                    "FsDatasetSpi does not support short-circuit local reads");
        }
        return fis;
    }

    private void checkBlockToken(ExtendedBlock block,
                                 Token<BlockTokenIdentifier> token, AccessMode accessMode)
            throws IOException {
        if (isBlockTokenEnabled) {
            BlockTokenIdentifier id = new BlockTokenIdentifier();
            ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
            DataInputStream in = new DataInputStream(buf);
            id.readFields(in);
            LOG.debug("BlockTokenIdentifier id: {}", id);
            blockPoolTokenSecretManager.checkAccess(id, null, block, accessMode,
                    null, null);
        }
    }

    /**
     * 关闭当前数据节点实例
     * 仅在关闭完成后才返回
     * 此方法只能由offerService线程调用，
     * 否则可能会发生死锁
     */
    public void shutdown() {
        stopMetricsLogger();
        if (plugins != null) {
            for (ServicePlugin p : plugins) {
                try {
                    p.stop();
                    LOG.info("Stopped plug-in {}", p);
                } catch (Throwable t) {
                    LOG.warn("ServicePlugin {} could not be stopped", p, t);
                }
            }
        }

        List<BPOfferService> bposArray = (this.blockPoolManager == null)
                ? new ArrayList<BPOfferService>()
                : this.blockPoolManager.getAllNamenodeThreads();
        // If shutdown is not for restart, set shouldRun to false early.
        if (!shutdownForUpgrade) {
            shouldRun = false;
        }

        // When shutting down for restart, DataXceiverServer is interrupted
        // in order to avoid any further acceptance of requests, but the peers
        // for block writes are not closed until the clients are notified.
        if (dataXceiverServer != null) {
            try {
                xserver.sendOOBToPeers();
                ((DataXceiverServer) this.dataXceiverServer.getRunnable()).kill();
                this.dataXceiverServer.interrupt();
            } catch (Exception e) {
                // Ignore, since the out of band messaging is advisory.
                LOG.trace("Exception interrupting DataXceiverServer", e);
            }
        }

        // Record the time of initial notification
        long timeNotified = Time.monotonicNow();

        if (localDataXceiverServer != null) {
            ((DataXceiverServer) this.localDataXceiverServer.getRunnable()).kill();
            this.localDataXceiverServer.interrupt();
        }

        // Terminate directory scanner and block scanner
        shutdownPeriodicScanners();
        shutdownDiskBalancer();

        // Stop the web server
        if (httpServer != null) {
            try {
                httpServer.close();
            } catch (Exception e) {
                LOG.warn("Exception shutting down DataNode HttpServer", e);
            }
        }

        volumeChecker.shutdownAndWait(1, TimeUnit.SECONDS);

        if (storageLocationChecker != null) {
            storageLocationChecker.shutdownAndWait(1, TimeUnit.SECONDS);
        }

        if (pauseMonitor != null) {
            pauseMonitor.stop();
        }

        // shouldRun is set to false here to prevent certain threads from exiting
        // before the restart prep is done.
        this.shouldRun = false;

        // wait reconfiguration thread, if any, to exit
        shutdownReconfigurationTask();

        LOG.info("Waiting up to 30 seconds for transfer threads to complete");
        HadoopExecutors.shutdown(this.xferService, LOG, 15L, TimeUnit.SECONDS);

        // wait for all data receiver threads to exit
        if (this.threadGroup != null) {
            int sleepMs = 2;
            while (true) {
                // When shutting down for restart, wait 1 second before forcing
                // termination of receiver threads.
                if (!this.shutdownForUpgrade ||
                        (this.shutdownForUpgrade && (Time.monotonicNow() - timeNotified
                                > 1000))) {
                    this.threadGroup.interrupt();
                    break;
                }
                LOG.info("Waiting for threadgroup to exit, active threads is {}",
                        this.threadGroup.activeCount());
                if (this.threadGroup.activeCount() == 0) {
                    break;
                }
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException e) {
                }
                sleepMs = sleepMs * 3 / 2; // exponential backoff
                if (sleepMs > 200) {
                    sleepMs = 200;
                }
            }
            this.threadGroup = null;
        }
        if (this.dataXceiverServer != null) {
            // wait for dataXceiverServer to terminate
            try {
                this.dataXceiverServer.join();
            } catch (InterruptedException ie) {
            }
        }
        if (this.localDataXceiverServer != null) {
            // wait for localDataXceiverServer to terminate
            try {
                this.localDataXceiverServer.join();
            } catch (InterruptedException ie) {
            }
        }
        if (metrics != null) {
            metrics.setDataNodeActiveXceiversCount(0);
            metrics.setDataNodeReadActiveXceiversCount(0);
            metrics.setDataNodeWriteActiveXceiversCount(0);
            metrics.setDataNodePacketResponderCount(0);
            metrics.setDataNodeBlockRecoveryWorkerCount(0);
        }

        // IPC server needs to be shutdown late in the process, otherwise
        // shutdown command response won't get sent.
        if (ipcServer != null) {
            ipcServer.stop();
        }

        if (ecWorker != null) {
            ecWorker.shutDown();
        }

        if (blockPoolManager != null) {
            try {
                this.blockPoolManager.shutDownAll(bposArray);
            } catch (InterruptedException ie) {
                LOG.warn("Received exception in BlockPoolManager#shutDownAll", ie);
            }
        }

        if (storage != null) {
            try {
                this.storage.unlockAll();
            } catch (IOException ie) {
                LOG.warn("Exception when unlocking storage", ie);
            }
        }
        if (data != null) {
            data.shutdown();
        }
        if (metrics != null) {
            metrics.shutdown();
        }
        if (dnConf.diskStatsEnabled && diskMetrics != null) {
            diskMetrics.shutdownAndWait();
        }
        if (dataNodeInfoBeanName != null) {
            MBeans.unregister(dataNodeInfoBeanName);
            dataNodeInfoBeanName = null;
        }
        if (shortCircuitRegistry != null) shortCircuitRegistry.shutdown();
        LOG.info("Shutdown complete.");
        synchronized (this) {
            // it is already false, but setting it again to avoid a findbug warning.
            this.shouldRun = false;
            // Notify the main 线程.
            notifyAll();
        }
        tracer.close();
        dataSetLockManager.lockLeakCheck();
    }

    /**
 * * * Check if there is a 磁盘 failure asynchronously
     * and if so, handle the error.
 */
    public void checkDiskErrorAsync(FsVolumeSpi volume) {
        volumeChecker.checkVolume(
                volume, (healthyVolumes, failedVolumes) -> {
                    if (failedVolumes.size() > 0) {
                        LOG.warn("checkDiskErrorAsync callback got {} failed volumes: {}",
                                failedVolumes.size(), failedVolumes);
                    } else {
                        LOG.debug("checkDiskErrorAsync: no volume failures detected");
                    }
                    lastDiskErrorCheck = Time.monotonicNow();
                    handleVolumeFailures(failedVolumes);
                });
    }

    private void handleDiskError(String failedVolumes, int failedNumber) {
        final boolean hasEnoughResources = data.hasEnoughResource();
        LOG.warn("DataNode.handleDiskError on: " +
                "[{}] Keep Running: {}", failedVolumes, hasEnoughResources);

        // If we have enough active valid volumes then we do not want to
        // shutdown the DN completely.
        int dpError = hasEnoughResources ? DatanodeProtocol.DISK_ERROR
                : DatanodeProtocol.FATAL_DISK_ERROR;
        metrics.incrVolumeFailures(failedNumber);

        // inform NameNodes
        for (BPOfferService bpos : blockPoolManager.getAllNamenodeThreads()) {
            bpos.trySendErrorReport(dpError, failedVolumes);
        }

        if (hasEnoughResources) {
            scheduleAllBlockReport(0);
            return; // do not shutdown
        }

        LOG.warn("DataNode is shutting down due to failed volumes: [{}]",
                failedVolumes);
        shouldRun = false;
    }

    /** Number of concurrent xceivers per node. */
    @Override // DataNodeMXBean
    public int getXceiverCount() {
        if (metrics == null) {
            return 0;
        }
        return metrics.getDataNodeActiveXceiverCount();
    }

    @Override // DataNodeMXBean
    public int getActiveTransferThreadCount() {
        if (metrics == null) {
            return 0;
        }
        return metrics.getDataNodeActiveXceiverCount()
                + metrics.getDataNodePacketResponderCount()
                + metrics.getDataNodeBlockRecoveryWorkerCount();
    }

    @Override // DataNodeMXBean
    public Map<String, Map<String, Long>> getDatanodeNetworkCounts() {
        return datanodeNetworkCounts.asMap();
    }

    void incrDatanodeNetworkErrors(String host) {
        metrics.incrDatanodeNetworkErrors();

        try {
            datanodeNetworkCounts.get(host).compute(NETWORK_ERRORS,
                    (key, errors) -> errors == null ? 1L : errors + 1L);
        } catch (ExecutionException e) {
            LOG.warn("Failed to increment network error counts for host: {}", host);
        }
    }

    @Override // DataNodeMXBean
    public int getXmitsInProgress() {
        return xmitsInProgress.get();
    }

    /**
 * * * Increments the xmitsInProgress count. xmitsInProgress count represents the
     * number of data 副本/reconstruction tasks running currently.
 */
    public void incrementXmitsInProgress() {
        xmitsInProgress.getAndIncrement();
    }

    /**
 * * * Increments the xmitInProgress count by given 值.
     *
     * @param delta the amount of xmitsInProgress to increase.
     * @see #incrementXmitsInProgress()
 */
    public void incrementXmitsInProcess(int delta) {
        Preconditions.checkArgument(delta >= 0);
        xmitsInProgress.getAndAdd(delta);
    }

    /** * Decrements the xmitsInProgress count */
    public void decrementXmitsInProgress() {
        xmitsInProgress.getAndDecrement();
    }

    /**
 * * * Decrements the xmitsInProgress count by given 值.
     *
     * @see #decrementXmitsInProgress()
 */
    public void decrementXmitsInProgress(int delta) {
        Preconditions.checkArgument(delta >= 0);
        xmitsInProgress.getAndAdd(0 - delta);
    }

    private void reportBadBlock(final BPOfferService bpos,
                                final ExtendedBlock block, final String msg) {
        FsVolumeSpi volume = getFSDataset().getVolume(block);
        if (volume == null) {
            LOG.warn("Cannot find FsVolumeSpi to report bad block: {}", block);
            return;
        }
        bpos.reportBadBlocks(
                block, volume.getStorageID(), volume.getStorageType());
        LOG.warn(msg);
    }

    /**
 * * 将单个数据块传输到指定的目标DataNode
     * 
     * 此方法是DataNode间数据复制的核心实现，负责检查块的状态并启动异步传输任务
     * 它首先验证块的完整性，然后创建一个DataTransfer任务提交到线程池执行
     * 
     * @param 数据块 要传输的块，包含块ID、块池ID和块大小等信息
     * @param xferTargets 目标DataNode列表，块将被传输到这些节点
     * @param xferTargetStorageTypes 目标节点的存储类型数组
     * @param xferTargetStorageIDs 目标节点的存储ID数组
     * @抛出 IOException 如果在传输过程中发生IO异常
 */
    @VisibleForTesting
    void transferBlock(ExtendedBlock block, DatanodeInfo[] xferTargets,
                       StorageType[] xferTargetStorageTypes, String[] xferTargetStorageIDs)
            throws IOException {
        // 获取块所属的BPOfferService
        BPOfferService bpos = getBPOSForBlock(block);
        // 获取块池对应的DataNode注册信息
        DatanodeRegistration bpReg = getDNRegistrationForBP(block.getBlockPoolId());

        // 块状态标志
        boolean replicaNotExist = false;              // 副本不存在
        boolean replicaStateNotFinalized = false;     // 副本状态不是FINALIZED
        boolean blockFileNotExist = false;            // 块文件不存在
        boolean lengthTooShort = false;               // 块长度太短（可能损坏）

        try {
            data.checkBlock(block, block.getNumBytes(), ReplicaState.FINALIZED);
        } catch (ReplicaNotFoundException e) {
            replicaNotExist = true;
        } catch (UnexpectedReplicaStateException e) {
            replicaStateNotFinalized = true;
        } catch (FileNotFoundException e) {
            blockFileNotExist = true;
        } catch (EOFException e) {
            lengthTooShort = true;
        } catch (IOException e) {
            // The IOException indicates not being able to access 数据块 文件,
            // treat it the same here as blockFileNotExist, to trigger
            // reporting it as a bad 数据块
            blockFileNotExist = true;
        }

        if (replicaNotExist || replicaStateNotFinalized) {
            String errStr = "Can't send invalid block " + block;
            LOG.info(errStr);
            bpos.trySendErrorReport(DatanodeProtocol.INVALID_BLOCK, errStr);
            return;
        }
        if (blockFileNotExist) {
            // Report back to NN bad 数据块 caused by non-existent 数据块 文件.
            reportBadBlock(bpos, block, "Can't replicate block " + block
                    + " because the block file doesn't exist, or is not accessible");
            return;
        }
        if (lengthTooShort) {
            // Check if NN recorded length matches on-磁盘 length
            // Shorter on-磁盘 len indicates corruption so report NN the corrupt 数据块
            reportBadBlock(bpos, block, "Can't replicate block " + block
                    + " because on-disk length " + data.getLength(block)
                    + " is shorter than NameNode recorded length " + block.getNumBytes());
            return;
        }

        int numTargets = xferTargets.length;
        if (numTargets > 0) {
            final String xferTargetsString =
                    StringUtils.join(" ", Arrays.asList(xferTargets));
            LOG.info("{} Starting thread to transfer {} to {}", bpReg, block,
                    xferTargetsString);

            final DataTransfer dataTransferTask = new DataTransfer(xferTargets,
                    xferTargetStorageTypes, xferTargetStorageIDs, block,
                    BlockConstructionStage.PIPELINE_SETUP_CREATE, "");

            this.xferService.execute(dataTransferTask);
        }
    }

    /**
 * * * 批量传输多个数据块到对应的目标DataNode
     * 
     * 此方法是对transferBlock的批量封装，用于处理来自NameNode的批量块复制请求
     * 它会遍历所有块，并为每个块调用transferBlock方法
     * 
     * @param poolId 块池ID，所有块都属于这个块池
     * @param blocks 要传输的块数组
     * @param xferTargets 每个块对应的目标DataNode二维数组
     * @param xferTargetStorageTypes 每个块对应的目标存储类型二维数组
     * @param xferTargetStorageIDs 每个块对应的目标存储ID二维数组
 */
    void transferBlocks(String poolId, Block blocks[],
                        DatanodeInfo[][] xferTargets, StorageType[][] xferTargetStorageTypes,
                        String[][] xferTargetStorageIDs) {
        // 遍历所有要传输的块
        for (int i = 0; i < blocks.length; i++) {
            try {
                // 将Block转换为ExtendedBlock，然后调用transferBlock方法传输单个块
                transferBlock(new ExtendedBlock(poolId, blocks[i]), xferTargets[i],
                        xferTargetStorageTypes[i], xferTargetStorageIDs[i]);
            } catch (IOException ie) {
                // 捕获并记录每个块传输过程中的异常，但不中断其他块的传输
                LOG.warn("Failed to transfer block {}", blocks[i], ie);
            }
        }
    }

  /* ********************************************************************
  协议 when a 客户端 reads data from 数据节点 (Cur Ver: 9):

  客户端's Request :
  =================

     Processed in DataXceiver:
     +----------------------------------------------+
     | Common Header   | 1 byte OP == OP_READ_BLOCK |
     +----------------------------------------------+

     Processed in readBlock() :
     +-------------------------------------------------------------------------+
     | 8 byte 数据块 ID | 8 byte genstamp | 8 byte start offset | 8 byte length |
     +-------------------------------------------------------------------------+
     |   vInt length   |  <DFSClient id> |
     +-----------------------------------+

     客户端 sends optional response only at the end of receiving data.

  数据节点 Response :
  ===================

    In readBlock() :
    If there is an error while initializing BlockSender :
       +---------------------------+
       | 2 byte OP_STATUS_ERROR    | and connection will be closed.
       +---------------------------+
    Otherwise
       +---------------------------+
       | 2 byte OP_STATUS_SUCCESS  |
       +---------------------------+

    Actual data, sent by BlockSender.sendBlock() :

      ChecksumHeader :
      +--------------------------------------------------+
      | 1 byte CHECKSUM_TYPE | 4 byte BYTES_PER_CHECKSUM |
      +--------------------------------------------------+
      Followed by actual data in the form of PACKETS:
      +------------------------------------+
      | Sequence of data PACKETs ....      |
      +------------------------------------+

    A "PACKET" is defined further below.

    The 客户端 reads data until it receives a packet with
    "LastPacketInBlock" set to true or with a zero length. It then replies
    to 数据节点 with one of the status codes:
    - CHECKSUM_OK:    All the chunk checksums have been verified
    - SUCCESS:        Data received; checksums not verified
    - ERROR_CHECKSUM: (Currently not used) Detected invalid checksums

      +---------------+
      | 2 byte Status |
      +---------------+

    The 数据节点 expects all well behaved clients to send the 2 byte
    status code. And if the the 客户端 doesn't, the DN will close the
    connection. So the status code is optional in the sense that it
    does not affect the correctness of the data. (And the 客户端 can
    always reconnect.)

    PACKET : Contains a packet header, checksum and data. Amount of data
    ======== carried is set by BUFFER_SIZE.

      +-----------------------------------------------------+
      | 4 byte packet length (excluding packet header)      |
      +-----------------------------------------------------+
      | 8 byte offset in the 数据块 | 8 byte sequence number |
      +-----------------------------------------------------+
      | 1 byte isLastPacketInBlock                          |
      +-----------------------------------------------------+
      | 4 byte Length of actual data                        |
      +-----------------------------------------------------+
      | x byte checksum data. x is defined below            |
      +-----------------------------------------------------+
      | actual data ......                                  |
      +-----------------------------------------------------+

      x = (length of data + BYTE_PER_CHECKSUM - 1)/BYTES_PER_CHECKSUM *
          CHECKSUM_SIZE

      CHECKSUM_SIZE depends on CHECKSUM_TYPE (usually, 4 for CRC32)

      The above packet format is used while writing data to DFS also.
      Not all the fields might be used while reading.

   ************************************************************************ */

    /**
 * * * Used for transferring a 数据块 of data.  This 类
     * sends a piece of data to another 数据节点.
 */
    private class DataTransfer implements Runnable {
        final DatanodeInfo[] targets;
        final StorageType[] targetStorageTypes;
        final private String[] targetStorageIds;
        final ExtendedBlock b;
        final BlockConstructionStage stage;
        final private DatanodeRegistration bpReg;
        final String clientname;
        final CachingStrategy cachingStrategy;

        /** Throttle to 数据块 副本 when data transfers or writes. */
        private DataTransferThrottler throttler;

        /**
 * * * Connect to the first item in the target list.  Pass along the
         * entire target list, the 数据块, and the data.
 */
        DataTransfer(DatanodeInfo targets[], StorageType[] targetStorageTypes,
                     String[] targetStorageIds, ExtendedBlock b,
                     BlockConstructionStage stage, final String clientname) {
            DataTransferProtocol.LOG.debug("{}: {} (numBytes={}), stage={}, " +
                            "clientname={}, targets={}, target storage types={}, " +
                            "target storage IDs={}", getClass().getSimpleName(), b,
                    b.getNumBytes(), stage, clientname, Arrays.asList(targets),
                    targetStorageTypes == null ? "[]" :
                            Arrays.asList(targetStorageTypes),
                    targetStorageIds == null ? "[]" : Arrays.asList(targetStorageIds));
            this.targets = targets;
            this.targetStorageTypes = targetStorageTypes;
            this.targetStorageIds = targetStorageIds;
            this.b = b;
            this.stage = stage;
            BPOfferService bpos = blockPoolManager.get(b.getBlockPoolId());
            bpReg = bpos.bpRegistration;
            this.clientname = clientname;
            this.cachingStrategy =
                    new CachingStrategy(true, getDnConf().readaheadLength);
            if (isTransfer(stage, clientname)) {
                this.throttler = xserver.getTransferThrottler();
            } else if (isWrite(stage)) {
                this.throttler = xserver.getWriteThrottler();
            }
        }

        /** * Do the deed, write the bytes */
        @Override
        public void run() {
            incrementXmitsInProgress();
            Socket sock = null;
            DataOutputStream out = null;
            DataInputStream in = null;
            BlockSender blockSender = null;
            final boolean isClient = clientname.length() > 0;

            try {
                final String dnAddr = targets[0].getXferAddr(connectToDnViaHostname);
                InetSocketAddress curTarget = NetUtils.createSocketAddr(dnAddr);
                LOG.debug("Connecting to datanode {}", dnAddr);
                sock = newSocket();
                NetUtils.connect(sock, curTarget, dnConf.socketTimeout);
                sock.setTcpNoDelay(dnConf.getDataTransferServerTcpNoDelay());
                sock.setSoTimeout(targets.length * dnConf.socketTimeout);

                //
                // Header info
                //
                Token<BlockTokenIdentifier> accessToken = getBlockAccessToken(b,
                        EnumSet.of(BlockTokenIdentifier.AccessMode.WRITE),
                        targetStorageTypes, targetStorageIds);

                long writeTimeout = dnConf.socketWriteTimeout +
                        HdfsConstants.WRITE_TIMEOUT_EXTENSION * (targets.length - 1);
                OutputStream unbufOut = NetUtils.getOutputStream(sock, writeTimeout);
                InputStream unbufIn = NetUtils.getInputStream(sock);
                DataEncryptionKeyFactory keyFactory =
                        getDataEncryptionKeyFactoryForBlock(b);
                IOStreamPair saslStreams = saslClient.socketSend(sock, unbufOut,
                        unbufIn, keyFactory, accessToken, bpReg);
                unbufOut = saslStreams.out;
                unbufIn = saslStreams.in;

                out = new DataOutputStream(new BufferedOutputStream(unbufOut,
                        DFSUtilClient.getSmallBufferSize(getConf())));
                in = new DataInputStream(unbufIn);
                blockSender = new BlockSender(b, 0, b.getNumBytes(),
                        false, false, true, DataNode.this, null, cachingStrategy);
                DatanodeInfo srcNode = new DatanodeInfoBuilder().setNodeID(bpReg)
                        .build();

                String storageId = targetStorageIds.length > 0 ?
                        targetStorageIds[0] : null;
                new Sender(out).writeBlock(b, targetStorageTypes[0], accessToken,
                        clientname, targets, targetStorageTypes, srcNode,
                        stage, 0, 0, 0, 0, blockSender.getChecksum(), cachingStrategy,
                        false, false, null, storageId,
                        targetStorageIds);

                // send data & checksum
                blockSender.sendBlock(out, unbufOut, throttler);

                // no response necessary
                LOG.info("{}, at {}: Transmitted {} (numBytes={}) to {}",
                        getClass().getSimpleName(), DataNode.this.getDisplayName(),
                        b, b.getNumBytes(), curTarget);

                // read ack
                if (isClient) {
                    DNTransferAckProto closeAck = DNTransferAckProto.parseFrom(
                            PBHelperClient.vintPrefixed(in));
                    LOG.debug("{}: close-ack={}", getClass().getSimpleName(), closeAck);
                    if (closeAck.getStatus() != Status.SUCCESS) {
                        if (closeAck.getStatus() == Status.ERROR_ACCESS_TOKEN) {
                            throw new InvalidBlockTokenException(
                                    "Got access token error for connect ack, targets="
                                            + Arrays.asList(targets));
                        } else {
                            throw new IOException("Bad connect ack, targets="
                                    + Arrays.asList(targets) + " status=" + closeAck.getStatus());
                        }
                    }
                } else {
                    metrics.incrBlocksReplicated();
                }
            } catch (IOException ie) {
                handleBadBlock(b, ie, false);
                LOG.warn("{}:Failed to transfer {} to {} got",
                        bpReg, b, targets[0], ie);
            } catch (Throwable t) {
                LOG.error("Failed to transfer block {}", b, t);
            } finally {
                decrementXmitsInProgress();
                IOUtils.closeStream(blockSender);
                IOUtils.closeStream(out);
                IOUtils.closeStream(in);
                IOUtils.closeSocket(sock);
            }
        }

        @Override
        public String toString() {
            return "DataTransfer " + b + " to " + Arrays.asList(targets);
        }
    }

    /**
 * * *
     * Use BlockTokenSecretManager to generate 数据块 token for current user.
 */
    public Token<BlockTokenIdentifier> getBlockAccessToken(ExtendedBlock b,
                                                           EnumSet<AccessMode> mode,
                                                           StorageType[] storageTypes, String[] storageIds) throws IOException {
        Token<BlockTokenIdentifier> accessToken =
                BlockTokenSecretManager.DUMMY_TOKEN;
        if (isBlockTokenEnabled) {
            accessToken = blockPoolTokenSecretManager.generateToken(b, mode,
                    storageTypes, storageIds);
        }
        return accessToken;
    }

    /**
 * * * Returns a new DataEncryptionKeyFactory that generates a key from the
     * BlockPoolTokenSecretManager, using the 数据块 pool ID of the given 数据块.
     *
     * @param 数据块 for which the 工厂 needs to create a key
     * @返回 DataEncryptionKeyFactory for 数据块's 数据块 pool ID
 */
    public DataEncryptionKeyFactory getDataEncryptionKeyFactoryForBlock(
            final ExtendedBlock block) {
        return new DataEncryptionKeyFactory() {
            @Override
            public DataEncryptionKey newDataEncryptionKey() {
                return dnConf.encryptDataTransfer ?
                        blockPoolTokenSecretManager.generateDataEncryptionKey(
                                block.getBlockPoolId()) : null;
            }
        };
    }

    /**
 * * After a 数据块 becomes finalized, a 数据节点 increases 指标 counter,
     * notifies 名称节点, and adds it to the 数据块 scanner
     * @param 数据块 数据块 to close
     * @param delHint hint on which excess 数据块 to delete
     * @param storageUuid UUID of the 存储 where 数据块 is stored
 */
    void closeBlock(ExtendedBlock block, String delHint, String storageUuid,
                    boolean isTransientStorage) {
        metrics.incrBlocksWritten();
        notifyNamenodeReceivedBlock(block, delHint, storageUuid,
                isTransientStorage);
    }

    /**
     * 启动DataNode守护进程并等待其完成
     * 这是DataNode启动的核心方法，负责启动各种网络服务和组件
     * 如果该线程被中断，它将停止等待
     */
    public void runDatanodeDaemon() throws IOException {
        // 验证blockPoolManager是否已启动，确保DataNode实例化成功
        if (!isDatanodeUp()) {
            throw new IOException("Failed to instantiate DataNode.");
        }

        // 启动数据接收服务器，用于处理数据块传输请求
        dataXceiverServer.start();
        
        // 如果配置了本地数据接收服务器，也启动它
        if (localDataXceiverServer != null) {
            localDataXceiverServer.start();
        }
        
        // 配置IPC服务器的跟踪器并启动IPC服务器，用于RPC通信
        ipcServer.setTracer(tracer);
        ipcServer.start();
        
        // 记录DataNode的启动时间
        startTime = now();
        
        // 启动配置的插件，扩展DataNode功能
        startPlugins(getConf());
    }

    /**
     * 判断DataNode是否处于运行状态
     * 该方法用于验证DataNode是否成功启动并正常运行
     * 
     * 判断标准：只要有一个块池服务(BPOfferService)处于活跃状态，DataNode就被视为启动成功
     * 这是因为DataNode可能连接到多个NameNode(高可用或联邦模式)，只要与其中一个NameNode的连接正常
     * 
     * @return 如果DataNode处于运行状态则返回true，否则返回false
     */
    public boolean isDatanodeUp() {
        // 遍历所有与NameNode通信的块池服务线程
        for (BPOfferService bp : blockPoolManager.getAllNamenodeThreads()) {
            // 检查当前块池服务是否活跃（与对应的NameNode连接正常）
            if (bp.isAlive()) {
                return true;
            }
        }
        // 所有块池服务都不活跃，DataNode启动失败
        return false;
    }

    /**
     * 实例化单个DataNode对象（无安全资源版本）
     * 注意：调用此方法后必须调用{@link DataNode#runDatanodeDaemon()}来启动守护进程
     */
     */
    public static DataNode instantiateDataNode(String args[],
                                               Configuration conf) throws IOException {
        return instantiateDataNode(args, conf, null);
    }

    /**
     * 实例化单个DataNode对象及其安全资源
     * 这是DataNode实例化的核心方法，负责解析参数、设置配置、获取存储位置、登录安全认证
     * 注意：调用此方法后必须调用{@link DataNode#runDatanodeDaemon()}来启动守护进程
     */
     */
    public static DataNode instantiateDataNode(String args[], Configuration conf,
                                               SecureResources resources) throws IOException {
        // 如果没有提供配置，创建新的HDFS配置
        if (conf == null)
            conf = new HdfsConfiguration();

        // 解析Hadoop通用选项
        if (args != null) {
            GenericOptionsParser hParser = new GenericOptionsParser(conf, args);
            args = hParser.getRemainingArgs();
        }

        // 解析DataNode特定的命令行参数
        if (!parseArguments(args, conf)) {
            printUsage(System.err);
            return null;
        }
        
        // 构建存储位置对象，确定数据块存储的目录
        Collection<StorageLocation> dataLocations = getStorageLocations(conf);
        
        // 配置用户组信息，用于权限管理
        UserGroupInformation.setConfiguration(conf);
        
        // 执行安全登录（如Kerberos认证），建立安全上下文
        SecurityUtil.login(conf, DFS_DATANODE_KEYTAB_FILE_KEY,
                DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, getHostName(conf));
        
        // 调用makeInstance创建并初始化DataNode实例
        return makeInstance(dataLocations, conf, resources);
    }

    /**
     * Get the effective file system where the path is located.
     * DF is a packaged cross-platform class, it can get volumes
     * information from current system.
     * @param path - absolute or fully qualified path
     * @param conf - the Configuration
     * @return the effective filesystem of the path
     */
    private static String getEffectiveFileSystem(
            String path, Configuration conf) {
        try {
            DF df = new DF(new File(path), conf);
            return df.getFilesystem();
        } catch (IOException ioe) {
            LOG.error("Failed to get filesystem for dir {}", path, ioe);
        }
        return null;
    }

    /**
 * * Sometimes we mount different disks for different 存储 types
     * as the 存储 location. It's important to check the 卷 is
     * mounted rightly before initializing 存储 locations.
     * @param conf - 配置
     * @param location - 存储 location
     * @返回 false if the filesystem of location is configured and mismatch
     * with effective filesystem.
 */
    private static boolean checkFileSystemWithConfigured(
            Configuration conf, StorageLocation location) {
        String configFs = StorageType.getConf(
                conf, location.getStorageType(), "filesystem");
        if (configFs != null && !configFs.isEmpty()) {
            String effectiveFs = getEffectiveFileSystem(
                    location.getUri().getPath(), conf);
            if (effectiveFs == null || !effectiveFs.equals(configFs)) {
                LOG.error("Filesystem mismatch for storage location {}. " +
                                "Configured is {}, effective is {}.",
                        location.getUri(), configFs, effectiveFs);
                return false;
            }
        }
        return true;
    }

    public static List<StorageLocation> getStorageLocations(Configuration conf) {
        Collection<String> rawLocations =
                conf.getTrimmedStringCollection(DFS_DATANODE_DATA_DIR_KEY);
        List<StorageLocation> locations =
                new ArrayList<StorageLocation>(rawLocations.size());

        for (String locationString : rawLocations) {
            final StorageLocation location;
            try {
                location = StorageLocation.parse(locationString);
            } catch (IOException | SecurityException ioe) {
                LOG.error("Failed to initialize storage directory {}." +
                        "Exception details: {}", locationString, ioe.toString());
                // Ignore the 异常.
                continue;
            }
            if (checkFileSystemWithConfigured(conf, location)) {
                locations.add(location);
            }
        }

        return locations;
    }

    /**
 * * Instantiate &amp; Start a single 数据节点 守护进程 and wait for it to
     * finish.
     *  If this 线程 is specifically interrupted, it will stop waiting.
 */
    @VisibleForTesting
    /**
     * 创建DataNode实例的简化方法（无安全资源版本）
     * 这是一个重载方法，主要用于简化调用，不涉及安全资源
     */
    public static DataNode createDataNode(String args[],
                                          Configuration conf) throws IOException {
        return createDataNode(args, conf, null);
    }

    /**
     * 实例化并启动单个DataNode守护进程
     * 这个方法负责创建DataNode实例并启动其守护进程，是DataNode启动的核心方法
     */
    @VisibleForTesting
    @InterfaceAudience.Private
    public static DataNode createDataNode(String args[], Configuration conf,
                                          SecureResources resources) throws IOException {
        // 初始化并构建DataNode实例
        DataNode dn = instantiateDataNode(args, conf, resources);
        if (dn != null) {
            // 启动DataNode守护进程，包括各种网络服务
            dn.runDatanodeDaemon();
        }
        return dn;
    }

    void join() {
        while (shouldRun) {
            try {
                blockPoolManager.joinAll();
                if (blockPoolManager.getAllNamenodeThreads().size() == 0) {
                    shouldRun = false;
                }
                // Terminate if shutdown is complete or 2 seconds after all BPs
                // are shutdown.
                synchronized (this) {
                    wait(2000);
                }
            } catch (InterruptedException ex) {
                LOG.warn("Received exception in Datanode#join: {}", ex.toString());
            }
        }
    }

    /**
     * 创建DataNode实例的核心方法
     * 该方法在确保至少有一个给定的数据目录（及其父目录，如果需要）可以创建后实例化DataNode
     * 这是DataNode实例化的最后一步，在验证存储目录可用后创建实际的DataNode对象
     * 
     * @param dataDirs 数据目录集合，新的DataNode实例将在这些目录中存储文件
     * @param conf 要使用的配置实例
     * @param resources 在Kerberos下运行所需的安全资源
     * @return 给定数据目录和配置的DataNode实例，如果无法创建任何目录则返回null
     * @throws IOException 如果实例化DataNode失败
     */
    static DataNode makeInstance(Collection<StorageLocation> dataDirs,
                                 Configuration conf, SecureResources resources) throws IOException {
        List<StorageLocation> locations;
        
        // 创建存储位置检查器，用于验证数据目录是否可用
        StorageLocationChecker storageLocationChecker = 
                new StorageLocationChecker(conf, new Timer());
        try {
            // 检查并验证所有数据目录，返回可使用的存储位置列表
            locations = storageLocationChecker.check(conf, dataDirs);
        } catch (InterruptedException ie) {
            // 捕获中断异常并转换为IOException
            throw new IOException("Failed to instantiate DataNode", ie);
        }
        
        // 初始化DataNode的指标系统，用于收集和报告性能指标
        DefaultMetricsSystem.initialize("DataNode");

        // 断言确保至少有一个可用的数据目录
        assert locations.size() > 0 : "number of data directories should be > 0";
        
        // 创建并返回新的DataNode实例
        return new DataNode(conf, locations, storageLocationChecker, resources);
    }

    @Override
    public String toString() {
        return "DataNode{data=" + data + ", localName='" + getDisplayName()
                + "', datanodeUuid='" + storage.getDatanodeUuid() + "', xmitsInProgress="
                + xmitsInProgress.get() + "}";
    }

    private static void printUsage(PrintStream out) {
        out.println(USAGE + "\n");
    }

    /**
     * 解析和验证DataNode的命令行参数并设置配置参数
     * 该方法负责处理启动DataNode时传入的命令行参数，确定启动模式并设置相应的配置
     * 
     * @param args 命令行参数数组
     * @param conf 要设置的配置对象
     * @return 如果参数正确则返回true，如果参数不正确则返回false
     */
    @VisibleForTesting
    static boolean parseArguments(String args[], Configuration conf) {
        // 默认设置为常规启动模式
        StartupOption startOpt = StartupOption.REGULAR;
        int i = 0;

        // 如果存在命令行参数
        if (args != null && args.length != 0) {
            // 获取第一个参数
            String cmd = args[i++];
            
            // 检查是否为机架参数（已不再支持）
            if ("-r".equalsIgnoreCase(cmd) || "--rack".equalsIgnoreCase(cmd)) {
                LOG.error("-r, --rack arguments are not supported anymore. RackID " +
                        "resolution is handled by the NameNode.");
                return false;
            } 
            // 检查是否为回滚模式
            else if (StartupOption.ROLLBACK.getName().equalsIgnoreCase(cmd)) {
                startOpt = StartupOption.ROLLBACK;
            } 
            // 检查是否为常规模式
            else if (StartupOption.REGULAR.getName().equalsIgnoreCase(cmd)) {
                startOpt = StartupOption.REGULAR;
            } 
            // 不支持的参数
            else {
                return false;
            }
        }

        // 将启动选项设置到配置中
        setStartupOption(conf, startOpt);
        
        // 确保没有指定多个命令（如果只有一个命令，则i应该等于args.length）
        return (args == null || i == args.length);    // 如果指定了多个命令则失败
    }

    /**
     * 设置DataNode的启动选项到配置对象中
     * 该方法将启动模式枚举转换为字符串并存储在配置中
     * 
     * @param conf 配置对象
     * @param opt 启动选项枚举值
     */
    private static void setStartupOption(Configuration conf, StartupOption opt) {
        // 将启动选项存储在配置中，使用预定义的配置键
        conf.set(DFS_DATANODE_STARTUP_KEY, opt.toString());
    }

    /**
     * 从配置对象中获取DataNode的启动选项
     * 该方法从配置中读取启动模式字符串并转换为枚举值
     * 
     * @param conf 配置对象
     * @return 启动选项枚举值，如果配置中不存在则返回常规模式
     */
    static StartupOption getStartupOption(Configuration conf) {
        // 从配置中获取启动选项，默认使用常规模式
        String value = conf.get(DFS_DATANODE_STARTUP_KEY,
                StartupOption.REGULAR.toString());
        // 将字符串值转换为枚举值
        return StartupOption.getEnum(value);
    }

    /**
 * * * This methods  arranges for the data node to send
     * the 数据块 report at the next heartbeat.
 */
    public void scheduleAllBlockReport(long delay) {
        for (BPOfferService bpos : blockPoolManager.getAllNamenodeThreads()) {
            bpos.scheduleBlockReport(delay);
        }
    }

    /**
 * * * Examples are adding and deleting blocks directly.
     * The most common usage will be when the data node's 存储 is simulated.
     *
     * @返回 the fsdataset that stores the blocks
 */
    @VisibleForTesting
    public FsDatasetSpi<?> getFSDataset() {
        return data;
    }

    @VisibleForTesting
    /** @返回 the 数据块 scanner. */
    public BlockScanner getBlockScanner() {
        return blockScanner;
    }

    @VisibleForTesting
    DirectoryScanner getDirectoryScanner() {
        return directoryScanner;
    }

    @VisibleForTesting
    public BlockPoolTokenSecretManager getBlockPoolTokenSecretManager() {
        return blockPoolTokenSecretManager;
    }

    /**
     * 安全模式下启动DataNode的主方法
     * 在安全集群中，这个方法会在获取特权资源后被调用
     */
    public static void secureMain(String args[], SecureResources resources) {
        int errorCode = 0;
        try {
            // 打印DataNode启动和关闭的日志消息
            StringUtils.startupShutdownMessage(DataNode.class, args, LOG);
            // 构建DataNode实例
            DataNode datanode = createDataNode(args, null, resources);
            if (datanode != null) {
                // 阻塞当前线程，等待DataNode守护进程结束
                datanode.join();
            } else {
                errorCode = 1;
            }
        } catch (Throwable e) {
            LOG.error("Exception in secureMain", e);
            terminate(1, e);
        } finally {
            // 在安全模式下，控制会转到Jsvc，DataNode进程需要显式退出
            LOG.warn("Exiting Datanode");
            terminate(errorCode);
        }
    }

    /**
     * DataNode的主入口方法
     * 这是DataNode进程启动的入口点，它会调用secureMain方法进行实际的初始化工作
     */
    public static void main(String args[]) {
        // 解析帮助参数，如果用户请求帮助则显示用法并退出
        if (DFSUtil.parseHelpArgument(args, DataNode.USAGE, System.out, true)) {
            System.exit(0);
        }

        // 调用secureMain方法，传入参数和null作为安全资源（非安全模式）
        secureMain(args, null);
    }

    /**
     * 初始化数据块副本的恢复过程
     * 
     * 此方法是InterDatanodeProtocol接口的实现，用于开始块恢复过程
     * 当一个数据块的副本需要恢复时（例如管道写入失败），其他DataNode会调用此方法
     * 
     * @param rBlock 正在恢复的块信息，包含块ID、块池ID和新的生成戳等信息
     * @return ReplicaRecoveryInfo 包含块恢复所需信息的对象，如当前副本的状态、长度等
     * @throws IOException 如果存储未初始化或恢复过程中发生IO异常
     */
    @Override // InterDatanodeProtocol
    public ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock)
            throws IOException {
        // 检查存储系统是否已初始化
        checkStorageState("initReplicaRecovery");
        // 委托给数据存储组件执行实际的副本恢复初始化操作
        return data.initReplicaRecovery(rBlock);
    }

    /**
     * 更新正在恢复的副本，设置新的生成戳和长度
     * 
     * 此方法是InterDatanodeProtocol接口的实现，用于在块恢复过程中更新副本的元数据
     * 当主DataNode确定了正确的块大小和生成戳后，会通知其他副本更新自己的信息
     * 
     * @param oldBlock 原始块信息
     * @param recoveryId 新的生成戳（恢复ID）
     * @param newBlockId 新的块ID
     * @param newLength 新的块长度
     * @return String 存储该块的存储卷UUID
     * @throws IOException 如果存储未初始化或更新过程中发生IO异常
     */
    @Override // InterDatanodeProtocol
    public String updateReplicaUnderRecovery(final ExtendedBlock oldBlock,
                                             final long recoveryId, final long newBlockId, final long newLength)
            throws IOException {
        // 检查存储系统是否已初始化
        checkStorageState("updateReplicaUnderRecovery");
        // 委托给数据存储组件执行实际的副本更新操作
        final Replica r = data.updateReplicaUnderRecovery(oldBlock,
                recoveryId, newBlockId, newLength);
        // 通知NameNode块信息已更新
        // 这对于HA模式非常重要，否则备用节点可能会失去对块位置的跟踪，直到下一次块报告
        ExtendedBlock newBlock = new ExtendedBlock(oldBlock);
        newBlock.setGenerationStamp(recoveryId);
        newBlock.setBlockId(newBlockId);
        newBlock.setNumBytes(newLength);
        final String storageID = r.getStorageUuid();
        notifyNamenodeReceivedBlock(newBlock, null, storageID,
                r.isOnTransientStorage());
        // 返回存储该块的存储卷UUID
        return storageID;
    }

    @Override // ClientDataNodeProtocol
    public long getReplicaVisibleLength(final ExtendedBlock block) throws IOException {
        checkReadAccess(block);
        return data.getReplicaVisibleLength(block);
    }

    private void checkReadAccess(final ExtendedBlock block) throws IOException {
        // Make sure this node has registered for the block pool.
        try {
            getDNRegistrationForBP(block.getBlockPoolId());
        } catch (IOException e) {
            // if it has not registered with the NN, throw an exception back.
            throw new org.apache.hadoop.ipc.RetriableException(
                    "Datanode not registered. Try again later.");
        }

        if (isBlockTokenEnabled) {
            Set<TokenIdentifier> tokenIds = UserGroupInformation.getCurrentUser()
                    .getTokenIdentifiers();
            if (tokenIds.size() != 1) {
                throw new IOException("Can't continue since none or more than one "
                        + "BlockTokenIdentifier is found.");
            }
            for (TokenIdentifier tokenId : tokenIds) {
                BlockTokenIdentifier id = (BlockTokenIdentifier) tokenId;
                LOG.debug("BlockTokenIdentifier: {}", id);
                blockPoolTokenSecretManager.checkAccess(id, null, block,
                        BlockTokenIdentifier.AccessMode.READ, null, null);
            }
        }
    }

    /**
     * 为管道恢复传输数据块副本
     * 
     * 此方法用于在管道恢复过程中，将一个有效的块副本传输到指定的目标DataNode
     * 当数据写入管道中的某个节点失败时，需要从其他节点复制数据来重建管道
     * 
     * @param b 要传输的块，对应的副本必须是RBW（正在写入）或FINALIZED（已完成）状态
     * @param targets 目标DataNode列表，块将被传输到这些节点
     * @param targetStorageTypes 目标节点的存储类型数组
     * @param targetStorageIds 目标节点的存储ID数组
     * @param client 客户端名称
     * @throws IOException 如果传输过程中发生IO异常
     */
    void transferReplicaForPipelineRecovery(final ExtendedBlock b,
                                            final DatanodeInfo[] targets, final StorageType[] targetStorageTypes,
                                            final String[] targetStorageIds, final String client)
            throws IOException {
        final long storedGS;      // 存储的块生成戳
        final long visible;       // 可见的块长度
        final BlockConstructionStage stage;  // 块构建阶段

        // 获取副本信息
        try (AutoCloseableLock lock = dataSetLockManager.readLock(
                LockLevel.BLOCK_POOl, b.getBlockPoolId())) {
            // 从存储中获取块信息
            Block storedBlock = data.getStoredBlock(b.getBlockPoolId(),
                    b.getBlockId());
            if (null == storedBlock) {
                throw new IOException(b + " not found in datanode.");
            }
            // 获取存储的生成戳
            storedGS = storedBlock.getGenerationStamp();
            // 验证存储的生成戳不小于请求中的生成戳
            if (storedGS < b.getGenerationStamp()) {
                throw new IOException(storedGS
                        + " = storedGS < b.getGenerationStamp(), b=" + b);
            }
            // 使用存储的生成戳更新块信息
            b.setGenerationStamp(storedGS);
            // 确定块的状态，设置相应的传输阶段
            if (data.isValidRbw(b)) {
                stage = BlockConstructionStage.TRANSFER_RBW;
                LOG.debug("Replica is being written!");
            } else if (data.isValidBlock(b)) {
                stage = BlockConstructionStage.TRANSFER_FINALIZED;
                LOG.debug("Replica is finalized!");
            } else {
                final String r = data.getReplicaString(b.getBlockPoolId(), b.getBlockId());
                throw new IOException(b + " is neither a RBW nor a Finalized, r=" + r);
            }
            visible = data.getReplicaVisibleLength(b);
        }
        // set visible length
        b.setNumBytes(visible);

        if (targets.length > 0) {
            if (LOG.isDebugEnabled()) {
                final String xferTargetsString =
                        StringUtils.join(" ", Arrays.asList(targets));
                LOG.debug("Transferring a replica to {}", xferTargetsString);
            }

            final DataTransfer dataTransferTask = new DataTransfer(targets,
                    targetStorageTypes, targetStorageIds, b, stage, client);

            @SuppressWarnings("unchecked")
            Future<Void> f = (Future<Void>) this.xferService.submit(dataTransferTask);
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IOException("Pipeline recovery for " + b + " is interrupted.",
                        e);
            }
        }
    }

    /**
 * * * Finalize a pending upgrade in response to DNA_FINALIZE.
     * @param blockPoolId the 数据块 pool to finalize
 */
    void finalizeUpgradeForPool(String blockPoolId) throws IOException {
        storage.finalizeUpgrade(blockPoolId);
    }

    static InetSocketAddress getStreamingAddr(Configuration conf) {
        return NetUtils.createSocketAddr(
                conf.getTrimmed(DFS_DATANODE_ADDRESS_KEY, DFS_DATANODE_ADDRESS_DEFAULT));
    }

    @Override // DataNodeMXBean
    public String getSoftwareVersion() {
        return VersionInfo.getVersion();
    }

    @Override // DataNodeMXBean
    public String getVersion() {
        return VersionInfo.getVersion() + ", r" + VersionInfo.getRevision();
    }

    @Override // DataNodeMXBean
    public String getRpcPort() {
        InetSocketAddress ipcAddr = NetUtils.createSocketAddr(
                this.getConf().get(DFS_DATANODE_IPC_ADDRESS_KEY));
        return Integer.toString(ipcAddr.getPort());
    }

    @Override // DataNodeMXBean
    public String getDataPort() {
        InetSocketAddress dataAddr = NetUtils.createSocketAddr(
                this.getConf().get(DFS_DATANODE_ADDRESS_KEY));
        return Integer.toString(dataAddr.getPort());
    }

    @Override // DataNodeMXBean
    public String getHttpPort() {
        return String.valueOf(infoPort);
    }

    @Override // DataNodeMXBean
    public long getDNStartedTimeInMillis() {
        return this.startTime;
    }

    public String getRevision() {
        return VersionInfo.getRevision();
    }

    /** * @返回 the 数据节点's HTTP（超文本传输协议） port */
    public int getInfoPort() {
        return infoPort;
    }

    /** * @返回 the 数据节点's https port */
    public int getInfoSecurePort() {
        return infoSecurePort;
    }

    /**
 * * * Returned information is a JSON representation of a map with
     * name node host name as the key and 数据块 pool Id as the 值.
     * Note that, if there are multiple NNs in an NA nameservice,
     * a given 数据块 pool may be represented twice.
 */
    @Override // DataNodeMXBean
    public String getNamenodeAddresses() {
        final Map<String, String> info = new HashMap<String, String>();
        for (BPOfferService bpos : blockPoolManager.getAllNamenodeThreads()) {
            if (bpos != null) {
                for (BPServiceActor actor : bpos.getBPServiceActors()) {
                    info.put(actor.getNNSocketAddress().getHostName(),
                            bpos.getBlockPoolId());
                }
            }
        }
        return JSON.toString(info);
    }

    /** * 返回 hostname of the 数据节点. */
    @Override // DataNodeMXBean
    public String getDatanodeHostname() {
        return this.hostName;
    }

    /**
 * * * Returned information is a JSON representation of an array,
     * each element of the array is a map contains the information
     * about a 数据块 pool service actor.
 */
    @Override // DataNodeMXBean
    public String getBPServiceActorInfo() {
        return JSON.toString(getBPServiceActorInfoMap());
    }

    @VisibleForTesting
    public List<Map<String, String>> getBPServiceActorInfoMap() {
        final List<Map<String, String>> infoArray = new ArrayList<>();
        for (BPOfferService bpos : blockPoolManager.getAllNamenodeThreads()) {
            if (bpos != null) {
                for (BPServiceActor actor : bpos.getBPServiceActors()) {
                    infoArray.add(actor.getActorInfoMap());
                }
            }
        }
        return infoArray;
    }

    /**
 * * * Returned information is a JSON representation of a map with
     * 卷 name as the key and 值 is a map of 卷 attribute
     * keys to its values
 */
    @Override // DataNodeMXBean
    public String getVolumeInfo() {
        if (data == null) {
            LOG.debug("Storage not yet initialized.");
            return "";
        }
        return JSON.toString(data.getVolumeInfoMap());
    }

    @Override // DataNodeMXBean
    public synchronized String getClusterId() {
        return clusterId;
    }

    @Override // DataNodeMXBean
    public String getDiskBalancerStatus() {
        try {
            return getDiskBalancer().queryWorkStatus().toJsonString();
        } catch (IOException ex) {
            LOG.debug("Reading diskbalancer Status failed.", ex);
            return "";
        }
    }

    /**
     * 检查集群是否启用了安全认证
     * 
     * @return 如果集群启用了安全认证则返回true，否则返回false
     */
    @Override
    public boolean isSecurityEnabled() {
        return UserGroupInformation.isSecurityEnabled();
    }

    /**
     * 根据新的配置刷新与NameNode的连接
     * 
     * 此方法负责根据提供的配置信息重新初始化或更新DataNode与NameNode之间的连接
     * 它可以用于动态添加或删除与NameNode的连接，支持HDFS联邦和高可用配置
     * 
     * @param conf 包含NameNode连接信息的新配置
     * @throws IOException 如果刷新过程中发生IO异常
     */
    public void refreshNamenodes(Configuration conf) throws IOException {
        // 委托给blockPoolManager处理实际的刷新操作
        blockPoolManager.refreshNamenodes(conf);
    }

    /**
     * (ClientDatanodeProtocol接口实现) 刷新与NameNode的连接配置
     * 
     * 这是一个RPC调用接口，允许客户端（通常是管理员）触发DataNode重新加载NameNode配置
     * 调用前会检查调用者是否有超级用户权限
     * 
     * @throws IOException 如果刷新过程中发生IO异常或调用者没有足够权限
     */
    @Override // ClientDatanodeProtocol
    public void refreshNamenodes() throws IOException {
        // 检查调用者是否有超级用户权限
        checkSuperuserPrivilege();
        // 创建新的Configuration对象，会自动加载最新的配置文件
        setConf(new Configuration());
        // 使用新配置刷新与NameNode的连接
        refreshNamenodes(getConf());
    }

    /**
     * (ClientDatanodeProtocol接口实现) 删除指定的块池
     * 
     * 此方法用于删除DataNode上与特定块池相关的所有数据和元数据
     * 通常在HDFS联邦配置中，当需要移除某个NameNode管理的块池时调用
     * 调用前会检查调用者是否有超级用户权限
     * 
     * @param blockPoolId 要删除的块池ID
     * @param force 是否强制删除，即使该块池仍有数据正在使用
     * @throws IOException 如果调用者没有足够权限、块池仍在运行或删除过程中发生IO异常
     */
    @Override // ClientDatanodeProtocol
    public void deleteBlockPool(String blockPoolId, boolean force)
            throws IOException {
        // 检查调用者是否有超级用户权限
        checkSuperuserPrivilege();
        // 记录删除块池的请求日志
        LOG.info("deleteBlockPool command received for block pool {}, " +
                "force={}", blockPoolId, force);
        // 检查该块池是否仍在运行
        if (blockPoolManager.get(blockPoolId) != null) {
            LOG.warn("The block pool {} is still running, cannot be deleted.",
                    blockPoolId);
            // 如果块池仍在运行，抛出异常要求先刷新NameNode连接以关闭块池服务
            throw new IOException(
                    "The block pool is still running. First do a refreshNamenodes to " +
                            "shutdown the block pool service");
        }
        // 检查存储是否已初始化
        checkStorageState("deleteBlockPool");
        // 委托给数据存储组件执行实际的块池删除操作
        data.deleteBlockPool(blockPoolId, force);
    }

    /**
     * 检查存储系统是否已初始化
     * 
     * 此方法在执行需要访问存储的操作前调用，确保DataNode的存储系统已正确初始化
     * 如果存储未初始化，将抛出IOException异常
     * 
     * @param methodName 调用此方法的方法名，用于日志记录
     * @throws IOException 如果存储系统尚未初始化
     */
    private void checkStorageState(String methodName) throws IOException {
        // 检查存储组件是否为null
        if (data == null) {
            String message = "Storage not yet initialized for " + methodName;
            LOG.debug(message);
            throw new IOException(message);
        }
    }

    /**
     * (ClientDatanodeProtocol接口实现) 关闭DataNode服务
     * 
     * 此方法用于安全地关闭DataNode服务，通常由管理员通过RPC调用触发
     * 调用前会检查调用者是否有超级用户权限
     * 关闭过程是异步执行的，这样可以立即返回RPC响应
     * 
     * @param forUpgrade 是否为了升级而关闭，这会影响关闭的行为和延迟时间
     * @throws IOException 如果调用者没有足够权限或关闭操作已在进行中
     */
    @Override // ClientDatanodeProtocol
    public synchronized void shutdownDatanode(boolean forUpgrade) throws IOException {
        // 检查调用者是否有超级用户权限
        checkSuperuserPrivilege();
        // 记录关闭DataNode的请求日志
        LOG.info("shutdownDatanode command received (upgrade={}). " +
                "Shutting down Datanode...", forUpgrade);

        // 关闭操作只能执行一次
        if (shutdownInProgress) {
            throw new IOException("Shutdown already in progress.");
        }
        // 设置关闭标志
        shutdownInProgress = true;
        shutdownForUpgrade = forUpgrade;

        // 创建一个异步线程来执行实际的关闭过程，这样可以立即返回RPC响应
        Thread shutdownThread = new Thread("Async datanode shutdown thread") {
            @Override
            public void run() {
                if (!shutdownForUpgrade) {
                    // 如果不是为了升级而关闭，则稍微延迟关闭过程
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        // 忽略中断异常
                    }
                }
                // 调用实际的关闭方法
                shutdown();
            }
        };

        // 将关闭线程设置为守护线程，这样它不会阻止JVM退出
        shutdownThread.setDaemon(true);
        // 启动关闭线程
        shutdownThread.start();
    }

    /**
     * (ClientDatanodeProtocol接口实现) 驱逐所有正在写入数据的客户端
     * 
     * 此方法用于强制中断所有当前正在向DataNode写入数据的客户端连接
     * 通常在需要紧急维护或关闭DataNode前调用，以确保数据一致性
     * 调用前会检查调用者是否有超级用户权限
     * 
     * @throws IOException 如果调用者没有足够权限
     */
    @Override // ClientDatanodeProtocol
    public void evictWriters() throws IOException {
        // 检查调用者是否有超级用户权限
        checkSuperuserPrivilege();
        // 记录驱逐所有写入器的日志
        LOG.info("Evicting all writers.");
        // 调用数据传输服务器的方法来停止所有写入操作
        xserver.stopWriters();
    }

    /**
 * * * (ClientDatanodeProtocol接口实现) 获取DataNode的本地信息
     * 
     * 此方法返回DataNode的基本信息，包括版本、配置版本和运行时间
     * 这些信息对于监控和管理DataNode非常有用
     * 
     * @返回 包含DataNode版本信息、配置版本和运行时间的DatanodeLocalInfo对象
 */
    @Override // ClientDatanodeProtocol
    public DatanodeLocalInfo getDatanodeInfo() {
        // 获取DataNode的运行时间（以秒为单位）
        long uptime = ManagementFactory.getRuntimeMXBean().getUptime() / 1000;
        // 创建并返回包含版本信息、配置版本和运行时间的DatanodeLocalInfo对象
        return new DatanodeLocalInfo(VersionInfo.getVersion(),
                confVersion, uptime);
    }

    @Override // ClientDatanodeProtocol & ReconfigurationProtocol
    public void startReconfiguration() throws IOException {
        checkSuperuserPrivilege();
        startReconfigurationTask();
    }

    @Override // ClientDatanodeProtocol & ReconfigurationProtocol
    public ReconfigurationTaskStatus getReconfigurationStatus() throws IOException {
        checkSuperuserPrivilege();
        return getReconfigurationTaskStatus();
    }

    @Override // ClientDatanodeProtocol & ReconfigurationProtocol
    public List<String> listReconfigurableProperties()
            throws IOException {
        checkSuperuserPrivilege();
        return RECONFIGURABLE_PROPERTIES;
    }

    @Override // ClientDatanodeProtocol
    public void triggerBlockReport(BlockReportOptions options)
            throws IOException {
        checkSuperuserPrivilege();
        InetSocketAddress namenodeAddr = options.getNamenodeAddr();
        boolean shouldTriggerToAllNn = (namenodeAddr == null);
        for (BPOfferService bpos : blockPoolManager.getAllNamenodeThreads()) {
            if (bpos != null) {
                for (BPServiceActor actor : bpos.getBPServiceActors()) {
                    if (shouldTriggerToAllNn || namenodeAddr.equals(actor.nnAddr)) {
                        actor.triggerBlockReport(options);
                    }
                }
            }
        }
    }

    /**
 * * * @param addr RPC（远程过程调用）（远程过程调用） address of the 名称节点
     * @返回 true if the 数据节点 is connected to a 名称节点 at the
     * given address
 */
    public boolean isConnectedToNN(InetSocketAddress addr) {
        for (BPOfferService bpos : getAllBpOs()) {
            for (BPServiceActor bpsa : bpos.getBPServiceActors()) {
                if (addr.equals(bpsa.getNNSocketAddress())) {
                    return bpsa.isAlive();
                }
            }
        }
        return false;
    }

    /**
 * * * @param bpid 数据块 pool Id
     * @返回 true - if BPOfferService 线程 is alive
 */
    public boolean isBPServiceAlive(String bpid) {
        BPOfferService bp = blockPoolManager.get(bpid);
        return bp != null ? bp.isAlive() : false;
    }

    boolean isRestarting() {
        return shutdownForUpgrade;
    }

    /**
 * * * A 数据节点 is considered to be fully started if all the BP threads are
     * alive and all the 数据块 pools are initialized.
     *
     * @返回 true - if the data node is fully started
 */
    public boolean isDatanodeFullyStarted() {
        return isDatanodeFullyStarted(false);
    }

    /**
 * * * A 数据节点 is considered to be fully started if all the BP threads are
     * alive and all the 数据块 pools are initialized. If checkConnectionToActiveNamenode is true,
     * the 数据节点 is considered to be fully started if it is also heartbeating to
     * active 名称节点 in addition to the above-mentioned conditions.
     *
     * @param checkConnectionToActiveNamenode if true, performs additional check of whether 数据节点
     * is heartbeating to active 名称节点.
     * @返回 true if the 数据节点 is fully started and also conditionally connected to active
     * 名称节点, false otherwise.
 */
    public boolean isDatanodeFullyStarted(boolean checkConnectionToActiveNamenode) {
        if (checkConnectionToActiveNamenode) {
            for (BPOfferService bp : blockPoolManager.getAllNamenodeThreads()) {
                if (!bp.isInitialized() || !bp.isAlive() || bp.getActiveNN() == null) {
                    return false;
                }
            }
            return true;
        }
        for (BPOfferService bp : blockPoolManager.getAllNamenodeThreads()) {
            if (!bp.isInitialized() || !bp.isAlive()) {
                return false;
            }
        }
        return true;
    }

    @VisibleForTesting
    public DatanodeID getDatanodeId() {
        return id;
    }

    @VisibleForTesting
    public void clearAllBlockSecretKeys() {
        blockPoolTokenSecretManager.clearAllKeysForTesting();
    }

    @Override // ClientDatanodeProtocol
    public long getBalancerBandwidth() {
        DataXceiverServer dxcs =
                (DataXceiverServer) this.dataXceiverServer.getRunnable();
        return dxcs.balanceThrottler.getBandwidth();
    }

    public DNConf getDnConf() {
        return dnConf;
    }

    public String getDatanodeUuid() {
        return storage == null ? null : storage.getDatanodeUuid();
    }

    boolean shouldRun() {
        return shouldRun;
    }

    @VisibleForTesting
    DataStorage getStorage() {
        return storage;
    }

    public ShortCircuitRegistry getShortCircuitRegistry() {
        return shortCircuitRegistry;
    }

    public DataTransferThrottler getEcReconstuctReadThrottler() {
        return ecReconstuctReadThrottler;
    }

    public DataTransferThrottler getEcReconstuctWriteThrottler() {
        return ecReconstuctWriteThrottler;
    }

    /** * Check the 磁盘 error synchronously. */
    @VisibleForTesting
    public void checkDiskError() throws IOException {
        Set<FsVolumeSpi> unhealthyVolumes;
        try {
            unhealthyVolumes = volumeChecker.checkAllVolumes(data);
            lastDiskErrorCheck = Time.monotonicNow();
        } catch (InterruptedException e) {
            LOG.error("Interrupted while running disk check", e);
            throw new IOException("Interrupted while running disk check", e);
        }

        if (unhealthyVolumes.size() > 0) {
            LOG.warn("checkDiskError got {} failed volumes - {}",
                    unhealthyVolumes.size(), unhealthyVolumes);
            handleVolumeFailures(unhealthyVolumes);
        } else {
            LOG.debug("checkDiskError encountered no failures");
        }
    }

    private void handleVolumeFailures(Set<FsVolumeSpi> unhealthyVolumes) {
        if (unhealthyVolumes.isEmpty()) {
            LOG.debug("handleVolumeFailures done with empty " +
                    "unhealthyVolumes");
            return;
        }

        data.handleVolumeFailures(unhealthyVolumes);
        int failedNumber = unhealthyVolumes.size();
        Set<StorageLocation> unhealthyLocations = new HashSet<>(failedNumber);

        StringBuilder sb = new StringBuilder("DataNode failed volumes:");
        for (FsVolumeSpi vol : unhealthyVolumes) {
            unhealthyLocations.add(vol.getStorageLocation());
            sb.append(vol.getStorageLocation()).append(";");
        }

        try {
            // Remove all unhealthy volumes from 数据节点.
            removeVolumes(unhealthyLocations, false);
        } catch (IOException e) {
            LOG.warn("Error occurred when removing unhealthy storage dirs", e);
        }
        LOG.debug("{}", sb);
        // send blockreport regarding 卷 failure
        handleDiskError(sb.toString(), failedNumber);
    }

    /**
 * * * A bad 数据块 need to be handled, either to add to blockScanner suspect queue
     * or report to 名称节点 directly.
     *
     * If the 方法 is called by scanner, then the 数据块 must be a bad 数据块, we
     * report it to 名称节点 directly. Otherwise if we judge it as a bad 数据块
     * according to 异常 type, then we 尝试 to add the bad 数据块 to
     * blockScanner suspect queue if blockScanner is enabled, or report to
     * 名称节点 directly otherwise.
     *
     * @param 数据块 The suspicious 数据块
     * @param e The 异常 encountered when accessing the 数据块
     * @param fromScanner Is it from blockScanner. The blockScanner will call this
     *          方法 only when it's sure that the 数据块 is corrupt.
 */
    void handleBadBlock(ExtendedBlock block, IOException e, boolean fromScanner) {

        boolean isBadBlock = fromScanner || (e instanceof DiskFileCorruptException
                || e instanceof CorruptMetaHeaderException);

        if (!isBadBlock) {
            return;
        }
        if (!fromScanner && blockScanner.isEnabled()) {
            FsVolumeSpi volume = data.getVolume(block);
            if (volume == null) {
                LOG.warn("Cannot find FsVolumeSpi to handle bad block: {}", block);
                return;
            }
            blockScanner.markSuspectBlock(volume.getStorageID(), block);
        } else {
            try {
                reportBadBlocks(block);
            } catch (IOException ie) {
                LOG.warn("report bad block {} failed", block, ie);
            }
        }
    }

    @VisibleForTesting
    public long getLastDiskErrorCheck() {
        return lastDiskErrorCheck;
    }

    public BlockRecoveryWorker getBlockRecoveryWorker() {
        return blockRecoveryWorker;
    }

    public ErasureCodingWorker getErasureCodingWorker() {
        return ecWorker;
    }

    IOStreamPair connectToDN(DatanodeInfo datanodeID, int timeout,
                             ExtendedBlock block,
                             Token<BlockTokenIdentifier> blockToken)
            throws IOException {

        return DFSUtilClient.connectToDN(datanodeID, timeout, getConf(),
                saslClient, NetUtils.getDefaultSocketFactory(getConf()), false,
                getDataEncryptionKeyFactoryForBlock(block), blockToken);
    }

    /**
     * Get timeout value of each OOB type from configuration
     */
    private void initOOBTimeout() {
        final int oobStart = Status.OOB_RESTART_VALUE; // the first OOB type
        final int oobEnd = Status.OOB_RESERVED3_VALUE; // the last OOB type
        final int numOobTypes = oobEnd - oobStart + 1;
        oobTimeouts = new long[numOobTypes];

        final String[] ele = getConf().get(DFS_DATANODE_OOB_TIMEOUT_KEY,
                DFS_DATANODE_OOB_TIMEOUT_DEFAULT).split(",");
        for (int i = 0; i < numOobTypes; i++) {
            oobTimeouts[i] = (i < ele.length) ? Long.parseLong(ele[i]) : 0;
        }
    }

    /**
     * Get the timeout to be used for transmitting the OOB type
     * @return the timeout in milliseconds
     */
    public long getOOBTimeout(Status status)
            throws IOException {
        if (status.getNumber() < Status.OOB_RESTART_VALUE ||
                status.getNumber() > Status.OOB_RESERVED3_VALUE) {
            // Not an OOB.
            throw new IOException("Not an OOB status: " + status);
        }

        return oobTimeouts[status.getNumber() - Status.OOB_RESTART_VALUE];
    }

    /**
     * Start a timer to periodically write DataNode metrics to the log file. This
     * behavior can be disabled by configuration.
     *
     */
    protected void startMetricsLogger() {
        long metricsLoggerPeriodSec = getConf().getInt(
                DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY,
                DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_DEFAULT);

        if (metricsLoggerPeriodSec <= 0) {
            return;
        }

        // Schedule the periodic logging.
        metricsLoggerTimer = new ScheduledThreadPoolExecutor(1);
        metricsLoggerTimer.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        metricsLoggerTimer.scheduleWithFixedDelay(new MetricsLoggerTask(METRICS_LOG_NAME,
                        "DataNode", (short) 0), metricsLoggerPeriodSec, metricsLoggerPeriodSec,
                TimeUnit.SECONDS);
    }

    protected void stopMetricsLogger() {
        if (metricsLoggerTimer != null) {
            metricsLoggerTimer.shutdown();
            metricsLoggerTimer = null;
        }
    }

    @VisibleForTesting
    ScheduledThreadPoolExecutor getMetricsLoggerTimer() {
        return metricsLoggerTimer;
    }

    public Tracer getTracer() {
        return tracer;
    }

    /**
     * Allows submission of a disk balancer Job.
     * @param planID  - Hash value of the plan.
     * @param planVersion - Plan version, reserved for future use. We have only
     *                    version 1 now.
     * @param planFile - Plan file name
     * @param planData - Actual plan data in json format
     * @throws IOException
     */
    @Override
    public void submitDiskBalancerPlan(String planID, long planVersion,
                                       String planFile, String planData, boolean skipDateCheck)
            throws IOException {
        checkSuperuserPrivilege();
        if (getStartupOption(getConf()) != StartupOption.REGULAR) {
            throw new DiskBalancerException(
                    "Datanode is in special state, e.g. Upgrade/Rollback etc."
                            + " Disk balancing not permitted.",
                    DiskBalancerException.Result.DATANODE_STATUS_NOT_REGULAR);
        }

        getDiskBalancer().submitPlan(planID, planVersion, planFile, planData,
                skipDateCheck);
    }

    /**
     * Cancels a running plan.
     * @param planID - Hash string that identifies a plan
     */
    @Override
    public void cancelDiskBalancePlan(String planID) throws
            IOException {
        checkSuperuserPrivilege();
        getDiskBalancer().cancelPlan(planID);
    }

    /**
     * Returns the status of current or last executed work plan.
     * @return DiskBalancerWorkStatus.
     * @throws IOException
     */
    @Override
    public DiskBalancerWorkStatus queryDiskBalancerPlan() throws IOException {
        checkSuperuserPrivilege();
        return getDiskBalancer().queryWorkStatus();
    }

    /**
     * Gets a runtime configuration value from  diskbalancer instance. For
     * example : DiskBalancer bandwidth.
     *
     * @param key - String that represents the run time key value.
     * @return value of the key as a string.
     * @throws IOException - Throws if there is no such key
     */
    @Override
    public String getDiskBalancerSetting(String key) throws IOException {
        checkSuperuserPrivilege();
        Preconditions.checkNotNull(key);
        switch (key) {
            case DiskBalancerConstants.DISKBALANCER_VOLUME_NAME:
                return getDiskBalancer().getVolumeNames();
            case DiskBalancerConstants.DISKBALANCER_BANDWIDTH:
                return Long.toString(getDiskBalancer().getBandwidth());
            default:
                LOG.error("Disk Balancer - Unknown key in get balancer setting. Key: {}",
                        key);
                throw new DiskBalancerException("Unknown key",
                        DiskBalancerException.Result.UNKNOWN_KEY);
        }
    }

    @VisibleForTesting
    void setBlockScanner(BlockScanner blockScanner) {
        this.blockScanner = blockScanner;
    }

    @Override // DataNodeMXBean
    public String getSendPacketDownstreamAvgInfo() {
        return dnConf.peerStatsEnabled && peerMetrics != null ?
                peerMetrics.dumpSendPacketDownstreamAvgInfoAsJson() : null;
    }

    @Override // DataNodeMXBean
    public String getSlowDisks() {
        if (!dnConf.diskStatsEnabled || diskMetrics == null) {
            //Disk Stats not enabled
            return null;
        }
        Set<String> slowDisks = diskMetrics.getDiskOutliersStats().keySet();
        return JSON.toString(slowDisks);
    }


    @Override
    public List<DatanodeVolumeInfo> getVolumeReport() throws IOException {
        checkSuperuserPrivilege();
        checkStorageState("getVolumeReport");
        Map<String, Object> volumeInfoMap = data.getVolumeInfoMap();
        if (volumeInfoMap == null) {
            LOG.warn("DataNode volume info not available.");
            return new ArrayList<>(0);
        }
        List<DatanodeVolumeInfo> volumeInfoList = new ArrayList<>();
        for (Entry<String, Object> volume : volumeInfoMap.entrySet()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> volumeInfo = (Map<String, Object>) volume.getValue();
            DatanodeVolumeInfo dnStorageInfo = new DatanodeVolumeInfo(
                    volume.getKey(), (Long) volumeInfo.get("usedSpace"),
                    (Long) volumeInfo.get("freeSpace"),
                    (Long) volumeInfo.get("reservedSpace"),
                    (Long) volumeInfo.get("reservedSpaceForReplicas"),
                    (Long) volumeInfo.get("numBlocks"),
                    (StorageType) volumeInfo.get("storageType"));
            volumeInfoList.add(dnStorageInfo);
        }
        return volumeInfoList;
    }

    @VisibleForTesting
    public DiskBalancer getDiskBalancer() throws IOException {
        if (this.diskBalancer == null) {
            throw new IOException("DiskBalancer is not initialized");
        }
        return this.diskBalancer;
    }

    /**
     * Construct DataTransfer in {@link DataNode#transferBlock}, the
     * BlockConstructionStage is PIPELINE_SETUP_CREATE and clientName is "".
     */
    private static boolean isTransfer(BlockConstructionStage stage,
                                      String clientName) {
        if (stage == PIPELINE_SETUP_CREATE && clientName.isEmpty()) {
            return true;
        }
        return false;
    }

    /**
 * * Construct DataTransfer in
     * {@link 数据节点#transferReplicaForPipelineRecovery}.
     *
     * When recover pipeline, BlockConstructionStage is
     * PIPELINE_SETUP_APPEND_RECOVERY,
     * PIPELINE_SETUP_STREAMING_RECOVERY,PIPELINE_CLOSE_RECOVERY. If
     * BlockConstructionStage is PIPELINE_CLOSE_RECOVERY, don't need transfer
     * replica. So BlockConstructionStage is PIPELINE_SETUP_APPEND_RECOVERY,
     * PIPELINE_SETUP_STREAMING_RECOVERY.
 */
    private static boolean isWrite(BlockConstructionStage stage) {
        return (stage == PIPELINE_SETUP_STREAMING_RECOVERY
                || stage == PIPELINE_SETUP_APPEND_RECOVERY);
    }

    public DataSetLockManager getDataSetLockManager() {
        return dataSetLockManager;
    }

    boolean isSlownodeByBlockPoolId(String bpId) {
        return blockPoolManager.isSlownodeByBlockPoolId(bpId);
    }

    boolean isSlownode() {
        return blockPoolManager.isSlownode();
    }

    @VisibleForTesting
    public BlockPoolManager getBlockPoolManager() {
        return blockPoolManager;
    }
}
