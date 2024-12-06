/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.security.KerberosInfo;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

/**********************************************************************
 * Protocol that a DFS datanode uses to communicate with the NameNode.
 * It's used to upload current load information and block reports.
 *
 * The only way a NameNode can communicate with a DataNode is by
 * returning values from these functions.
 *
 **********************************************************************/
@KerberosInfo(
        serverPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY,
        clientPrincipal = DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
/**
 * todo DatanodeProtocol是Datanode与Namenode间的接口， Datanode会使用这个接口与Namenode握手、 注册、 发送心跳、 进行全量以及增量的数据块汇报。
 * todo Namenode会在Datanode的心跳响应中携带名字节点指令， Datanode收到名字节点指令之后会执行对应的操作。

 */
public interface DatanodeProtocol {
    /**
     * This class is used by both the Namenode (client) and BackupNode (server)
     * to insulate from the protocol serialization.
     *
     * If you are adding/changing DN's interface then you need to
     * change both this class and ALSO related protocol buffer
     * wire protocol definition in DatanodeProtocol.proto.
     *
     * For more details on protocol buffer wire protocol, please see
     * .../org/apache/hadoop/hdfs/protocolPB/overview.html
     */
    public static final long versionID = 28L;

    // error code
    final static int NOTIFY = 0;
    final static int DISK_ERROR = 1; // there are still valid volumes on DN
    final static int INVALID_BLOCK = 2;
    final static int FATAL_DISK_ERROR = 3; // no valid volumes left on DN

    /**
     * Determines actions that data node should perform
     * when receiving a datanode command.
     */
    //todo 未定义
    final static int DNA_UNKNOWN = 0;    // unknown action
    //
    //todo 数据块复制
    // DNA_TRANSFER指令用于触发数据节点的数据块复制操作，
    // 当HDFS系统中某个数据块 的副本数小于配置的副本系数时，
    // Namenode会通过DNA_TRANSFER指令通知某个拥有这个数据块副本的Datanode将该数据块复制到其他数据节点上。
    final static int DNA_TRANSFER = 1;   // transfer blocks to another datanode
    //todo 数据库删除
    // todo DNA_INVALIDATE用于 通知Datanode删除数据节点上的指定数据块，
    // todo 这是因为Namenode发现了某个数据块的副 本数已经超过了配置的副本系数，
    // todo 这时Namenode会通知某个数据节点删除这个数据节点 上多余的数据块副本。
    final static int DNA_INVALIDATE = 2; // invalidate blocks
    // todo 关闭数据节点
    // todo DNA_SHUTDOWN已经废弃不用了，
    // todo Datanode接收到DNASHUTDOWN指令后会直接抛出UnsupportedOperationException异常。
    // todo 关闭Datanode是通过 调用ClientDatanodeProtocol.shutdownDatanode()方法来触发的。
    final static int DNA_SHUTDOWN = 3;   // shutdown node
    //todo 重新注册数据节点
    final static int DNA_REGISTER = 4;   // re-register
    //todo 提交上一次升级
    final static int DNA_FINALIZE = 5;   // finalize previous upgrade

    //todo 数据块恢复
    //todo 当客户端在写文件时发生异常退出，会造成数据流管道中不同数据 节点上数据块状态的不一致，
    // 这时Namenode会从数据流管道中选出一个数据节点作为主 恢复节点，
    // 协调数据流管道中的其他数据节点进行租约恢复操作，以同步这个数据块的状 态。
    final static int DNA_RECOVERBLOCK = 6;  // request a block recovery
    //todo   //安全相关
    final static int DNA_ACCESSKEYUPDATE = 7;  // update access key
    // todo 更新平衡器宽度
    final static int DNA_BALANCERBANDWIDTHUPDATE = 8; // update balancer bandwidth
    //todo 缓存数据块
    final static int DNA_CACHE = 9;      // cache blocks
    //todo   //取消缓存数据块
    final static int DNA_UNCACHE = 10;   // uncache blocks
    //todo  //擦除编码重建命令
    final static int DNA_ERASURE_CODING_RECONSTRUCTION = 11; // erasure coding reconstruction command
    //todo 块存储移动命令
    int DNA_BLOCK_STORAGE_MOVEMENT = 12; // block storage movement command
    //todo 删除sps工作命令
    int DNA_DROP_SPS_WORK_COMMAND = 13; // drop sps work command

    /**
     * Register Datanode.
     *
     * @see org.apache.hadoop.hdfs.server.namenode.FSNamesystem#registerDatanode(DatanodeRegistration)
     * @param registration datanode registration information
     * @return the given {@link org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration} with
     *  updated registration information
     */
    /**
     * todo * 一个完整的Datanode启动操作会与Namenode进行4次交互，也就是调用4次 DatanodeProtocol定义的方法。
     *    * 首先调用versionRequest()与Namenode进行握手操作，
     *    * 然后 调用registerDatanode()向Namenode注册当前的Datanode，
     *    * 接着调用blockReport()汇报 Datanode上存储的所有数据块，
     *    * 最后调用cacheReport()汇报Datanode缓存的所有数据块。
     *    *
     *    * 成功进行握手操作后，
     *    * Datanode会调用ClientProtocol.registerDatanode()方法向 Namenode注册当前的Datanode，
     *    * 这个方法的参数是一个DatanodeRegistration对象，
     *    * 它封装 了DatanodeID、Datanode的存储系统的布局版本号(layoutversion)、
     *    * 当前命名空间的 ID(namespaceId)、集群ID(clusterId)、
     *    * 文件系统的创建时间(ctime)以及Datanode当 前的软件版本号(softwareVersion)。
     *    *
     *    * namenode节点会判断Datanode的软件版本号与Namenode 的软件版本号是否兼容，
     *    * 如果兼容则进行注册操作，并返回一个DatanodeRegistration对象 供Datanode后续处理逻辑使用。
     * @param registration
     * @return
     * @throws IOException
     */
    @Idempotent
    public DatanodeRegistration registerDatanode(DatanodeRegistration registration
    ) throws IOException;

    /**
     * sendHeartbeat() tells the NameNode that the DataNode is still
     * alive and well.  Includes some status info, too.
     * It also gives the NameNode a chance to return
     * an array of "DatanodeCommand" objects in HeartbeatResponse.
     * A DatanodeCommand tells the DataNode to invalidate local block(s),
     * or to copy them to other DataNodes, etc.
     * @param registration datanode registration information.
     * @param reports utilization report per storage.
     * @param dnCacheCapacity the total cache capacity of the datanode (in bytes).
     * @param dnCacheUsed the amount of cache used by the datanode (in bytes).
     * @param xmitsInProgress number of transfers from this datanode to others.
     * @param xceiverCount number of active transceiver threads.
     * @param failedVolumes number of failed volumes.
     * @param volumeFailureSummary info about volume failures.
     * @param requestFullBlockReportLease whether to request a full block
     *                                    report lease.
     * @param slowPeers Details of peer DataNodes that were detected as being
     *                  slow to respond to packet writes. Empty report if no
     *                  slow peers were detected by the DataNode.
     * @param slowDisks Details of disks on DataNodes that were detected as
     *                  being slow. Empty report if no slow disks were detected.
     * @throws IOException on error.
     */
    @Idempotent
    public HeartbeatResponse sendHeartbeat(DatanodeRegistration registration,
                                           StorageReport[] reports,
                                           long dnCacheCapacity,
                                           long dnCacheUsed,
                                           int xmitsInProgress,
                                           int xceiverCount,
                                           int failedVolumes,
                                           VolumeFailureSummary volumeFailureSummary,
                                           boolean requestFullBlockReportLease,
                                           @Nonnull SlowPeerReports slowPeers,
                                           @Nonnull SlowDiskReports slowDisks)
            throws IOException;

    /**
     * blockReport() tells the NameNode about all the locally-stored blocks.
     * The NameNode returns an array of Blocks that have become obsolete
     * and should be deleted.  This function is meant to upload *all*
     * the locally-stored blocks.  It's invoked upon startup and then
     * infrequently afterwards.
     * @param registration datanode registration
     * @param poolId the block pool ID for the blocks
     * @param reports report of blocks per storage
     *     Each finalized block is represented as 3 longs. Each under-
     *     construction replica is represented as 4 longs.
     *     This is done instead of Block[] to reduce memory used by block reports.
     * @param reports report of blocks per storage
     * @param context Context information for this block report.
     *
     * @return - the next command for DN to process.
     * @throws IOException
     */
    @Idempotent
    public DatanodeCommand blockReport(DatanodeRegistration registration,
                                       String poolId, StorageBlockReport[] reports,
                                       BlockReportContext context) throws IOException;


    /**
     * Communicates the complete list of locally cached blocks to the NameNode.
     *
     * This method is similar to
     * {@link #blockReport(DatanodeRegistration, String, StorageBlockReport[], BlockReportContext)},
     * which is used to communicated blocks stored on disk.
     *
     * @param registration The datanode registration.
     * @param poolId     The block pool ID for the blocks.
     * @param blockIds   A list of block IDs.
     * @return The DatanodeCommand.
     * @throws IOException
     */
    @Idempotent
    public DatanodeCommand cacheReport(DatanodeRegistration registration,
                                       String poolId, List<Long> blockIds) throws IOException;

    /**
     * blockReceivedAndDeleted() allows the DataNode to tell the NameNode about
     * recently-received and -deleted block data.
     *
     * For the case of received blocks, a hint for preferred replica to be
     * deleted when there is any excessive blocks is provided.
     * For example, whenever client code
     * writes a new Block here, or another DataNode copies a Block to
     * this DataNode, it will call blockReceived().
     */
    @Idempotent
    public void blockReceivedAndDeleted(DatanodeRegistration registration,
                                        String poolId,
                                        StorageReceivedDeletedBlocks[] rcvdAndDeletedBlocks)
            throws IOException;

    /**
     * errorReport() tells the NameNode about something that has gone
     * awry.  Useful for debugging.
     */
    @Idempotent
    public void errorReport(DatanodeRegistration registration,
                            int errorCode,
                            String msg) throws IOException;

    @Idempotent
    public NamespaceInfo versionRequest() throws IOException;

    /**
     * same as {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#reportBadBlocks(LocatedBlock[])}
     * }
     */
    @Idempotent
    public void reportBadBlocks(LocatedBlock[] blocks) throws IOException;

    /**
     * Commit block synchronization in lease recovery
     */
    @Idempotent
    public void commitBlockSynchronization(ExtendedBlock block,
                                           long newgenerationstamp, long newlength,
                                           boolean closeFile, boolean deleteblock, DatanodeID[] newtargets,
                                           String[] newtargetstorages) throws IOException;
}
