/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.hdfs.server.datanode.DataNode.DN_CLIENTTRACE_FORMAT;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Checksum;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSPacket;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketReceiver;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodePeerMetrics;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.tracing.Span;
import org.apache.hadoop.tracing.Tracer;

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_DONTNEED;
import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.SYNC_FILE_RANGE_WRITE;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;

/** A class that receives a block and writes to its own disk, meanwhile
 * may copies it to another site. If a throttler is provided,
 * streaming throttling is also supported.
 **/
class BlockReceiver implements Closeable {
  public static final Logger LOG = DataNode.LOG;
  static final Logger CLIENT_TRACE_LOG = DataNode.CLIENT_TRACE_LOG;

  @VisibleForTesting
  static long CACHE_DROP_LAG_BYTES = 8 * 1024 * 1024;
  private final long datanodeSlowLogThresholdMs;
  private DataInputStream in = null; // from where data are read
  private DataChecksum clientChecksum; // checksum used by client
  private DataChecksum diskChecksum; // checksum we write to disk
  
  /**
   * In the case that the client is writing with a different
   * checksum polynomial than the block is stored with on disk,
   * the DataNode needs to recalculate checksums before writing.
   */
  private final boolean needsChecksumTranslation;
  private DataOutputStream checksumOut = null; // to crc file at local disk
  private final int bytesPerChecksum;
  private final int checksumSize;
  
  private final PacketReceiver packetReceiver = new PacketReceiver(false);
  
  protected final String inAddr;
  protected final String myAddr;
  private String mirrorAddr;
  private String mirrorNameForMetrics;
  private DataOutputStream mirrorOut;
  private Daemon responder = null;
  private DataTransferThrottler throttler;
  private ReplicaOutputStreams streams;
  private DatanodeInfo srcDataNode = null;
  private DatanodeInfo[] downstreamDNs = DatanodeInfo.EMPTY_ARRAY;
  private final DataNode datanode;
  volatile private boolean mirrorError;

  // Cache management state
  private boolean dropCacheBehindWrites;
  private long lastCacheManagementOffset = 0;
  private boolean syncBehindWrites;
  private boolean syncBehindWritesInBackground;

  /** The client name.  It is empty if a datanode is the client */
  private final String clientname;
  private final boolean isClient; 
  private final boolean isDatanode;

  /** the block to receive */
  private final ExtendedBlock block; 
  /** the replica to write */
  private ReplicaInPipeline replicaInfo;
  /** pipeline stage */
  private final BlockConstructionStage stage;
  private final boolean isTransfer;
  private boolean isPenultimateNode = false;

  private boolean syncOnClose;
  private volatile boolean dirSyncOnFinalize;
  private boolean dirSyncOnHSyncDone = false;
  private long restartBudget;
  /** the reference of the volume where the block receiver writes to */
  private ReplicaHandler replicaHandler;

  /**
   * for replaceBlock response
   */
  private final long responseInterval;
  private long lastResponseTime = 0;
  private boolean isReplaceBlock = false;
  private DataOutputStream replyOut = null;
  private long maxWriteToDiskMs = 0;
  
  private boolean pinning;
  private final AtomicLong lastSentTime = new AtomicLong(0L);
  private long maxSendIdleTime;

  BlockReceiver(final ExtendedBlock block, final StorageType storageType,
      final DataInputStream in,
      final String inAddr, final String myAddr,
      final BlockConstructionStage stage, 
      final long newGs, final long minBytesRcvd, final long maxBytesRcvd, 
      final String clientname, final DatanodeInfo srcDataNode,
      final DataNode datanode, DataChecksum requestedChecksum,
      CachingStrategy cachingStrategy,
      final boolean allowLazyPersist,
      final boolean pinning,
      final String storageId) throws IOException {
    try{
      this.block = block;
      this.in = in;
      this.inAddr = inAddr;
      this.myAddr = myAddr;
      this.srcDataNode = srcDataNode;
      this.datanode = datanode;

      this.clientname = clientname;
      this.isDatanode = clientname.length() == 0;
      this.isClient = !this.isDatanode;
      this.restartBudget = datanode.getDnConf().restartReplicaExpiry;
      this.datanodeSlowLogThresholdMs =
          datanode.getDnConf().getSlowIoWarningThresholdMs();
      // For replaceBlock() calls response should be sent to avoid socketTimeout
      // at clients. So sending with the interval of 0.5 * socketTimeout
      final long readTimeout = datanode.getDnConf().socketTimeout;
      this.responseInterval = (long) (readTimeout * 0.5);
      //for datanode, we have
      //1: clientName.length() == 0, and
      //2: stage == null or PIPELINE_SETUP_CREATE
      this.stage = stage;
      this.isTransfer = stage == BlockConstructionStage.TRANSFER_RBW
          || stage == BlockConstructionStage.TRANSFER_FINALIZED;

      this.pinning = pinning;
      this.lastSentTime.set(Time.monotonicNow());
      // Downstream will timeout in readTimeout on receiving the next packet.
      // If there is no data traffic, a heartbeat packet is sent at
      // the interval of 0.5*readTimeout. Here, we set 0.9*readTimeout to be
      // the threshold for detecting congestion.
      this.maxSendIdleTime = (long) (readTimeout * 0.9);
      if (LOG.isDebugEnabled()) {
        LOG.debug(getClass().getSimpleName() + ": " + block
            + "\n storageType=" + storageType + ", inAddr=" + inAddr
            + ", myAddr=" + myAddr + "\n stage=" + stage + ", newGs=" + newGs
            + ", minBytesRcvd=" + minBytesRcvd
            + ", maxBytesRcvd=" + maxBytesRcvd + "\n clientname=" + clientname
            + ", srcDataNode=" + srcDataNode
            + ", datanode=" + datanode.getDisplayName()
            + "\n requestedChecksum=" + requestedChecksum
            + "\n cachingStrategy=" + cachingStrategy
            + "\n allowLazyPersist=" + allowLazyPersist + ", pinning=" + pinning
            + ", isClient=" + isClient + ", isDatanode=" + isDatanode
            + ", responseInterval=" + responseInterval
            + ", storageID=" + (storageId != null ? storageId : "null")
        );
      }

      //
      // Open local disk out
      //
      if (isDatanode) { //replication or move
        replicaHandler =
            datanode.data.createTemporary(storageType, storageId, block, false);
      } else {
        switch (stage) {
        case PIPELINE_SETUP_CREATE:
          replicaHandler = datanode.data.createRbw(storageType, storageId,
              block, allowLazyPersist, newGs);
          if (newGs != 0L) {
            block.setGenerationStamp(newGs);
          }
          datanode.notifyNamenodeReceivingBlock(
              block, replicaHandler.getReplica().getStorageUuid());
          break;
        case PIPELINE_SETUP_STREAMING_RECOVERY:
          replicaHandler = datanode.data.recoverRbw(
              block, newGs, minBytesRcvd, maxBytesRcvd);
          block.setGenerationStamp(newGs);
          break;
        case PIPELINE_SETUP_APPEND:
          replicaHandler = datanode.data.append(block, newGs, minBytesRcvd);
          block.setGenerationStamp(newGs);
          datanode.notifyNamenodeReceivingBlock(
              block, replicaHandler.getReplica().getStorageUuid());
          break;
        case PIPELINE_SETUP_APPEND_RECOVERY:
          replicaHandler = datanode.data.recoverAppend(block, newGs, minBytesRcvd);
          block.setGenerationStamp(newGs);
          datanode.notifyNamenodeReceivingBlock(
              block, replicaHandler.getReplica().getStorageUuid());
          break;
        case TRANSFER_RBW:
        case TRANSFER_FINALIZED:
          // this is a transfer destination
          replicaHandler = datanode.data.createTemporary(storageType, storageId,
              block, isTransfer);
          break;
        default: throw new IOException("Unsupported stage " + stage + 
              " while receiving block " + block + " from " + inAddr);
        }
      }
      replicaInfo = replicaHandler.getReplica();
      this.dropCacheBehindWrites = (cachingStrategy.getDropBehind() == null) ?
        datanode.getDnConf().dropCacheBehindWrites :
          cachingStrategy.getDropBehind();
      this.syncBehindWrites = datanode.getDnConf().syncBehindWrites;
      this.syncBehindWritesInBackground = datanode.getDnConf().
          syncBehindWritesInBackground;
      
      final boolean isCreate = isDatanode || isTransfer 
          || stage == BlockConstructionStage.PIPELINE_SETUP_CREATE;
      streams = replicaInfo.createStreams(isCreate, requestedChecksum);
      assert streams != null : "null streams!";

      // read checksum meta information
      this.clientChecksum = requestedChecksum;
      this.diskChecksum = streams.getChecksum();
      this.needsChecksumTranslation = !clientChecksum.equals(diskChecksum);
      this.bytesPerChecksum = diskChecksum.getBytesPerChecksum();
      this.checksumSize = diskChecksum.getChecksumSize();

      this.checksumOut = new DataOutputStream(new BufferedOutputStream(
          streams.getChecksumOut(), DFSUtilClient.getSmallBufferSize(
          datanode.getConf())));
      // write data chunk header if creating a new replica
      if (isCreate) {
        BlockMetadataHeader.writeHeader(checksumOut, diskChecksum);
      } 
    } catch (ReplicaAlreadyExistsException | ReplicaNotFoundException
        | DiskOutOfSpaceException e) {
      throw e;
    } catch(IOException ioe) {
      if (replicaInfo != null) {
        replicaInfo.releaseAllBytesReserved();
      }
      IOUtils.closeStream(this);
      cleanupBlock();
      
      // check if there is a disk error
      IOException cause = DatanodeUtil.getCauseIfDiskError(ioe);
      DataNode.LOG
          .warn("IOException in BlockReceiver constructor :" + ioe.getMessage()
          + (cause == null ? "" : ". Cause is "), cause);
      if (cause != null) {
        ioe = cause;
        // Volume error check moved to FileIoProvider
      }
      
      throw ioe;
    }
  }

  /** Return the datanode object. */
  DataNode getDataNode() {return datanode;}

  Replica getReplica() {
    return replicaInfo;
  }

  public void releaseAnyRemainingReservedSpace() {
    if (replicaInfo != null) {
      if (replicaInfo.getReplicaInfo().getBytesReserved() > 0) {
        LOG.warn("Block {} has not released the reserved bytes. "
                + "Releasing {} bytes as part of close.", replicaInfo.getBlockId(),
            replicaInfo.getReplicaInfo().getBytesReserved());
        replicaInfo.releaseAllBytesReserved();
      }
    }
  }

  /**
   * close files and release volume reference.
   */
  @Override
  public void close() throws IOException {
    Span span = Tracer.getCurrentSpan();
    if (span != null) {
      span.addKVAnnotation("maxWriteToDiskMs",
            Long.toString(maxWriteToDiskMs));
    }
    packetReceiver.close();

    IOException ioe = null;
    if (syncOnClose && (streams.getDataOut() != null || checksumOut != null)) {
      datanode.metrics.incrFsyncCount();      
    }
    long flushTotalNanos = 0;
    boolean measuredFlushTime = false;
    // close checksum file
    try {
      if (checksumOut != null) {
        long flushStartNanos = System.nanoTime();
        checksumOut.flush();
        long flushEndNanos = System.nanoTime();
        if (syncOnClose) {
          long fsyncStartNanos = flushEndNanos;
          streams.syncChecksumOut();
          datanode.metrics.addFsyncNanos(System.nanoTime() - fsyncStartNanos);
        }
        flushTotalNanos += flushEndNanos - flushStartNanos;
        measuredFlushTime = true;
        checksumOut.close();
        checksumOut = null;
      }
    } catch(IOException e) {
      ioe = e;
    }
    finally {
      IOUtils.closeStream(checksumOut);
    }
    // close block file
    try {
      if (streams.getDataOut() != null) {
        long flushStartNanos = System.nanoTime();
        streams.flushDataOut();
        long flushEndNanos = System.nanoTime();
        if (syncOnClose) {
          long fsyncStartNanos = flushEndNanos;
          streams.syncDataOut();
          datanode.metrics.addFsyncNanos(System.nanoTime() - fsyncStartNanos);
        }
        flushTotalNanos += flushEndNanos - flushStartNanos;
        measuredFlushTime = true;
        streams.closeDataStream();
      }
    } catch (IOException e) {
      ioe = e;
    }
    finally{
      streams.close();
    }
    if (replicaHandler != null) {
      IOUtils.cleanupWithLogger(null, replicaHandler);
      replicaHandler = null;
    }
    if (measuredFlushTime) {
      datanode.metrics.addFlushNanos(flushTotalNanos);
    }
    if(ioe != null) {
      // Volume error check moved to FileIoProvider
      throw ioe;
    }
  }

  /**
   * Check if a packet was sent within an acceptable period of time.
   *
   * Some example of when this method may return false:
   * <ul>
   * <li>Upstream did not send packet for a long time</li>
   * <li>Packet was received but got stuck in local disk I/O</li>
   * <li>Packet was received but got stuck on send to mirror</li>
   * </ul>
   *
   * @return true if packet was sent within an acceptable period of time;
   *         otherwise false.
   */
  boolean packetSentInTime() {
    final long diff = Time.monotonicNow() - this.lastSentTime.get();
    final boolean allowedIdleTime = (diff <= this.maxSendIdleTime);
    LOG.debug("A packet was last sent {}ms ago.", diff);
    if (!allowedIdleTime) {
      LOG.warn("A packet was last sent {}ms ago. Maximum idle time: {}ms.",
          diff, this.maxSendIdleTime);
    }
    return allowedIdleTime;
  }

  /**
   * Flush block data and metadata files to disk.
   * @throws IOException
   */
  void flushOrSync(boolean isSync, long seqno) throws IOException {
    long flushTotalNanos = 0;
    long begin = Time.monotonicNow();
    DataNodeFaultInjector.get().delay();
    if (checksumOut != null) {
      long flushStartNanos = System.nanoTime();
      checksumOut.flush();
      long flushEndNanos = System.nanoTime();
      if (isSync) {
        streams.syncChecksumOut();
        datanode.metrics.addFsyncNanos(System.nanoTime() - flushEndNanos);
      }
      flushTotalNanos += flushEndNanos - flushStartNanos;
    }
    if (streams.getDataOut() != null) {
      long flushStartNanos = System.nanoTime();
      streams.flushDataOut();
      long flushEndNanos = System.nanoTime();
      if (isSync) {
        long fsyncStartNanos = flushEndNanos;
        streams.syncDataOut();
        datanode.metrics.addFsyncNanos(System.nanoTime() - fsyncStartNanos);
      }
      flushTotalNanos += flushEndNanos - flushStartNanos;
    }
    if (isSync && !dirSyncOnHSyncDone && replicaInfo instanceof LocalReplica) {
      ((LocalReplica) replicaInfo).fsyncDirectory();
      dirSyncOnHSyncDone = true;
    }
    if (checksumOut != null || streams.getDataOut() != null) {
      datanode.metrics.addFlushNanos(flushTotalNanos);
      if (isSync) {
        datanode.metrics.incrFsyncCount();
      }
    }
    long duration = Time.monotonicNow() - begin;
    if (duration > datanodeSlowLogThresholdMs && LOG.isWarnEnabled()) {
      datanode.metrics.incrSlowFlushOrSyncCount();
      LOG.warn("Slow flushOrSync took " + duration + "ms (threshold="
          + datanodeSlowLogThresholdMs + "ms), isSync:" + isSync + ", flushTotalNanos="
          + flushTotalNanos + "ns, volume=" + getVolumeBaseUri()
          + ", blockId=" + replicaInfo.getBlockId()
          + ", seqno=" + seqno);
    }
  }

  /**
   * While writing to mirrorOut, failure to write to mirror should not
   * affect this datanode unless it is caused by interruption.
   */
  private void handleMirrorOutError(IOException ioe) throws IOException {
    String bpid = block.getBlockPoolId();
    LOG.info(datanode.getDNRegistrationForBP(bpid)
        + ":Exception writing " + block + " to mirror " + mirrorAddr, ioe);
    if (Thread.interrupted()) { // shut down if the thread is interrupted
      throw ioe;
    } else { // encounter an error while writing to mirror
      // continue to run even if can not write to mirror
      // notify client of the error
      // and wait for the client to shut down the pipeline
      mirrorError = true;
    }
  }
  
  /**
   * Verify multiple CRC chunks. 
   */
  private void verifyChunks(ByteBuffer dataBuf, ByteBuffer checksumBuf)
      throws IOException {
    try {
      clientChecksum.verifyChunkedSums(dataBuf, checksumBuf, clientname, 0);
    } catch (ChecksumException ce) {
      PacketHeader header = packetReceiver.getHeader();
      String specificOffset = "specific offsets are:"
          + " offsetInBlock = " + header.getOffsetInBlock()
          + " offsetInPacket = " + ce.getPos();
      LOG.warn("Checksum error in block "
          + block + " from " + inAddr
          + ", " + specificOffset, ce);
      // No need to report to namenode when client is writing.
      if (srcDataNode != null && isDatanode) {
        try {
          LOG.info("report corrupt " + block + " from datanode " +
                    srcDataNode + " to namenode");
          datanode.reportRemoteBadBlock(srcDataNode, block);
        } catch (IOException e) {
          LOG.warn("Failed to report bad " + block + 
                    " from datanode " + srcDataNode + " to namenode");
        }
      }
      throw new IOException("Unexpected checksum mismatch while writing "
          + block + " from " + inAddr);
    }
  }
  
    
  /**
   * Translate CRC chunks from the client's checksum implementation
   * to the disk checksum implementation.
   * 
   * This does not verify the original checksums, under the assumption
   * that they have already been validated.
   */
  private void translateChunks(ByteBuffer dataBuf, ByteBuffer checksumBuf) {
    diskChecksum.calculateChunkedSums(dataBuf, checksumBuf);
  }

  /** 
   * Check whether checksum needs to be verified.
   * Skip verifying checksum iff this is not the last one in the 
   * pipeline and clientName is non-null. i.e. Checksum is verified
   * on all the datanodes when the data is being written by a 
   * datanode rather than a client. Whe client is writing the data, 
   * protocol includes acks and only the last datanode needs to verify 
   * checksum.
   * @return true if checksum verification is needed, otherwise false.
   */
  private boolean shouldVerifyChecksum() {
    return (mirrorOut == null || isDatanode || needsChecksumTranslation);
  }

  /**
   * 接收并处理一个数据包，数据包可能包含多个数据块
   * 
   * 该方法是BlockReceiver的核心方法之一，负责：
   * 1. 从客户端或上游DataNode接收数据包
   * 2. 验证数据包头部信息
   * 3. 将数据包镜像传输到下游DataNode（如果有）
   * 4. 验证和处理校验和
   * 5. 将数据写入磁盘
   * 6. 更新块信息和性能指标
   * 
   * @return 数据包中包含的数据字节数，如果是最后一个数据包则返回-1
   * @throws IOException 如果在数据接收或处理过程中发生错误
   */
  private int receivePacket() throws IOException {
    // 读取下一个数据包
    packetReceiver.receiveNextPacket(in);

    // 获取数据包头部和序列号信息
    PacketHeader header = packetReceiver.getHeader();
    long seqno = header.getSeqno();
    LOG.debug("接收块 {} 的数据包，序列号:{} 头部:{}", block,
        seqno, header);

    // 验证数据包头部信息的有效性
    if (header.getOffsetInBlock() > replicaInfo.getNumBytes()) {
      throw new IOException("接收到无序数据包，块: " + block + 
          "来源: " + inAddr + " 偏移量: " + header.getOffsetInBlock() +
          ". 期望从偏移量开始: " + replicaInfo.getNumBytes());
    }
    if (header.getDataLen() < 0) {
      throw new IOException("writeBlock过程中数据长度错误(" + block + 
                            ") 来源: " + inAddr + " 偏移量: " + 
                            header.getOffsetInBlock() + ": " +
                            header.getDataLen()); 
    }

    // 提取数据包关键信息
    long offsetInBlock = header.getOffsetInBlock();  // 块内偏移量
    boolean lastPacketInBlock = header.isLastPacketInBlock();  // 是否为最后一个数据包
    final int len = header.getDataLen();  // 数据长度
    boolean syncBlock = header.getSyncBlock();  // 是否需要同步块

    // 避免在关闭时重复同步
    if (syncBlock && lastPacketInBlock) {
      this.syncOnClose = false;
      // 由于请求了同步，无论syncOnClose配置如何，在完成时都同步目录
      this.dirSyncOnFinalize = true;
    }

    // 更新已接收字节数
    final long firstByteInBlock = offsetInBlock;
    offsetInBlock += len;
    if (replicaInfo.getNumBytes() < offsetInBlock) {
      replicaInfo.setNumBytes(offsetInBlock);
    }
    
    // 将数据包加入待确认队列（除非请求了同步）
    if (responder != null && !syncBlock && !shouldVerifyChecksum()) {
      ((PacketResponder) responder.getRunnable()).enqueue(seqno,
          lastPacketInBlock, offsetInBlock, Status.SUCCESS);
    }

    // 测试用：丢弃心跳包
    if (seqno < 0 && len == 0 &&
        DataNodeFaultInjector.get().dropHeartbeatPacket()) {
      return 0;
    }

    // 增加接收到的数据包计数（排除心跳包）
    if (seqno != DFSPacket.HEART_BEAT_SEQNO) {
      datanode.metrics.incrPacketsReceived();
    }
    // 第一步：将数据包镜像传输到下游节点（如果有）
    if (mirrorOut != null && !mirrorError) {
      try {
        long begin = Time.monotonicNow();
        // 测试用：停止向下游发送数据包，生产环境中通常是空操作
        DataNodeFaultInjector.get().stopSendingPacketDownstream(mirrorAddr);
        packetReceiver.mirrorPacketTo(mirrorOut);
        mirrorOut.flush();
        long now = Time.monotonicNow();
        this.lastSentTime.set(now);
        long duration = now - begin;
        DataNodeFaultInjector.get().logDelaySendingPacketDownstream(
            mirrorAddr,
            duration);
        
        // 跟踪发送数据包到管道中最后一个节点的延迟
        trackSendPacketToLastNodeInPipeline(duration);
        
        // 如果发送到下游节点时间过长，记录慢写指标
        if (duration > datanodeSlowLogThresholdMs) {
          datanode.metrics.incrPacketsSlowWriteToMirror();
          if (LOG.isWarnEnabled()) {
            LOG.warn("向镜像节点写入数据包耗时过长：{}ms " +
                "(阈值={}ms)，下游DNs={}, 块ID={}, 序列号={}",
                duration, datanodeSlowLogThresholdMs,
                Arrays.toString(downstreamDNs), replicaInfo.getBlockId(),
                seqno);
          }
        }
      } catch (IOException e) {
        // 处理镜像输出错误
        handleMirrorOutError(e);
      }
    }
    
    // 获取数据包的数据部分和校验和部分
    ByteBuffer dataBuf = packetReceiver.getDataSlice();
    ByteBuffer checksumBuf = packetReceiver.getChecksumSlice();
    
    // 处理特殊情况：最后一个数据包或空包
    if (lastPacketInBlock || len == 0) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("接收空包或块的末尾数据包，块ID: " + block);
      }
      // 如果请求了同步，则同步块
      if (syncBlock) {
        flushOrSync(true, seqno);
      }
    } else {
      // 计算期望的校验和长度
      final int checksumLen = diskChecksum.getChecksumSize(len);
      final int checksumReceivedLen = checksumBuf.capacity();

      // 验证接收到的校验和长度是否正确
      if (checksumReceivedLen > 0 && checksumReceivedLen != checksumLen) {
        throw new IOException("无效的校验和长度：接收到的长度是 "
            + checksumReceivedLen + " 但期望的长度是 " + checksumLen);
      }

      // 如果需要验证校验和
      if (checksumReceivedLen > 0 && shouldVerifyChecksum()) {
        try {
          // 验证数据块的校验和
          verifyChunks(dataBuf, checksumBuf);
        } catch (IOException ioe) {
          // 本地检测到校验和错误，无需继续处理
          if (responder != null) {
            try {
              // 向客户端发送校验和错误响应
              ((PacketResponder) responder.getRunnable()).enqueue(seqno,
                  lastPacketInBlock, offsetInBlock,
                  Status.ERROR_CHECKSUM);
              // 等待响应发送完成并中断当前线程
              Thread.sleep(3000);
            } catch (InterruptedException e) { }
          }
          throw new IOException("由于校验和错误而终止。" + ioe);
        }
 
        // 如果需要，将校验和转换为磁盘存储的格式
        if (needsChecksumTranslation) {
          translateChunks(dataBuf, checksumBuf);
        }
      }

      // 如果没有收到校验和且不是临时存储，则计算校验和
      if (checksumReceivedLen == 0 && !streams.isTransientStorage()) {
        checksumBuf = ByteBuffer.allocate(checksumLen);
        diskChecksum.calculateChunkedSums(dataBuf, checksumBuf);
      }
      
      // 到这里，缓冲区中的数据已经使用了磁盘校验和

      // 定义是否不应该写入校验和的条件（没有接收到校验和且是临时存储）
      final boolean shouldNotWriteChecksum = checksumReceivedLen == 0
          && streams.isTransientStorage();
      try {
        long onDiskLen = replicaInfo.getBytesOnDisk();
        if (onDiskLen<offsetInBlock) {
          // Normally the beginning of an incoming packet is aligned with the
          // existing data on disk. If the beginning packet data offset is not
          // checksum chunk aligned, the end of packet will not go beyond the
          // next chunk boundary.
          // When a failure-recovery is involved, the client state and the
          // the datanode state may not exactly agree. I.e. the client may
          // resend part of data that is already on disk. Correct number of
          // bytes should be skipped when writing the data and checksum
          // buffers out to disk.
          long partialChunkSizeOnDisk = onDiskLen % bytesPerChecksum;
          long lastChunkBoundary = onDiskLen - partialChunkSizeOnDisk;
          boolean alignedOnDisk = partialChunkSizeOnDisk == 0;
          boolean alignedInPacket = firstByteInBlock % bytesPerChecksum == 0;

          // If the end of the on-disk data is not chunk-aligned, the last
          // checksum needs to be overwritten.
          boolean overwriteLastCrc = !alignedOnDisk && !shouldNotWriteChecksum;
          // If the starting offset of the packat data is at the last chunk
          // boundary of the data on disk, the partial checksum recalculation
          // can be skipped and the checksum supplied by the client can be used
          // instead. This reduces disk reads and cpu load.
          boolean doCrcRecalc = overwriteLastCrc &&
              (lastChunkBoundary != firstByteInBlock);

          // If this is a partial chunk, then verify that this is the only
          // chunk in the packet. If the starting offset is not chunk
          // aligned, the packet should terminate at or before the next
          // chunk boundary.
          if (!alignedInPacket && len > bytesPerChecksum) {
            throw new IOException("Unexpected packet data length for "
                +  block + " from " + inAddr + ": a partial chunk must be "
                + " sent in an individual packet (data length = " + len
                +  " > bytesPerChecksum = " + bytesPerChecksum + ")");
          }

          // If the last portion of the block file is not a full chunk,
          // then read in pre-existing partial data chunk and recalculate
          // the checksum so that the checksum calculation can continue
          // from the right state. If the client provided the checksum for
          // the whole chunk, this is not necessary.
          Checksum partialCrc = null;
          if (doCrcRecalc) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("receivePacket for " + block 
                  + ": previous write did not end at the chunk boundary."
                  + " onDiskLen=" + onDiskLen);
            }
            long offsetInChecksum = BlockMetadataHeader.getHeaderSize() +
                onDiskLen / bytesPerChecksum * checksumSize;
            partialCrc = computePartialChunkCrc(onDiskLen, offsetInChecksum);
          }

          // The data buffer position where write will begin. If the packet
          // data and on-disk data have no overlap, this will not be at the
          // beginning of the buffer.
          int startByteToDisk = (int)(onDiskLen-firstByteInBlock) 
              + dataBuf.arrayOffset() + dataBuf.position();

          // 实际要写入磁盘的数据字节数
          int numBytesToDisk = (int)(offsetInBlock-onDiskLen);
          
          // 写入数据到磁盘
          long begin = Time.monotonicNow();
          streams.writeDataToDisk(dataBuf.array(),
              startByteToDisk, numBytesToDisk);
          // 生产环境中的空操作
          DataNodeFaultInjector.get().delayWriteToDisk();
          long duration = Time.monotonicNow() - begin;
          // 检查是否为慢写操作
          if (duration > datanodeSlowLogThresholdMs) {
            datanode.metrics.incrPacketsSlowWriteToDisk();
            if (LOG.isWarnEnabled()) {
              LOG.warn("BlockReceiver写入数据到磁盘速度慢：耗时 {}ms " +
                      "(阈值={}ms), 卷={}, 块ID={}, 序列号={}",
                  duration, datanodeSlowLogThresholdMs, getVolumeBaseUri(),
                  replicaInfo.getBlockId(), seqno);
            }
          }

          if (duration > maxWriteToDiskMs) {
            maxWriteToDiskMs = duration;
          }

          // 存储最后一个校验和的字节数组
          final byte[] lastCrc;
          // 判断是否应该写入校验和
          if (shouldNotWriteChecksum) {
            lastCrc = null;
          } else {
            int skip = 0;
            byte[] crcBytes = null;

            // First, prepare to overwrite the partial crc at the end.
            if (overwriteLastCrc) { // not chunk-aligned on disk
              // prepare to overwrite last checksum
              adjustCrcFilePosition();
            }

            // The CRC was recalculated for the last partial chunk. Update the
            // CRC by reading the rest of the chunk, then write it out.
            if (doCrcRecalc) {
              // Calculate new crc for this chunk.
              int bytesToReadForRecalc =
                  (int)(bytesPerChecksum - partialChunkSizeOnDisk);
              if (numBytesToDisk < bytesToReadForRecalc) {
                bytesToReadForRecalc = numBytesToDisk;
              }

              partialCrc.update(dataBuf.array(), startByteToDisk,
                  bytesToReadForRecalc);
              byte[] buf = FSOutputSummer.convertToByteStream(partialCrc,
                  checksumSize);
              crcBytes = copyLastChunkChecksum(buf, checksumSize, buf.length);
              checksumOut.write(buf);
              if(LOG.isDebugEnabled()) {
                LOG.debug("Writing out partial crc for data len " + len +
                    ", skip=" + skip);
              }
              skip++; //  For the partial chunk that was just read.
            }

            // Determine how many checksums need to be skipped up to the last
            // boundary. The checksum after the boundary was already counted
            // above. Only count the number of checksums skipped up to the
            // boundary here.
            long skippedDataBytes = lastChunkBoundary - firstByteInBlock;

            if (skippedDataBytes > 0) {
              skip += (int)(skippedDataBytes / bytesPerChecksum) +
                  ((skippedDataBytes % bytesPerChecksum == 0) ? 0 : 1);
            }
            skip *= checksumSize; // Convert to number of bytes

            // write the rest of checksum
            final int offset = checksumBuf.arrayOffset() +
                checksumBuf.position() + skip;
            final int end = offset + checksumLen - skip;
            // 检查是否有更多校验和需要写入
            // 如果offset >= end，表示没有更多校验和要写（已经重写了部分块校验和）
            if (offset >= end && doCrcRecalc) {
              lastCrc = crcBytes;
            } else {
              // 计算剩余需要写入的校验和字节数
              final int remainingBytes = checksumLen - skip;
              // 保存最后一个块的校验和
              lastCrc = copyLastChunkChecksum(checksumBuf.array(),
                  checksumSize, end);
              // 写入剩余的校验和
              checksumOut.write(checksumBuf.array(), offset, remainingBytes);
            }
          }

          // 刷新整个数据包，如果请求了同步则同步
          flushOrSync(syncBlock, seqno);
          
          // 更新副本信息，记录最后一个校验和和数据长度
          replicaInfo.setLastChecksumAndDataLen(offsetInBlock, lastCrc);

          // 更新写入字节数指标
          datanode.metrics.incrBytesWritten(len);
          // 更新总写入时间指标
          datanode.metrics.incrTotalWriteTime(duration);

          // 管理写入操作系统缓存
          manageWriterOsCache(offsetInBlock, seqno);
        }
      } catch (IOException iex) {
        // 卷错误检查已移至FileIoProvider
        throw iex;
      }
    }

    // 如果请求了同步，将其加入待确认队列（在fsync完成后）
    if (responder != null && (syncBlock || shouldVerifyChecksum())) {
      ((PacketResponder) responder.getRunnable()).enqueue(seqno,
          lastPacketInBlock, offsetInBlock, Status.SUCCESS);
    }

    /*
     * 为replaceBlock()调用发送进行中响应回调用方
     * 以避免因平衡器节流而导致超时。HDFS-6247
     */
    if (isReplaceBlock
        && (Time.monotonicNow() - lastResponseTime > responseInterval)) {
      BlockOpResponseProto.Builder response = BlockOpResponseProto.newBuilder()
          .setStatus(Status.IN_PROGRESS);
      response.build().writeDelimitedTo(replyOut);
      replyOut.flush();

      lastResponseTime = Time.monotonicNow();
    }

    // 如果存在节流器，则限制I/O速度
    if (throttler != null) {
      throttler.throttle(len);
    }
    
    // 返回处理结果：如果是最后一个数据包返回-1，否则返回数据长度
    return lastPacketInBlock?-1:len;
  }

  /**
   * Only tracks the latency of sending packet to the last node in pipeline.
   * This is a conscious design choice.
   * <p>
   * In the case of pipeline [dn0, dn1, dn2], 5ms latency from dn0 to dn1, 100ms
   * from dn1 to dn2, NameNode claims dn2 is slow since it sees 100ms latency to
   * dn2. Note that NameNode is not ware of pipeline structure in this context
   * and only sees latency between two DataNodes.
   * </p>
   * <p>
   * In another case of the same pipeline, 100ms latency from dn0 to dn1, 5ms
   * from dn1 to dn2, NameNode will miss detecting dn1 being slow since it's not
   * the last node. However the assumption is that in a busy enough cluster
   * there are many other pipelines where dn1 is the last node, e.g. [dn3, dn4,
   * dn1]. Also our tracking interval is relatively long enough (at least an
   * hour) to improve the chances of the bad DataNodes being the last nodes in
   * multiple pipelines.
   * </p>
   */
  private void trackSendPacketToLastNodeInPipeline(final long elapsedMs) {
    final DataNodePeerMetrics peerMetrics = datanode.getPeerMetrics();
    if (datanode.getDnConf().peerStatsEnabled && peerMetrics != null && isPenultimateNode) {
      peerMetrics.addSendPacketDownstream(mirrorNameForMetrics, elapsedMs);
    }
  }

  private static byte[] copyLastChunkChecksum(byte[] array, int size, int end) {
    return Arrays.copyOfRange(array, end - size, end);
  }

  private void manageWriterOsCache(long offsetInBlock, long seqno) {
    try {
      if (streams.getOutFd() != null &&
          offsetInBlock > lastCacheManagementOffset + CACHE_DROP_LAG_BYTES) {
        long begin = Time.monotonicNow();
        //
        // For SYNC_FILE_RANGE_WRITE, we want to sync from
        // lastCacheManagementOffset to a position "two windows ago"
        //
        //                         <========= sync ===========>
        // +-----------------------O--------------------------X
        // start                  last                      curPos
        // of file                 
        //
        if (syncBehindWrites) {
          if (syncBehindWritesInBackground) {
            this.datanode.getFSDataset().submitBackgroundSyncFileRangeRequest(
                block, streams, lastCacheManagementOffset,
                offsetInBlock - lastCacheManagementOffset,
                SYNC_FILE_RANGE_WRITE);
          } else {
            streams.syncFileRangeIfPossible(lastCacheManagementOffset,
                offsetInBlock - lastCacheManagementOffset,
                SYNC_FILE_RANGE_WRITE);
          }
        }
        //
        // For POSIX_FADV_DONTNEED, we want to drop from the beginning 
        // of the file to a position prior to the current position.
        //
        // <=== drop =====> 
        //                 <---W--->
        // +--------------+--------O--------------------------X
        // start        dropPos   last                      curPos
        // of file             
        //                     
        long dropPos = lastCacheManagementOffset - CACHE_DROP_LAG_BYTES;
        if (dropPos > 0 && dropCacheBehindWrites) {
          streams.dropCacheBehindWrites(block.getBlockName(), 0, dropPos,
              POSIX_FADV_DONTNEED);
        }
        lastCacheManagementOffset = offsetInBlock;
        // For testing. Normally no-op.
        DataNodeFaultInjector.get().delayWriteToOsCache();
        long duration = Time.monotonicNow() - begin;
        if (duration > datanodeSlowLogThresholdMs) {
          datanode.metrics.incrPacketsSlowWriteToOsCache();
          if (LOG.isWarnEnabled()) {
            LOG.warn("Slow manageWriterOsCache took {}ms " +
                    "(threshold={}ms), volume={}, blockId={}, seqno={}",
                duration, datanodeSlowLogThresholdMs, getVolumeBaseUri(),
                replicaInfo.getBlockId(), seqno);
          }
        }
      }
    } catch (Throwable t) {
      LOG.warn("Error managing cache for writer of block " + block, t);
    }
  }
  
  public void sendOOB() throws IOException, InterruptedException {
    if (isDatanode) {
      return;
    }
    ((PacketResponder) responder.getRunnable()).sendOOBResponse(PipelineAck
        .getRestartOOBStatus());
  }

  /**
   * 接收数据块并将其写入存储，同时处理向下游节点的镜像传输
   * 
   * 此方法是BlockReceiver的核心方法，负责从上游接收数据块，验证数据完整性，
   * 写入本地存储，并根据需要将数据转发到下游DataNode节点
   * 
   * @param mirrOut 到下一个DataNode的输出流
   * @param mirrIn 来自下一个DataNode的输入流
   * @param replyOut 到上一个DataNode的输出流
   * @param mirrAddr 镜像节点的地址
   * @param throttlerArg 数据传输限速器
   * @param downstreams 下游DataNode节点列表
   * @param isReplaceBlock 是否是替换块操作
   * @throws IOException 如果接收过程中发生IO异常
   */
  void receiveBlock(
      DataOutputStream mirrOut, // 到下一个DataNode的输出流
      DataInputStream mirrIn,   // 来自下一个DataNode的输入流
      DataOutputStream replyOut,  // 到上一个DataNode的输出流
      String mirrAddr, DataTransferThrottler throttlerArg,
      DatanodeInfo[] downstreams,
      boolean isReplaceBlock) throws IOException {

    // 初始化同步设置
    syncOnClose = datanode.getDnConf().syncOnClose;
    dirSyncOnFinalize = syncOnClose;
    // 用于跟踪responder是否已关闭
    boolean responderClosed = false;
    // 设置镜像节点的输出流和地址
    mirrorOut = mirrOut;
    mirrorAddr = mirrAddr;
    // 初始化性能监控
    initPerfMonitoring(downstreams);
    // 设置限速器
    throttler = throttlerArg;

    // 保存回复输出流和是否是替换块操作的标志
    this.replyOut = replyOut;
    this.isReplaceBlock = isReplaceBlock;

    try {
      // 如果是客户端请求且非传输模式，创建并启动PacketResponder线程
      // 该线程负责处理对客户端的响应
      if (isClient && !isTransfer) {
        responder = new Daemon(datanode.threadGroup, 
            new PacketResponder(replyOut, mirrIn, downstreams));
        responder.start(); // 启动线程处理响应
      }

      // 循环接收数据包，直到接收到最后一个数据包（返回-1）
      while (receivePacket() >= 0) { /* 接收直到最后一个数据包 */ }

      // 等待所有未完成的数据包响应，然后通知responder优雅关闭
      // 标记responder已关闭以便后续处理
      if (responder != null) {
        ((PacketResponder)responder.getRunnable()).close();
        responderClosed = true;
      }

      // 如果是DataNode间复制或传输-RBW/Finalized模式，最终化块或将临时块转换为RBW状态
      // 对于客户端写入，块在PacketResponder中被最终化
      if (isDatanode || isTransfer) {
        // 持有卷引用以最终化块
        try (ReplicaHandler handler = claimReplicaHandler()) {
          // 关闭块和校验和文件
          close();
          block.setNumBytes(replicaInfo.getNumBytes());

          if (stage == BlockConstructionStage.TRANSFER_RBW) {
            // 对于TRANSFER_RBW模式，将临时块转换为RBW状态
            datanode.data.convertTemporaryToRbw(block);
          } else {
            // 对于DataNode间复制或TRANSFER_FINALIZED模式，最终化块
            datanode.data.finalizeBlock(block, dirSyncOnFinalize);
          }
        }
        // 更新已写入块的计数
        datanode.metrics.incrBlocksWritten();
      }

    } catch (IOException ioe) {
      // 发生异常时释放所有保留的字节
      replicaInfo.releaseAllBytesReserved();
      if (datanode.isRestarting()) {
        // 如果正在重启，不抛出异常，否则会导致responder过早终止
        LOG.info("Shutting down for restart (" + block + ").");
      } else {
        LOG.info("Exception for " + block, ioe);
        throw ioe;
      }
    } finally {
      // 清除线程的中断状态
      Thread.interrupted();

      // 如果启动了重启关闭，需要通知上游
      // 如果responder已正常关闭，则无需特殊处理
      if (!responderClosed) { // 数据传输未完成
        if (responder != null) {
          // 如果DataNode正在快速重启，向上游发送特殊确认
          if (datanode.isRestarting() && isClient && !isTransfer) {
            try (Writer out = new OutputStreamWriter(
                replicaInfo.createRestartMetaStream(), "UTF-8")) {
              // 写入当前时间加上重启预算
              out.write(Long.toString(Time.now() + restartBudget));
              out.flush();
            } catch (IOException ioe) {
              // 最坏情况是无法恢复这个RBW副本
              // 客户端将回退到常规的管道恢复
            } finally {
              IOUtils.closeStream(streams.getDataOut());
            }
            try {              
              // 即使在确认包刷新后连接关闭，客户端也可能首先对连接关闭做出反应
              // 插入延迟以降低客户端错过OOB确认的可能性
              Thread.sleep(1000);
            } catch (InterruptedException ie) {
              // 已经在关闭过程中，忽略此异常
            }
          }
          responder.interrupt();
        }
        // 关闭当前BlockReceiver并清理块
        IOUtils.closeStream(this);
        cleanupBlock();
      }
      // 处理responder线程的清理
      if (responder != null) {
        try {
          responder.interrupt();
          // responder的join()超时应该比配置的截止时间稍早
          // 否则，此线程上的join()也可能超时
          long joinTimeout = datanode.getDnConf().getXceiverStopTimeout();
          joinTimeout = joinTimeout > 1  ? joinTimeout*8/10 : joinTimeout;
          responder.join(joinTimeout);
          // 如果responder线程仍然存活，记录警告并抛出异常
          if (responder.isAlive()) {
            String msg = "Join on responder thread " + responder
                + " timed out";
            LOG.warn(msg + "\n" + StringUtils.getStackTrace(responder));
            throw new IOException(msg);
          }
        } catch (InterruptedException e) {
          responder.interrupt();
          // 如果不是因为重启而关闭，抛出异常
          if (!datanode.isRestarting()) {
            throw new InterruptedIOException("Interrupted receiveBlock");
          }
        }
        responder = null;
      }
    }
  }

  /**
   * If we have downstream DNs and peerMetrics are enabled, then initialize
   * some state for monitoring the performance of downstream DNs.
   *
   * @param downstreams downstream DNs, or null if there are none.
   */
  private void initPerfMonitoring(DatanodeInfo[] downstreams) {
    if (downstreams != null && downstreams.length > 0) {
      downstreamDNs = downstreams;
      isPenultimateNode = (downstreams.length == 1);
      if (isPenultimateNode && datanode.getDnConf().peerStatsEnabled) {
        mirrorNameForMetrics = (downstreams[0].getInfoSecurePort() != 0 ?
            downstreams[0].getInfoSecureAddr() : downstreams[0].getInfoAddr());
        LOG.debug("Will collect peer metrics for downstream node {}",
            mirrorNameForMetrics);
      }
    }
  }

  /**
   * Fetch the base URI of the volume on which this replica resides.
   *
   * @returns Volume base URI as string if available. Else returns the
   *          the string "unavailable".
   */
  private String getVolumeBaseUri() {
    final ReplicaInfo ri = replicaInfo.getReplicaInfo();
    if (ri != null && ri.getVolume() != null) {
      return ri.getVolume().getBaseURI().toString();
    }
    return "unavailable";
  }

  /** Cleanup a partial block 
   * if this write is for a replication request (and not from a client)
   */
  private void cleanupBlock() throws IOException {
    if (isDatanode) {
      datanode.data.unfinalizeBlock(block);
    }
  }

  /**
   * Adjust the file pointer in the local meta file so that the last checksum
   * will be overwritten.
   */
  private void adjustCrcFilePosition() throws IOException {
    streams.flushDataOut();
    if (checksumOut != null) {
      checksumOut.flush();
    }

    // rollback the position of the meta file
    datanode.data.adjustCrcChannelPosition(block, streams, checksumSize);
  }

  /**
   * Convert a checksum byte array to a long
   */
  static private long checksum2long(byte[] checksum) {
    long crc = 0L;
    for(int i=0; i<checksum.length; i++) {
      crc |= (0xffL&checksum[i])<<((checksum.length-i-1)*8);
    }
    return crc;
  }

  /**
   * reads in the partial crc chunk and computes checksum
   * of pre-existing data in partial chunk.
   */
  private Checksum computePartialChunkCrc(long blkoff, long ckoff)
      throws IOException {

    // find offset of the beginning of partial chunk.
    //
    int sizePartialChunk = (int) (blkoff % bytesPerChecksum);
    blkoff = blkoff - sizePartialChunk;
    if (LOG.isDebugEnabled()) {
      LOG.debug("computePartialChunkCrc for " + block
          + ": sizePartialChunk=" + sizePartialChunk
          + ", block offset=" + blkoff
          + ", metafile offset=" + ckoff);
    }

    // create an input stream from the block file
    // and read in partial crc chunk into temporary buffer
    //
    byte[] buf = new byte[sizePartialChunk];
    byte[] crcbuf = new byte[checksumSize];
    try (ReplicaInputStreams instr =
        datanode.data.getTmpInputStreams(block, blkoff, ckoff)) {
      instr.readDataFully(buf, 0, sizePartialChunk);

      // open meta file and read in crc value computer earlier
      instr.readChecksumFully(crcbuf, 0, crcbuf.length);
    }

    // compute crc of partial chunk from data read in the block file.
    final Checksum partialCrc = DataChecksum.newDataChecksum(
        diskChecksum.getChecksumType(), diskChecksum.getBytesPerChecksum());
    partialCrc.update(buf, 0, sizePartialChunk);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Read in partial CRC chunk from disk for " + block);
    }

    // paranoia! verify that the pre-computed crc matches what we
    // recalculated just now
    if (partialCrc.getValue() != checksum2long(crcbuf)) {
      String msg = "Partial CRC " + partialCrc.getValue() +
                   " does not match value computed the " +
                   " last time file was closed " +
                   checksum2long(crcbuf);
      throw new IOException(msg);
    }
    return partialCrc;
  }

  /** The caller claims the ownership of the replica handler. */
  private ReplicaHandler claimReplicaHandler() {
    ReplicaHandler handler = replicaHandler;
    replicaHandler = null;
    return handler;
  }

  private enum PacketResponderType {
    NON_PIPELINE, LAST_IN_PIPELINE, HAS_DOWNSTREAM_IN_PIPELINE
  }

  /**
   * Processes responses from downstream datanodes in the pipeline
   * and sends back replies to the originator.
   */
  class PacketResponder implements Runnable, Closeable {
    /** queue for packets waiting for ack - synchronization using monitor lock */
    private final Queue<Packet> ackQueue = new ArrayDeque<>();
    /** the thread that spawns this responder */
    private final Thread receiverThread = Thread.currentThread();
    /** is this responder running? - synchronization using monitor lock */
    private volatile boolean running = true;
    /** input from the next downstream datanode */
    private final DataInputStream downstreamIn;
    /** output to upstream datanode/client */
    private final DataOutputStream upstreamOut;
    /** The type of this responder */
    private final PacketResponderType type;
    /** for log and error messages */
    private final String myString; 
    private boolean sending = false;

    @Override
    public String toString() {
      return myString;
    }

    PacketResponder(final DataOutputStream upstreamOut,
        final DataInputStream downstreamIn, final DatanodeInfo[] downstreams) {
      this.downstreamIn = downstreamIn;
      this.upstreamOut = upstreamOut;

      this.type = downstreams == null? PacketResponderType.NON_PIPELINE
          : downstreams.length == 0? PacketResponderType.LAST_IN_PIPELINE
              : PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE;

      final StringBuilder b = new StringBuilder(getClass().getSimpleName())
          .append(": ").append(block).append(", type=").append(type);
      if (type == PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE) {
        b.append(", downstreams=").append(downstreams.length)
            .append(":").append(Arrays.asList(downstreams));
      }
      this.myString = b.toString();
    }

    private boolean isRunning() {
      // When preparing for a restart, it should continue to run until
      // interrupted by the receiver thread.
      return running && (datanode.shouldRun || datanode.isRestarting());
    }
    
    /**
     * enqueue the seqno that is still be to acked by the downstream datanode.
     * @param seqno sequence number of the packet
     * @param lastPacketInBlock if true, this is the last packet in block
     * @param offsetInBlock offset of this packet in block
     */
    void enqueue(final long seqno, final boolean lastPacketInBlock,
        final long offsetInBlock, final Status ackStatus) {
      final Packet p = new Packet(seqno, lastPacketInBlock, offsetInBlock,
          System.nanoTime(), ackStatus);
      LOG.debug("{}: enqueue {}", this, p);
      synchronized (ackQueue) {
        if (running) {
          ackQueue.add(p);
          ackQueue.notifyAll();
        }
      }
    }

    /**
     * Send an OOB response. If all acks have been sent already for the block
     * and the responder is about to close, the delivery is not guaranteed.
     * This is because the other end can close the connection independently.
     * An OOB coming from downstream will be automatically relayed upstream
     * by the responder. This method is used only by originating datanode.
     *
     * @param ackStatus the type of ack to be sent
     */
    void sendOOBResponse(final Status ackStatus) throws IOException,
        InterruptedException {
      if (!running) {
        LOG.info("Cannot send OOB response " + ackStatus + 
            ". Responder not running.");
        return;
      }

      synchronized(this) {
        if (sending) {
          wait(datanode.getOOBTimeout(ackStatus));
          // Didn't get my turn in time. Give up.
          if (sending) {
            throw new IOException("Could not send OOB reponse in time: "
                + ackStatus);
          }
        }
        sending = true;
      }

      LOG.info("Sending an out of band ack of type " + ackStatus);
      try {
        sendAckUpstreamUnprotected(null, PipelineAck.UNKOWN_SEQNO, 0L, 0L,
            PipelineAck.combineHeader(datanode.getECN(), ackStatus,
                datanode.getSLOWByBlockPoolId(block.getBlockPoolId())));
      } finally {
        // Let others send ack. Unless there are miltiple OOB send
        // calls, there can be only one waiter, the responder thread.
        // In any case, only one needs to be notified.
        synchronized(this) {
          sending = false;
          notify();
        }
      }
    }
    
    /** Wait for a packet with given {@code seqno} to be enqueued to ackQueue */
    Packet waitForAckHead(long seqno) throws InterruptedException {
      synchronized (ackQueue) {
        while (isRunning() && ackQueue.isEmpty()) {
          LOG.debug("{}: seqno={} waiting for local datanode to finish write.",
              myString, seqno);
          ackQueue.wait();
        }
        return isRunning() ? ackQueue.element() : null;
      }
    }

    /**
     * wait for all pending packets to be acked. Then shutdown thread.
     */
    @Override
    public void close() {
      synchronized (ackQueue) {
        while (isRunning() && !ackQueue.isEmpty()) {
          try {
            ackQueue.wait();
          } catch (InterruptedException e) {
            running = false;
            Thread.currentThread().interrupt();
          }
        }
        LOG.debug("{}: closing", this);
        running = false;
        ackQueue.notifyAll();
      }

      synchronized (this) {
        running = false;
        notifyAll();
      }
    }

    /**
     * Thread to process incoming acks.
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
      datanode.metrics.incrDataNodePacketResponderCount();
      boolean lastPacketInBlock = false;
      final long startTime = CLIENT_TRACE_LOG.isInfoEnabled() ? System.nanoTime() : 0;
      while (isRunning() && !lastPacketInBlock) {
        long totalAckTimeNanos = 0;
        boolean isInterrupted = false;
        try {
          Packet pkt = null;
          long expected = -2;
          PipelineAck ack = new PipelineAck();
          long seqno = PipelineAck.UNKOWN_SEQNO;
          long ackRecvNanoTime = 0;
          try {
            if (type != PacketResponderType.LAST_IN_PIPELINE && !mirrorError) {
              DataNodeFaultInjector.get().failPipeline(replicaInfo, mirrorAddr);
              // read an ack from downstream datanode
              ack.readFields(downstreamIn);
              ackRecvNanoTime = System.nanoTime();
              if (LOG.isDebugEnabled()) {
                LOG.debug(myString + " got " + ack);
              }
              // Process an OOB ACK.
              Status oobStatus = ack.getOOBStatus();
              if (oobStatus != null) {
                LOG.info("Relaying an out of band ack of type " + oobStatus);
                sendAckUpstream(ack, PipelineAck.UNKOWN_SEQNO, 0L, 0L,
                    PipelineAck.combineHeader(datanode.getECN(),
                      Status.SUCCESS,
                      datanode.getSLOWByBlockPoolId(block.getBlockPoolId())));
                continue;
              }
              seqno = ack.getSeqno();
            }
            if (seqno != PipelineAck.UNKOWN_SEQNO
                || type == PacketResponderType.LAST_IN_PIPELINE) {
              pkt = waitForAckHead(seqno);
              if (!isRunning()) {
                break;
              }
              expected = pkt.seqno;
              if (type == PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE
                  && seqno != expected) {
                throw new IOException(myString + "seqno: expected=" + expected
                    + ", received=" + seqno);
              }
              if (type == PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE) {
                // The total ack time includes the ack times of downstream
                // nodes.
                // The value is 0 if this responder doesn't have a downstream
                // DN in the pipeline.
                totalAckTimeNanos = ackRecvNanoTime - pkt.ackEnqueueNanoTime;
                // Report the elapsed time from ack send to ack receive minus
                // the downstream ack time.
                long ackTimeNanos = totalAckTimeNanos
                    - ack.getDownstreamAckTimeNanos();
                if (ackTimeNanos < 0) {
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Calculated invalid ack time: " + ackTimeNanos
                        + "ns.");
                  }
                } else {
                  datanode.metrics.addPacketAckRoundTripTimeNanos(ackTimeNanos);
                }
              }
              lastPacketInBlock = pkt.lastPacketInBlock;
            }
          } catch (InterruptedException ine) {
            isInterrupted = true;
          } catch (IOException ioe) {
            if (Thread.interrupted()) {
              isInterrupted = true;
            } else if (ioe instanceof EOFException && !packetSentInTime()) {
              // The downstream error was caused by upstream including this
              // node not sending packet in time. Let the upstream determine
              // who is at fault.  If the immediate upstream node thinks it
              // has sent a packet in time, this node will be reported as bad.
              // Otherwise, the upstream node will propagate the error up by
              // closing the connection.
              LOG.warn("The downstream error might be due to congestion in " +
                  "upstream including this node. Propagating the error: ",
                  ioe);
              throw ioe;
            } else {
              // continue to run even if can not read from mirror
              // notify client of the error
              // and wait for the client to shut down the pipeline
              mirrorError = true;
              LOG.info(myString, ioe);
            }
          }

          if (Thread.interrupted() || isInterrupted) {
            /*
             * The receiver thread cancelled this thread. We could also check
             * any other status updates from the receiver thread (e.g. if it is
             * ok to write to replyOut). It is prudent to not send any more
             * status back to the client because this datanode has a problem.
             * The upstream datanode will detect that this datanode is bad, and
             * rightly so.
             *
             * The receiver thread can also interrupt this thread for sending
             * an out-of-band response upstream.
             */
            LOG.info(myString + ": Thread is interrupted.");
            running = false;
            continue;
          }

          if (lastPacketInBlock) {
            // Finalize the block and close the block file
            finalizeBlock(startTime);
            // For test only, no-op in production system.
            DataNodeFaultInjector.get().delayAckLastPacket();
          }

          Status myStatus = pkt != null ? pkt.ackStatus : Status.SUCCESS;
          sendAckUpstream(ack, expected, totalAckTimeNanos,
            (pkt != null ? pkt.offsetInBlock : 0),
              PipelineAck.combineHeader(datanode.getECN(), myStatus,
                  datanode.getSLOWByBlockPoolId(block.getBlockPoolId())));
          if (pkt != null) {
            // remove the packet from the ack queue
            removeAckHead();
          }
        } catch (IOException e) {
          LOG.warn("IOException in PacketResponder.run(): ", e);
          if (running) {
            // Volume error check moved to FileIoProvider
            LOG.info(myString, e);
            running = false;
            if (!Thread.interrupted()) { // failure not caused by interruption
              receiverThread.interrupt();
            }
          }
        } catch (Throwable e) {
          if (running) {
            LOG.info(myString, e);
            running = false;
            receiverThread.interrupt();
          }
        }
      }
      // Any exception will be caught and processed in the previous loop, so we
      // will always arrive here when the thread exiting
      datanode.metrics.decrDataNodePacketResponderCount();
      LOG.info(myString + " terminating");
    }
    
    /**
     * Finalize the block and close the block file
     * @param startTime time when BlockReceiver started receiving the block
     */
    private void finalizeBlock(long startTime) throws IOException {
      long endTime = 0;
      // Hold a volume reference to finalize block.
      try (ReplicaHandler handler = BlockReceiver.this.claimReplicaHandler()) {
        BlockReceiver.this.close();
        endTime = CLIENT_TRACE_LOG.isInfoEnabled() ? System.nanoTime() : 0;
        block.setNumBytes(replicaInfo.getNumBytes());
        datanode.data.finalizeBlock(block, dirSyncOnFinalize);
      }

      if (pinning) {
        datanode.data.setPinning(block);
      }
      
      datanode.closeBlock(block, null, replicaInfo.getStorageUuid(),
          replicaInfo.isOnTransientStorage());
      if (CLIENT_TRACE_LOG.isInfoEnabled() && isClient) {
        long offset = 0;
        DatanodeRegistration dnR = datanode.getDNRegistrationForBP(block
            .getBlockPoolId());
        CLIENT_TRACE_LOG.info(String.format(DN_CLIENTTRACE_FORMAT, inAddr,
            myAddr, replicaInfo.getVolume(), block.getNumBytes(),
            "HDFS_WRITE", clientname, offset, dnR.getDatanodeUuid(),
            block, endTime - startTime));
      } else {
        LOG.info("Received " + block + " on volume "  + replicaInfo.getVolume()
            + " size " + block.getNumBytes() + " from " + inAddr);
      }
    }
    
    /**
     * The wrapper for the unprotected version. This is only called by
     * the responder's run() method.
     *
     * @param ack Ack received from downstream
     * @param seqno sequence number of ack to be sent upstream
     * @param totalAckTimeNanos total ack time including all the downstream
     *          nodes
     * @param offsetInBlock offset in block for the data in packet
     * @param myHeader the local ack header
     */
    private void sendAckUpstream(PipelineAck ack, long seqno,
        long totalAckTimeNanos, long offsetInBlock,
        int myHeader) throws IOException {
      try {
        // Wait for other sender to finish. Unless there is an OOB being sent,
        // the responder won't have to wait.
        synchronized(this) {
          while(sending) {
            wait();
          }
          sending = true;
        }

        try {
          if (!running) return;
          sendAckUpstreamUnprotected(ack, seqno, totalAckTimeNanos,
              offsetInBlock, myHeader);
        } finally {
          synchronized(this) {
            sending = false;
            notify();
          }
        }
      } catch (InterruptedException ie) {
        // The responder was interrupted. Make it go down without
        // interrupting the receiver(writer) thread.  
        running = false;
      }
    }

    /**
     * @param ack Ack received from downstream
     * @param seqno sequence number of ack to be sent upstream
     * @param totalAckTimeNanos total ack time including all the downstream
     *          nodes
     * @param offsetInBlock offset in block for the data in packet
     * @param myHeader the local ack header
     */
    private void sendAckUpstreamUnprotected(PipelineAck ack, long seqno,
        long totalAckTimeNanos, long offsetInBlock, int myHeader)
        throws IOException {
      final int[] replies;
      if (ack == null) {
        // A new OOB response is being sent from this node. Regardless of
        // downstream nodes, reply should contain one reply.
        replies = new int[] { myHeader };
      } else if (mirrorError) { // ack read error
        int h = PipelineAck.combineHeader(datanode.getECN(), Status.SUCCESS,
            datanode.getSLOWByBlockPoolId(block.getBlockPoolId()));
        int h1 = PipelineAck.combineHeader(datanode.getECN(), Status.ERROR,
            datanode.getSLOWByBlockPoolId(block.getBlockPoolId()));
        replies = new int[] {h, h1};
      } else {
        short ackLen = type == PacketResponderType.LAST_IN_PIPELINE ? 0 : ack
            .getNumOfReplies();
        replies = new int[ackLen + 1];
        replies[0] = myHeader;
        for (int i = 0; i < ackLen; ++i) {
          replies[i + 1] = ack.getHeaderFlag(i);
        }
        DataNodeFaultInjector.get().markSlow(mirrorAddr, replies);
        // If the mirror has reported that it received a corrupt packet,
        // do self-destruct to mark myself bad, instead of making the
        // mirror node bad. The mirror is guaranteed to be good without
        // corrupt data on disk.
        if (ackLen > 0 && PipelineAck.getStatusFromHeader(replies[1]) ==
          Status.ERROR_CHECKSUM) {
          throw new IOException("Shutting down writer and responder "
              + "since the down streams reported the data sent by this "
              + "thread is corrupt");
        }
      }
      PipelineAck replyAck = new PipelineAck(seqno, replies,
          totalAckTimeNanos);
      if (replyAck.isSuccess()
          && offsetInBlock > replicaInfo.getBytesAcked()) {
        replicaInfo.setBytesAcked(offsetInBlock);
      }
      // send my ack back to upstream datanode
      long begin = Time.monotonicNow();
      DataNodeFaultInjector.get().delay();
      /* for test only, no-op in production system */
      DataNodeFaultInjector.get().delaySendingAckToUpstream(inAddr);
      replyAck.write(upstreamOut);
      upstreamOut.flush();
      long duration = Time.monotonicNow() - begin;
      DataNodeFaultInjector.get().logDelaySendingAckToUpstream(
          inAddr,
          duration);
      if (duration > datanodeSlowLogThresholdMs) {
        datanode.metrics.incrSlowAckToUpstreamCount();
        LOG.warn("Slow PacketResponder send ack to upstream took " + duration
            + "ms (threshold=" + datanodeSlowLogThresholdMs + "ms), " + myString
            + ", replyAck=" + replyAck
            + ", downstream DNs=" + Arrays.toString(downstreamDNs)
            + ", blockId=" + replicaInfo.getBlockId()
            + ", seqno=" + seqno);
      } else if (LOG.isDebugEnabled()) {
        LOG.debug(myString + ", replyAck=" + replyAck);
      }

      // If a corruption was detected in the received data, terminate after
      // sending ERROR_CHECKSUM back.
      Status myStatus = PipelineAck.getStatusFromHeader(myHeader);
      if (myStatus == Status.ERROR_CHECKSUM) {
        throw new IOException("Shutting down writer and responder "
            + "due to a checksum error in received data. The error "
            + "response has been sent upstream.");
      }
    }
    
    /**
     * Remove a packet from the head of the ack queue
     *
     * This should be called only when the ack queue is not empty
     */
    private void removeAckHead() {
      synchronized (ackQueue) {
        ackQueue.remove();
        ackQueue.notifyAll();
      }
    }
  }

  /**
   * This information is cached by the Datanode in the ackQueue.
   */
  private static class Packet {
    final long seqno;
    final boolean lastPacketInBlock;
    final long offsetInBlock;
    final long ackEnqueueNanoTime;
    final Status ackStatus;

    Packet(long seqno, boolean lastPacketInBlock, long offsetInBlock,
        long ackEnqueueNanoTime, Status ackStatus) {
      this.seqno = seqno;
      this.lastPacketInBlock = lastPacketInBlock;
      this.offsetInBlock = offsetInBlock;
      this.ackEnqueueNanoTime = ackEnqueueNanoTime;
      this.ackStatus = ackStatus;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(seqno=" + seqno
        + ", lastPacketInBlock=" + lastPacketInBlock
        + ", offsetInBlock=" + offsetInBlock
        + ", ackEnqueueNanoTime=" + ackEnqueueNanoTime
        + ", ackStatus=" + ackStatus
        + ")";
    }
  }
}
