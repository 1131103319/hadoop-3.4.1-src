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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FsTracer;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.DataNodeLockManager.LockLevel;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ReadaheadPool.ReadaheadRequest;
import org.apache.hadoop.net.SocketOutputStream;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.tracing.TraceScope;

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_DONTNEED;
import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_SEQUENTIAL;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;

/**
 * Reads a block from the disk and sends it to a recipient.
 * 
 * Data sent from the BlockeSender in the following format:
 * <br><b>Data format:</b> <pre>
 *    +--------------------------------------------------+
 *    | ChecksumHeader | Sequence of data PACKETS...     |
 *    +--------------------------------------------------+ 
 * </pre>   
 * <b>ChecksumHeader format:</b> <pre>
 *    +--------------------------------------------------+
 *    | 1 byte CHECKSUM_TYPE | 4 byte BYTES_PER_CHECKSUM |
 *    +--------------------------------------------------+ 
 * </pre>   
 * An empty packet is sent to mark the end of block and read completion.
 * 
 * PACKET Contains a packet header, checksum and data. Amount of data
 * carried is set by BUFFER_SIZE.
 * <pre>
 *   +-----------------------------------------------------+
 *   | Variable length header. See {@link PacketHeader}    |
 *   +-----------------------------------------------------+
 *   | x byte checksum data. x is defined below            |
 *   +-----------------------------------------------------+
 *   | actual data ......                                  |
 *   +-----------------------------------------------------+
 * 
 *   Data is made of Chunks. Each chunk is of length <= BYTES_PER_CHECKSUM.
 *   A checksum is calculated for each chunk.
 *  
 *   x = (length of data + BYTE_PER_CHECKSUM - 1)/BYTES_PER_CHECKSUM *
 *       CHECKSUM_SIZE
 *  
 *   CHECKSUM_SIZE depends on CHECKSUM_TYPE (usually, 4 for CRC32) 
 *  </pre>
 *  
 *  The client reads data until it receives a packet with 
 *  "LastPacketInBlock" set to true or with a zero length. If there is 
 *  no checksum error, it replies to DataNode with OP_STATUS_CHECKSUM_OK.
 */
class BlockSender implements java.io.Closeable {
  static final Logger LOG = DataNode.LOG;
  static final Logger CLIENT_TRACE_LOG = DataNode.CLIENT_TRACE_LOG;
  private static final boolean is32Bit = 
      System.getProperty("sun.arch.data.model").equals("32");
  /**
   * Minimum buffer used while sending data to clients. Used only if
   * transferTo() is enabled. 64KB is not that large. It could be larger, but
   * not sure if there will be much more improvement.
   */
  private static final int MIN_BUFFER_WITH_TRANSFERTO = 64*1024;
  private static final int IO_FILE_BUFFER_SIZE;
  static {
    HdfsConfiguration conf = new HdfsConfiguration();
    IO_FILE_BUFFER_SIZE = DFSUtilClient.getIoFileBufferSize(conf);
  }
  private static final int TRANSFERTO_BUFFER_SIZE = Math.max(
      IO_FILE_BUFFER_SIZE, MIN_BUFFER_WITH_TRANSFERTO);
  
  /** the block to read from */
  private final ExtendedBlock block;

  /** InputStreams and file descriptors to read block/checksum. */
  private ReplicaInputStreams ris;
  /** updated while using transferTo() */
  private long blockInPosition = -1;
  /** Checksum utility */
  private final DataChecksum checksum;
  /** Initial position to read */
  private long initialOffset;
  /** Current position of read */
  private long offset;
  /** Position of last byte to read from block file */
  private final long endOffset;
  /** Number of bytes in chunk used for computing checksum */
  private final int chunkSize;
  /** Number bytes of checksum computed for a chunk */
  private final int checksumSize;
  /** If true, failure to read checksum is ignored */
  private final boolean corruptChecksumOk;
  /** Sequence number of packet being sent */
  private long seqno;
  /** Set to true if transferTo is allowed for sending data to the client */
  private final boolean transferToAllowed;
  /** Set to true once entire requested byte range has been sent to the client */
  private boolean sentEntireByteRange;
  /** When true, verify checksum while reading from checksum file */
  private final boolean verifyChecksum;
  /** Format used to print client trace log messages */
  private final String clientTraceFmt;
  private volatile ChunkChecksum lastChunkChecksum = null;
  private DataNode datanode;

  /** The replica of the block that is being read. */
  private final Replica replica;

  // Cache-management related fields
  private final long readaheadLength;

  private ReadaheadRequest curReadahead;

  private final boolean alwaysReadahead;
  
  private final boolean dropCacheBehindLargeReads;
  
  private final boolean dropCacheBehindAllReads;
  
  private long lastCacheDropOffset;
  private final FileIoProvider fileIoProvider;
  
  @VisibleForTesting
  static long CACHE_DROP_INTERVAL_BYTES = 1024 * 1024; // 1MB
  
  /**
   * See {{@link BlockSender#isLongRead()}
   */
  private static final long LONG_READ_THRESHOLD_BYTES = 256 * 1024;

  // The number of bytes per checksum here determines the alignment
  // of reads: we always start reading at a checksum chunk boundary,
  // even if the checksum type is NULL. So, choosing too big of a value
  // would risk sending too much unnecessary data. 512 (1 disk sector)
  // is likely to result in minimal extra IO.
  private static final long CHUNK_SIZE = 512;

  private static final String EIO_ERROR = "Input/output error";
  /**
   * Constructor
   * 
   * @param block Block that is being read
   * @param startOffset starting offset to read from
   * @param length length of data to read
   * @param corruptChecksumOk if true, corrupt checksum is okay
   * @param verifyChecksum verify checksum while reading the data
   * @param sendChecksum send checksum to client.
   * @param datanode datanode from which the block is being read
   * @param clientTraceFmt format string used to print client trace logs
   * @throws IOException
   */
  BlockSender(ExtendedBlock block, long startOffset, long length,
              boolean corruptChecksumOk, boolean verifyChecksum,
              boolean sendChecksum, DataNode datanode, String clientTraceFmt,
              CachingStrategy cachingStrategy)
      throws IOException {
    InputStream blockIn = null;
    DataInputStream checksumIn = null;
    FsVolumeReference volumeRef = null;
    this.fileIoProvider = datanode.getFileIoProvider();
    try {
      this.block = block;
      this.corruptChecksumOk = corruptChecksumOk;
      this.verifyChecksum = verifyChecksum;
      this.clientTraceFmt = clientTraceFmt;

      /*
       * If the client asked for the cache to be dropped behind all reads,
       * we honor that.  Otherwise, we use the DataNode defaults.
       * When using DataNode defaults, we use a heuristic where we only
       * drop the cache for large reads.
       */
      if (cachingStrategy.getDropBehind() == null) {
        this.dropCacheBehindAllReads = false;
        this.dropCacheBehindLargeReads =
            datanode.getDnConf().dropCacheBehindReads;
      } else {
        this.dropCacheBehindAllReads =
            this.dropCacheBehindLargeReads =
                 cachingStrategy.getDropBehind().booleanValue();
      }
      /*
       * Similarly, if readahead was explicitly requested, we always do it.
       * Otherwise, we read ahead based on the DataNode settings, and only
       * when the reads are large.
       */
      if (cachingStrategy.getReadahead() == null) {
        this.alwaysReadahead = false;
        this.readaheadLength = datanode.getDnConf().readaheadLength;
      } else {
        this.alwaysReadahead = true;
        this.readaheadLength = cachingStrategy.getReadahead().longValue();
      }
      this.datanode = datanode;
      
      if (verifyChecksum) {
        // To simplify implementation, callers may not specify verification
        // without sending.
        Preconditions.checkArgument(sendChecksum,
            "If verifying checksum, currently must also send it.");
      }

      // if there is a append write happening right after the BlockSender
      // is constructed, the last partial checksum maybe overwritten by the
      // append, the BlockSender need to use the partial checksum before
      // the append write.
      ChunkChecksum chunkChecksum = null;
      final long replicaVisibleLength;
      try (AutoCloseableLock lock = datanode.getDataSetLockManager().readLock(
          LockLevel.BLOCK_POOl, block.getBlockPoolId())) {
        replica = getReplica(block, datanode);
        replicaVisibleLength = replica.getVisibleLength();
      }
      if (replica.getState() == ReplicaState.RBW) {
        final ReplicaInPipeline rbw = (ReplicaInPipeline) replica;
        rbw.waitForMinLength(startOffset + length, 3, TimeUnit.SECONDS);
        chunkChecksum = rbw.getLastChecksumAndDataLen();
      }
      if (replica instanceof FinalizedReplica) {
        chunkChecksum = getPartialChunkChecksumForFinalized(
            (FinalizedReplica)replica);
      }

      if (replica.getGenerationStamp() < block.getGenerationStamp()) {
        throw new IOException("Replica gen stamp < block genstamp, block="
            + block + ", replica=" + replica);
      } else if (replica.getGenerationStamp() > block.getGenerationStamp()) {
        if (DataNode.LOG.isDebugEnabled()) {
          DataNode.LOG.debug("Bumping up the client provided"
              + " block's genstamp to latest " + replica.getGenerationStamp()
              + " for block " + block);
        }
        block.setGenerationStamp(replica.getGenerationStamp());
      }
      if (replicaVisibleLength < 0) {
        throw new IOException("Replica is not readable, block="
            + block + ", replica=" + replica);
      }
      if (DataNode.LOG.isDebugEnabled()) {
        DataNode.LOG.debug("block=" + block + ", replica=" + replica);
      }

      // transferToFully() fails on 32 bit platforms for block sizes >= 2GB,
      // use normal transfer in those cases
      this.transferToAllowed = datanode.getDnConf().transferToAllowed &&
        (!is32Bit || length <= Integer.MAX_VALUE);

      // Obtain a reference before reading data
      FsVolumeSpi volume = datanode.data.getVolume(block);
      if (volume == null) {
        LOG.warn("Cannot find FsVolumeSpi to obtain a reference for block: {}", block);
        throw new ReplicaNotFoundException(block);
      }
      volumeRef = volume.obtainReference();

      /* 
       * (corruptChecksumOK, meta_file_exist): operation
       * True,   True: will verify checksum  
       * True,  False: No verify, e.g., need to read data from a corrupted file 
       * False,  True: will verify checksum
       * False, False: throws IOException file not found
       */
      DataChecksum csum = null;
      if (verifyChecksum || sendChecksum) {
        LengthInputStream metaIn = null;
        boolean keepMetaInOpen = false;
        try {
          DataNodeFaultInjector.get().throwTooManyOpenFiles();
          metaIn = datanode.data.getMetaDataInputStream(block);
          if (!corruptChecksumOk || metaIn != null) {
            if (metaIn == null) {
              //need checksum but meta-data not found
              throw new FileNotFoundException("Meta-data not found for " +
                  block);
            }

            // The meta file will contain only the header if the NULL checksum
            // type was used, or if the replica was written to transient storage.
            // Also, when only header portion of a data packet was transferred
            // and then pipeline breaks, the meta file can contain only the
            // header and 0 byte in the block data file.
            // Checksum verification is not performed for replicas on transient
            // storage.  The header is important for determining the checksum
            // type later when lazy persistence copies the block to non-transient
            // storage and computes the checksum.
            int expectedHeaderSize = BlockMetadataHeader.getHeaderSize();
            if (!replica.isOnTransientStorage() &&
                metaIn.getLength() >= expectedHeaderSize) {
              checksumIn = new DataInputStream(new BufferedInputStream(
                  metaIn, IO_FILE_BUFFER_SIZE));

              csum = BlockMetadataHeader.readDataChecksum(checksumIn, block);
              keepMetaInOpen = true;
            } else if (!replica.isOnTransientStorage() &&
                metaIn.getLength() < expectedHeaderSize) {
              LOG.warn("The meta file length {} is less than the expected " +
                  "header length {}, indicating the meta file is corrupt",
                  metaIn.getLength(), expectedHeaderSize);
              throw new CorruptMetaHeaderException("The meta file length "+
                  metaIn.getLength()+" is less than the expected length "+
                  expectedHeaderSize);
            }
          } else {
            LOG.warn("Could not find metadata file for " + block);
          }
        } catch (FileNotFoundException e) {
          if ((e.getMessage() != null) && !(e.getMessage()
              .contains("Too many open files"))) {
            datanode.data.invalidateMissingBlock(block.getBlockPoolId(),
                block.getLocalBlock());
          }
          throw e;
        } finally {
          if (!keepMetaInOpen) {
            IOUtils.closeStream(metaIn);
          }
        }
      }
      if (csum == null) {
        csum = DataChecksum.newDataChecksum(DataChecksum.Type.NULL,
            (int)CHUNK_SIZE);
      }

      /*
       * If chunkSize is very large, then the metadata file is mostly
       * corrupted. For now just truncate bytesPerchecksum to blockLength.
       */       
      int size = csum.getBytesPerChecksum();
      if (size > 10*1024*1024 && size > replicaVisibleLength) {
        csum = DataChecksum.newDataChecksum(csum.getChecksumType(),
            Math.max((int)replicaVisibleLength, 10*1024*1024));
        size = csum.getBytesPerChecksum();        
      }
      chunkSize = size;
      checksum = csum;
      checksumSize = checksum.getChecksumSize();
      length = length < 0 ? replicaVisibleLength : length;

      // end is either last byte on disk or the length for which we have a 
      // checksum
      long end = chunkChecksum != null ? chunkChecksum.getDataLength()
          : replica.getBytesOnDisk();
      if (startOffset < 0 || startOffset > end
          || (length + startOffset) > end) {
        String msg = " Offset " + startOffset + " and length " + length
        + " don't match block " + block + " ( blockLen " + end + " )";
        LOG.warn(datanode.getDNRegistrationForBP(block.getBlockPoolId()) +
            ":sendBlock() : " + msg);
        throw new IOException(msg);
      }
      
      // Ensure read offset is position at the beginning of chunk
      offset = startOffset - (startOffset % chunkSize);
      if (length >= 0) {
        // Ensure endOffset points to end of chunk.
        long tmpLen = startOffset + length;
        if (tmpLen % chunkSize != 0) {
          tmpLen += (chunkSize - tmpLen % chunkSize);
        }
        if (tmpLen < end) {
          // will use on-disk checksum here since the end is a stable chunk
          end = tmpLen;
        } else if (chunkChecksum != null) {
          // last chunk is changing. flag that we need to use in-memory checksum 
          this.lastChunkChecksum = chunkChecksum;
        }
      }
      endOffset = end;

      // seek to the right offsets
      if (offset > 0 && checksumIn != null) {
        long checksumSkip = (offset / chunkSize) * checksumSize;
        // note blockInStream is seeked when created below
        if (checksumSkip > 0) {
          // Should we use seek() for checksum file as well?
          IOUtils.skipFully(checksumIn, checksumSkip);
        }
      }
      seqno = 0;

      if (DataNode.LOG.isDebugEnabled()) {
        DataNode.LOG.debug("replica=" + replica);
      }
      blockIn = datanode.data.getBlockInputStream(block, offset); // seek to offset
      ris = new ReplicaInputStreams(
          blockIn, checksumIn, volumeRef, fileIoProvider);
    } catch (IOException ioe) {
      IOUtils.cleanupWithLogger(null, volumeRef);
      IOUtils.closeStream(this);
      IOUtils.closeStream(blockIn);
      IOUtils.closeStream(checksumIn);
      throw ioe;
    }
  }

  private ChunkChecksum getPartialChunkChecksumForFinalized(
      FinalizedReplica finalized) throws IOException {
    // There are a number of places in the code base where a finalized replica
    // object is created. If last partial checksum is loaded whenever a
    // finalized replica is created, it would increase latency in DataNode
    // initialization. Therefore, the last partial chunk checksum is loaded
    // lazily.

    // Load last checksum in case the replica is being written concurrently
    final long replicaVisibleLength = replica.getVisibleLength();
    if (replicaVisibleLength % CHUNK_SIZE != 0 &&
        finalized.getLastPartialChunkChecksum() == null) {
      // the finalized replica does not have precomputed last partial
      // chunk checksum. Recompute now.
      try {
        finalized.loadLastPartialChunkChecksum();
        return new ChunkChecksum(finalized.getVisibleLength(),
            finalized.getLastPartialChunkChecksum());
      } catch (FileNotFoundException e) {
        // meta file is lost. Continue anyway to preserve existing behavior.
        DataNode.LOG.warn(
            "meta file " + finalized.getMetaFile() + " is missing!");
        return null;
      }
    } else {
      // If the checksum is null, BlockSender will use on-disk checksum.
      return new ChunkChecksum(finalized.getVisibleLength(),
          finalized.getLastPartialChunkChecksum());
    }
  }

  /**
   * close opened files.
   */
  @Override
  public void close() throws IOException {
    if (ris.getDataInFd() != null &&
        ((dropCacheBehindAllReads) ||
         (dropCacheBehindLargeReads && isLongRead()))) {
      try {
        ris.dropCacheBehindReads(block.getBlockName(), lastCacheDropOffset,
            offset - lastCacheDropOffset, POSIX_FADV_DONTNEED);
      } catch (Exception e) {
        LOG.warn("Unable to drop cache on file close", e);
      }
    }
    if (curReadahead != null) {
      curReadahead.cancel();
    }

    try {
      ris.closeStreams();
    } finally {
      IOUtils.closeStream(ris);
      ris = null;
    }
  }
  
  private static Replica getReplica(ExtendedBlock block, DataNode datanode)
      throws ReplicaNotFoundException {
    Replica replica = datanode.data.getReplica(block.getBlockPoolId(),
        block.getBlockId());
    if (replica == null) {
      throw new ReplicaNotFoundException(block);
    }
    return replica;
  }

  /**
   * Converts an IOExcpetion (not subclasses) to SocketException.
   * This is typically done to indicate to upper layers that the error 
   * was a socket error rather than often more serious exceptions like 
   * disk errors.
   */
  private static IOException ioeToSocketException(IOException ioe) {
    if (ioe.getClass().equals(IOException.class)) {
      // "se" could be a new class in stead of SocketException.
      IOException se = new SocketException("Original Exception : " + ioe);
      se.initCause(ioe);
      /* Change the stacktrace so that original trace is not truncated
       * when printed.*/ 
      se.setStackTrace(ioe.getStackTrace());
      return se;
    }
    // otherwise just return the same exception.
    return ioe;
  }

  /**
   * @param datalen Length of data 
   * @return number of chunks for data of given size
   */
  private int numberOfChunks(long datalen) {
    return (int) ((datalen + chunkSize - 1)/chunkSize);
  }
  
  /**
   * Sends a packet with up to maxChunks chunks of data.
   * 
   * @param pkt buffer used for writing packet data
   * @param maxChunks maximum number of chunks to send
   * @param out stream to send data to
   * @param transferTo use transferTo to send data
   * @param throttler used for throttling data transfer bandwidth
   */
  private int sendPacket(ByteBuffer pkt, int maxChunks, OutputStream out,
      boolean transferTo, DataTransferThrottler throttler) throws IOException {
    int dataLen = (int) Math.min(endOffset - offset,
                             (chunkSize * (long) maxChunks));
    
    int numChunks = numberOfChunks(dataLen); // Number of chunks be sent in the packet
    int checksumDataLen = numChunks * checksumSize;
    int packetLen = dataLen + checksumDataLen + 4;
    boolean lastDataPacket = offset + dataLen == endOffset && dataLen > 0;

    // The packet buffer is organized as follows:
    // _______HHHHCCCCD?D?D?D?
    //        ^   ^
    //        |   \ checksumOff
    //        \ headerOff
    // _ padding, since the header is variable-length
    // H = header and length prefixes
    // C = checksums
    // D? = data, if transferTo is false.
    
    int headerLen = writePacketHeader(pkt, dataLen, packetLen);
    
    // Per above, the header doesn't start at the beginning of the
    // buffer
    int headerOff = pkt.position() - headerLen;
    
    int checksumOff = pkt.position();
    byte[] buf = pkt.array();
    
    if (checksumSize > 0 && ris.getChecksumIn() != null) {
      readChecksum(buf, checksumOff, checksumDataLen);

      // write in progress that we need to use to get last checksum
      if (lastDataPacket && lastChunkChecksum != null) {
        int start = checksumOff + checksumDataLen - checksumSize;
        byte[] updatedChecksum = lastChunkChecksum.getChecksum();
        if (updatedChecksum != null) {
          System.arraycopy(updatedChecksum, 0, buf, start, checksumSize);
        }
      }
    }
    
    int dataOff = checksumOff + checksumDataLen;
    if (!transferTo) { // normal transfer
      try {
        ris.readDataFully(buf, dataOff, dataLen);
      } catch (IOException ioe) {
        if (ioe.getMessage().startsWith(EIO_ERROR)) {
          throw new DiskFileCorruptException("A disk IO error occurred", ioe);
        }
        throw ioe;
      }

      if (verifyChecksum) {
        verifyChecksum(buf, dataOff, dataLen, numChunks, checksumOff);
      }
    }
    
    try {
      if (transferTo) {
        SocketOutputStream sockOut = (SocketOutputStream)out;
        // First write header and checksums
        sockOut.write(buf, headerOff, dataOff - headerOff);

        // no need to flush since we know out is not a buffered stream
        FileChannel fileCh = ((FileInputStream)ris.getDataIn()).getChannel();
        LongWritable waitTime = new LongWritable();
        LongWritable transferTime = new LongWritable();
        fileIoProvider.transferToSocketFully(
            ris.getVolumeRef().getVolume(), sockOut, fileCh, blockInPosition,
            dataLen, waitTime, transferTime);
        datanode.metrics.addSendDataPacketBlockedOnNetworkNanos(waitTime.get());
        datanode.metrics.addSendDataPacketTransferNanos(transferTime.get());
        blockInPosition += dataLen;
      } else {
        // normal transfer
        out.write(buf, headerOff, dataOff + dataLen - headerOff);
      }
    } catch (IOException e) {
      if (e instanceof SocketTimeoutException) {
        /*
         * writing to client timed out.  This happens if the client reads
         * part of a block and then decides not to read the rest (but leaves
         * the socket open).
         * 
         * Reporting of this case is done in DataXceiver#run
         */
        LOG.warn("Sending packets timed out.", e);
      } else {
        /* Exception while writing to the client. Connection closure from
         * the other end is mostly the case and we do not care much about
         * it. But other things can go wrong, especially in transferTo(),
         * which we do not want to ignore.
         *
         * The message parsing below should not be considered as a good
         * coding example. NEVER do it to drive a program logic. NEVER.
         * It was done here because the NIO throws an IOException for EPIPE.
         */
        String ioem = e.getMessage();
        if (ioem != null) {
          /*
           * If we got an EIO when reading files or transferTo the client
           * socket, it's very likely caused by bad disk track or other file
           * corruptions.
           */
          if (ioem.startsWith(EIO_ERROR)) {
            throw new DiskFileCorruptException("A disk IO error occurred", e);
          }
          String causeMessage = e.getCause() != null ? e.getCause().getMessage() : "";
          causeMessage = causeMessage != null ? causeMessage : "";
          if (!ioem.startsWith("Broken pipe")
              && !ioem.startsWith("Connection reset")
              && !causeMessage.startsWith("Broken pipe")
              && !causeMessage.startsWith("Connection reset")) {
            LOG.error("BlockSender.sendChunks() exception: ", e);
            datanode.getBlockScanner().markSuspectBlock(
                ris.getVolumeRef().getVolume().getStorageID(), block);
          }
        }
      }
      throw ioeToSocketException(e);
    }

    if (throttler != null) { // rebalancing so throttle
      throttler.throttle(packetLen);
    }

    return dataLen;
  }
  
  /**
   * Read checksum into given buffer
   * @param buf buffer to read the checksum into
   * @param checksumOffset offset at which to write the checksum into buf
   * @param checksumLen length of checksum to write
   * @throws IOException on error
   */
  private void readChecksum(byte[] buf, final int checksumOffset,
      final int checksumLen) throws IOException {
    if (checksumSize <= 0 && ris.getChecksumIn() == null) {
      return;
    }
    try {
      ris.readChecksumFully(buf, checksumOffset, checksumLen);
    } catch (IOException e) {
      LOG.warn(" Could not read or failed to verify checksum for data"
          + " at offset " + offset + " for block " + block, e);
      ris.closeChecksumStream();
      if (corruptChecksumOk) {
        if (checksumLen > 0) {
          // Just fill the array with zeros.
          Arrays.fill(buf, checksumOffset, checksumOffset + checksumLen,
              (byte) 0);
        }
      } else {
        throw e;
      }
    }
  }

  /**
   * Compute checksum for chunks and verify the checksum that is read from
   * the metadata file is correct.
   * 
   * @param buf buffer that has checksum and data
   * @param dataOffset position where data is written in the buf
   * @param datalen length of data
   * @param numChunks number of chunks corresponding to data
   * @param checksumOffset offset where checksum is written in the buf
   * @throws ChecksumException on failed checksum verification
   */
  public void verifyChecksum(final byte[] buf, final int dataOffset,
      final int datalen, final int numChunks, final int checksumOffset)
      throws ChecksumException {
    int dOff = dataOffset;
    int cOff = checksumOffset;
    int dLeft = datalen;

    for (int i = 0; i < numChunks; i++) {
      checksum.reset();
      int dLen = Math.min(dLeft, chunkSize);
      checksum.update(buf, dOff, dLen);
      if (!checksum.compare(buf, cOff)) {
        long failedPos = offset + datalen - dLeft;
        StringBuilder replicaInfoString = new StringBuilder();
        if (replica != null) {
          replicaInfoString.append(" for replica: " + replica.toString());
        }
        throw new ChecksumException("Checksum failed at " + failedPos
            + replicaInfoString, failedPos);
      }
      dLeft -= dLen;
      dOff += dLen;
      cOff += checksumSize;
    }
  }
  
  /**
   * 读取数据块及其元数据并将数据流式传输到客户端或另一个数据节点
   * 
   * 此方法是DataNode中数据发送的核心入口点，负责协调数据块的读取和传输过程，
   * 支持性能跟踪、错误处理和资源管理
   * 
   * @param out 用于写入块数据的输出流
   * @param baseStream 可选参数，若不为空，则假定out是此流的包装，
   *                   这可以启用数据发送的优化，如使用transferTo方法
   * @param throttler 用于限制数据传输带宽的节流器
   * @return 总共读取的字节数，包括校验和数据
   * @throws IOException 如果传输过程中发生IO异常
   */
  long sendBlock(DataOutputStream out, OutputStream baseStream, 
                 DataTransferThrottler throttler) throws IOException {
    // 创建性能跟踪范围，用于监控块发送过程
    final TraceScope scope = FsTracer.get(null)
        .newScope("sendBlock_" + block.getBlockId());
    try {
      // 委托给内部方法doSendBlock进行实际的数据发送
      return doSendBlock(out, baseStream, throttler);
    } finally {
      // 确保跟踪范围被关闭，无论发送成功还是失败
      scope.close();
    }
  }

  /**
   * 实际执行数据块发送的内部方法
   * 
   * 此方法实现了数据块的分块读取、校验和验证以及网络发送的完整逻辑，
   * 支持普通传输和优化传输(transferTo)两种模式
   * 
   * @param out 用于写入块数据的输出流
   * @param baseStream 底层输出流，用于优化传输
   * @param throttler 带宽限制器
   * @return 总共读取的字节数
   * @throws IOException 如果传输过程中发生IO异常
   */
  private long doSendBlock(DataOutputStream out, OutputStream baseStream,
        DataTransferThrottler throttler) throws IOException {
    // 参数校验：确保输出流不为空
    if (out == null) {
      throw new IOException( "out stream is null" );
    }
    
    // 保存初始偏移量和初始化总读取字节数计数器
    initialOffset = offset;
    long totalRead = 0;
    // 默认使用传入的输出流进行数据发送
    OutputStream streamForSendChunks = out;
    
    // 初始化缓存丢弃的偏移量
    lastCacheDropOffset = initialOffset;

    // 对于长读操作，通知操作系统此文件描述符将被顺序访问，以优化性能
    if (isLongRead() && ris.getDataInFd() != null) {
      ris.dropCacheBehindReads(block.getBlockName(), 0, 0,
          POSIX_FADV_SEQUENTIAL);
    }
    
    // 根据配置触发文件开头的预读
    manageOsCache();

    // 如果启用了客户端跟踪日志，记录开始时间
    final long startTime = CLIENT_TRACE_LOG.isDebugEnabled() ? System.nanoTime() : 0;
    try {
      // 确定每个数据包中最多包含的块数量和数据包缓冲区大小
      int maxChunksPerPacket;
      int pktBufSize = PacketHeader.PKT_MAX_HEADER_LEN;
      
      // 判断是否可以使用transferTo优化传输方式
      boolean transferTo = transferToAllowed && !verifyChecksum
          && baseStream instanceof SocketOutputStream
          && ris.getDataIn() instanceof FileInputStream;
      
      if (transferTo) {
        // 使用transferTo模式：直接从文件通道传输数据到套接字
        FileChannel fileChannel = 
            ((FileInputStream)ris.getDataIn()).getChannel();
        // 记录文件通道当前位置
        blockInPosition = fileChannel.position();
        // 使用底层流进行传输
        streamForSendChunks = baseStream;
        // 计算每个数据包中的最大块数
        maxChunksPerPacket = numberOfChunks(TRANSFERTO_BUFFER_SIZE);
        
        // transferTo模式下，数据包只需容纳校验和
        pktBufSize += checksumSize * maxChunksPerPacket;
      } else {
        // 使用普通传输模式
        maxChunksPerPacket = Math.max(1,
            numberOfChunks(IO_FILE_BUFFER_SIZE));
        // 普通模式下，数据包需要容纳数据和校验和
        pktBufSize += (chunkSize + checksumSize) * maxChunksPerPacket;
      }

      // 分配数据包缓冲区
      ByteBuffer pktBuf = ByteBuffer.allocate(pktBufSize);

      // 循环发送数据，直到发送完所有数据或线程被中断
      while (endOffset > offset && !Thread.currentThread().isInterrupted()) {
        // 管理操作系统缓存（预读和丢弃）
        manageOsCache();
        // 发送一个数据包
        long len = sendPacket(pktBuf, maxChunksPerPacket, streamForSendChunks,
            transferTo, throttler);
        // 更新当前偏移量
        offset += len;
        // 更新总读取字节数（包括数据和校验和）
        totalRead += len + (numberOfChunks(len) * checksumSize);
        // 增加序列号
        seqno++;
      }
      
      // 如果线程没有被中断，则发送完整的块
      if (!Thread.currentThread().isInterrupted()) {
        try {
          // 发送一个空数据包标记块传输结束
          sendPacket(pktBuf, maxChunksPerPacket, streamForSendChunks, transferTo,
              throttler);
          // 确保所有数据都被刷新到输出流
          out.flush();
        } catch (IOException e) { // 处理套接字错误
          throw ioeToSocketException(e);
        }

        // 标记已发送完整的字节范围
        sentEntireByteRange = true;
      }
    } finally {
      // 如果启用了客户端跟踪日志，记录传输完成日志
      if ((clientTraceFmt != null) && CLIENT_TRACE_LOG.isDebugEnabled()) {
        final long endTime = System.nanoTime();
        CLIENT_TRACE_LOG.debug(String.format(clientTraceFmt, totalRead,
            initialOffset, endTime - startTime));
      }
      // 确保关闭所有资源
      close();
    }
    // 返回总共读取的字节数
    return totalRead;
  }

  /**
   * Manage the OS buffer cache by performing read-ahead
   * and drop-behind.
   */
  private void manageOsCache() throws IOException {
    // We can't manage the cache for this block if we don't have a file
    // descriptor to work with.
    if (ris.getDataInFd() == null) {
      return;
    }

    // Perform readahead if necessary
    if ((readaheadLength > 0) && (datanode.readaheadPool != null) &&
          (alwaysReadahead || isLongRead())) {
      curReadahead = datanode.readaheadPool.readaheadStream(
          clientTraceFmt, ris.getDataInFd(), offset, readaheadLength,
          Long.MAX_VALUE, curReadahead);
    }

    // Drop what we've just read from cache, since we aren't
    // likely to need it again
    if (dropCacheBehindAllReads ||
        (dropCacheBehindLargeReads && isLongRead())) {
      long nextCacheDropOffset = lastCacheDropOffset + CACHE_DROP_INTERVAL_BYTES;
      if (offset >= nextCacheDropOffset) {
        long dropLength = offset - lastCacheDropOffset;
        ris.dropCacheBehindReads(block.getBlockName(), lastCacheDropOffset,
            dropLength, POSIX_FADV_DONTNEED);
        lastCacheDropOffset = offset;
      }
    }
  }

  /**
   * Returns true if we have done a long enough read for this block to qualify
   * for the DataNode-wide cache management defaults.  We avoid applying the
   * cache management defaults to smaller reads because the overhead would be
   * too high.
   *
   * Note that if the client explicitly asked for dropBehind, we will do it
   * even on short reads.
   * 
   * This is also used to determine when to invoke
   * posix_fadvise(POSIX_FADV_SEQUENTIAL).
   */
  private boolean isLongRead() {
    return (endOffset - initialOffset) > LONG_READ_THRESHOLD_BYTES;
  }

  /**
   * Write packet header into {@code pkt},
   * return the length of the header written.
   */
  private int writePacketHeader(ByteBuffer pkt, int dataLen, int packetLen) {
    pkt.clear();
    // both syncBlock and syncPacket are false
    PacketHeader header = new PacketHeader(packetLen, offset, seqno,
        (dataLen == 0), dataLen, false);
    
    int size = header.getSerializedSize();
    pkt.position(PacketHeader.PKT_MAX_HEADER_LEN - size);
    header.putInBuffer(pkt);
    return size;
  }
  
  boolean didSendEntireByteRange() {
    return sentEntireByteRange;
  }

  /**
   * @return the checksum type that will be used with this block transfer.
   */
  DataChecksum getChecksum() {
    return checksum;
  }

  /**
   * @return the offset into the block file where the sender is currently
   * reading.
   */
  long getOffset() {
    return offset;
  }
}
