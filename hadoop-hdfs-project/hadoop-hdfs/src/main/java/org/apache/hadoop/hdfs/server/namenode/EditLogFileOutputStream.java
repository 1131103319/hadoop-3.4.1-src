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

package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.LayoutFlags;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * An implementation of the abstract class {@link EditLogOutputStream}, which
 * stores edits in a local file.
 * todo EditLogFileOutputStream是向本地文件系统中保存的editlog文件写数据的输出流， 向EditLogFileOutputStream写数据时，
 *  数据首先被写入到输出流的缓冲区中， 当显式地调用flush()操作后， 数据才会从缓冲区同步到editlog文件中。
 *  在创建[ edits_inprogress_0000000000000000485  ]文件的时候, 首先会用"-1"填充1M大小的文件空间,然后将写入的指针归0.
 *  当有数据的时候,进行写入, 写入的时候,会覆盖之前预制填充的数据. 但不管怎么样, 如果数据大小不满1M的话, 那么edits文件的大小最小为1M.
 *  每次重启namenode 的时候都会将之前的edits_inprogress文件关闭,并重命名为edits_**** 文件,
 *  创建一个新的edits_inprogress_0000000000000000485文件.
 */
@InterfaceAudience.Private
public class EditLogFileOutputStream extends EditLogOutputStream {
    private static final Logger LOG =
            LoggerFactory.getLogger(EditLogFileOutputStream.class);
    public static final int MIN_PREALLOCATION_LENGTH = 1024 * 1024;
    //todo   // 输出流对应的editlog文件。
    private File file;
    //todo   // editlog文件对应的输出流。
    private FileOutputStream fp; // file stream for storing edit logs
    //todo   // editlog文件对应的输出流通道。
    private FileChannel fc; // channel of the file stream for sync
    //todo   // 一个具有两块缓存的缓冲区， 数据必须先写入缓存， 然后再由缓存同步到磁盘上。
    private EditsDoubleBuffer doubleBuf;
    //todo //用来扩充editlog文件大小的数据块。 当要进行同步操作时，
    //  // 如果editlog文件不够大， 则使用fill来扩充editlog。
    //  // 文件最小1M
    static final ByteBuffer fill = ByteBuffer.allocateDirect(MIN_PREALLOCATION_LENGTH);
    private boolean shouldSyncWritesAndSkipFsync = false;

    private static boolean shouldSkipFsyncForTests = false;
    //todo // EditLogFileOutputStream有一个static的代码段， 将fill字段用
    //  // FSEditLogOpCodes.OP_INVALID 字节填满。
    static {
        fill.position(0);
        for (int i = 0; i < fill.capacity(); i++) {
            fill.put(FSEditLogOpCodes.OP_INVALID.getOpCode());
        }
    }

    /**
     * Creates output buffers and file object.
     *
     * @param conf
     *          Configuration object
     * @param name
     *          File name to store edit log
     * @param size
     *          Size of flush buffer
     * @throws IOException
     */
    public EditLogFileOutputStream(Configuration conf, File name, int size)
            throws IOException {
        super();
        shouldSyncWritesAndSkipFsync = conf.getBoolean(
                DFSConfigKeys.DFS_NAMENODE_EDITS_NOEDITLOGCHANNELFLUSH,
                DFSConfigKeys.DFS_NAMENODE_EDITS_NOEDITLOGCHANNELFLUSH_DEFAULT);

        file = name;
        doubleBuf = new EditsDoubleBuffer(size);
        RandomAccessFile rp;
        if (shouldSyncWritesAndSkipFsync) {
            rp = new RandomAccessFile(name, "rwd");
        } else {
            rp = new RandomAccessFile(name, "rw");
        }
        try {
            fp = new FileOutputStream(rp.getFD()); // open for append
        } catch (IOException e) {
            IOUtils.closeStream(rp);
            throw e;
        }
        fc = rp.getChannel();
        fc.position(fc.size());
    }
    //todo // 直接调用doubleBuf中的对应方法
    //  // 向输出流写入一个操作
    @Override
    public void write(FSEditLogOp op) throws IOException {
        //todo     //向doubleBuf写入FSEditLogOp对象
        doubleBuf.writeOp(op, getCurrentLogVersion());
    }

    /**
     * Write a transaction to the stream. The serialization format is:
     * <ul>
     *   <li>the opcode (byte)</li>
     *   <li>the transaction id (long)</li>
     *   <li>the actual Writables for the transaction</li>
     * </ul>
     * */
    @Override
    public void writeRaw(byte[] bytes, int offset, int length) throws IOException {
        doubleBuf.writeRaw(bytes, offset, length);
    }

    /**
     * Create empty edits logs file.
     */
    @Override
    public void create(int layoutVersion) throws IOException {
        fc.truncate(0);
        fc.position(0);
        writeHeader(layoutVersion, doubleBuf.getCurrentBuf());
        setReadyToFlush();
        flush();
        setCurrentLogVersion(layoutVersion);
    }

    /**
     * Write header information for this EditLogFileOutputStream to the provided
     * DataOutputSream.
     *
     * @param layoutVersion the LayoutVersion of the EditLog
     * @param out the output stream to write the header to.
     * @throws IOException in the event of error writing to the stream.
     */
    @VisibleForTesting
    public static void writeHeader(int layoutVersion, DataOutputStream out)
            throws IOException {
        out.writeInt(layoutVersion);
        LayoutFlags.write(out);
    }

    @Override
    public void close() throws IOException {
        if (fp == null) {
            throw new IOException("Trying to use aborted output stream");
        }

        try {
            // close should have been called after all pending transactions
            // have been flushed & synced.
            // if already closed, just skip
            if (doubleBuf != null) {
                doubleBuf.close();
                doubleBuf = null;
            }

            // remove any preallocated padding bytes from the transaction log.
            if (fc != null && fc.isOpen()) {
                fc.truncate(fc.position());
                fc.close();
                fc = null;
            }
            fp.close();
            fp = null;
        } finally {
            IOUtils.cleanupWithLogger(LOG, fc, fp);
            doubleBuf = null;
            fc = null;
            fp = null;
        }
        fp = null;
    }

    @Override
    public void abort() throws IOException {
        if (fp == null) {
            return;
        }
        IOUtils.cleanupWithLogger(LOG, fp);
        fp = null;
    }

    /**
     * All data that has been written to the stream so far will be flushed. New
     * data can be still written to the stream while flushing is performed.
     * todo * 为同步数据做准备
     *    * 调用doubleBuf.setReadyToFlush()交换两个缓冲区
     */
    @Override
    public void setReadyToFlush() throws IOException {
        doubleBuf.setReadyToFlush();
    }

    /**
     * Flush ready buffer to persistent store. currentBuffer is not flushed as it
     * accumulates new log records while readyBuffer will be flushed and synced.
     * todo * 将准备好的缓冲区刷新到持久性存储。
     *    * 由于会刷新和同步readyBuffer，因此currentBuffer不会累积新的日志记录，因此不会刷新。
     */
    @Override
    public void flushAndSync(boolean durable) throws IOException {
        if (fp == null) {
            throw new IOException("Trying to use aborted output stream");
        }
        if (doubleBuf.isFlushed()) {
            LOG.info("Nothing to flush");
            return;
        }
        //todo     // preallocate()方法用于在editLog文件大小不够时， 填充editlog文件。
        preallocate(); // preallocate file if necessary
        //todo     //将缓存中的数据同步到editlog文件中。
        doubleBuf.flushTo(fp);
        if (durable && !shouldSkipFsyncForTests && !shouldSyncWritesAndSkipFsync) {
            fc.force(false); // metadata updates not needed
        }
    }

    /**
     * @return true if the number of buffered data exceeds the intial buffer size
     */
    @Override
    public boolean shouldForceSync() {
        return doubleBuf.shouldForceSync();
    }

    private void preallocate() throws IOException {
        long position = fc.position();
        long size = fc.size();
        int bufSize = doubleBuf.getReadyBuf().getLength();
        long need = bufSize - (size - position);
        if (need <= 0) {
            return;
        }
        long oldSize = size;
        long total = 0;
        long fillCapacity = fill.capacity();
        while (need > 0) {
            fill.position(0);
            IOUtils.writeFully(fc, fill, size);
            need -= fillCapacity;
            size += fillCapacity;
            total += fillCapacity;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Preallocated " + total + " bytes at the end of " +
                    "the edit log (offset " + oldSize + ")");
        }
    }

    /**
     * Returns the file associated with this stream.
     */
    File getFile() {
        return file;
    }

    @Override
    public String toString() {
        return "EditLogFileOutputStream(" + file + ")";
    }

    /**
     * @return true if this stream is currently open.
     */
    public boolean isOpen() {
        return fp != null;
    }

    @VisibleForTesting
    public void setFileChannelForTesting(FileChannel fc) {
        this.fc = fc;
    }

    @VisibleForTesting
    public FileChannel getFileChannelForTesting() {
        return fc;
    }

    /**
     * For the purposes of unit tests, we don't need to actually
     * write durably to disk. So, we can skip the fsync() calls
     * for a speed improvement.
     * @param skip true if fsync should <em>not</em> be called
     */
    @VisibleForTesting
    public static void setShouldSkipFsyncForTesting(boolean skip) {
        shouldSkipFsyncForTests = skip;
    }
}
