package cainsgl.core.persistence.AOF.impl;

import cainsgl.core.config.MutConfiguration;
import cainsgl.core.excepiton.MutPersistenceException;
import cainsgl.core.persistence.AOF.valueObj.AOFReadVO;
import io.netty.channel.EventLoop;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * AOF version 4.
 *
 * <p>One instance belongs to exactly one command-owner {@link EventLoop}.  Only that
 * EventLoop may append commands or rotate buffers.  A sealed buffer is handed to the
 * process-wide single writer, which is the only thread allowed to read, write, clear,
 * and return that buffer.  Consequently an ACTIVE buffer is never flipped by the
 * writer while its owner is still appending to it.</p>
 *
 * <p>Buffer lifecycle: FREE -&gt; ACTIVE -&gt; SEALED -&gt; WRITING -&gt; FREE.</p>
 */
public final class AOFListener4 implements AutoCloseable {
    private static final Logger log = LogManager.getLogger(AOFListener4.class);

    private static final String FILE_NAME = MutConfiguration.AOF.FILE_NAME;
    private static final int BUFFER_SIZE = MutConfiguration.AOF.BUFFER_SIZE;
    private static final long FLUSH_INTERVAL_MILLIS = MutConfiguration.AOF.INTERVAL_TIME;

    /** All owners submit sealed buffers here; this is the only AOF file consumer. */
    private static final BlockingQueue<FlushTask> FLUSH_QUEUE = new LinkedBlockingQueue<>();

    static {
        Thread writer = new Thread(AOFListener4::writerLoop, "mut-aof-writer");
        writer.setDaemon(true);
        writer.start();
    }

    private final byte[] commandName;
    private final EventLoop ownerEventLoop;
    private final ArrayDeque<ByteBuffer> freeBuffers = new ArrayDeque<>();
    private final AtomicBoolean flushRequested = new AtomicBoolean();
    private final ScheduledExecutorService flushScheduler;

    /** Accessed only by ownerEventLoop. */
    private ByteBuffer active;
    /** Accessed only by ownerEventLoop. */
    private boolean closed;

    /**
     * Creates an AOF stream whose calls to {@link #addCommand(byte[][], long)} must
     * run on {@code ownerEventLoop}.
     *
     * @param bufferCount number of buffers owned by this stream; two implements
     *                    strict double buffering, while three or more reduce
     *                    backpressure when disk I/O is slow
     */
    public AOFListener4(byte[] commandName, EventLoop ownerEventLoop, int bufferCount) {
        if (commandName == null || commandName.length == 0) {
            throw new IllegalArgumentException("commandName must not be empty");
        }
        if (ownerEventLoop == null) {
            throw new IllegalArgumentException("ownerEventLoop must not be null");
        }
        if (BUFFER_SIZE <= 0 || bufferCount < 2) {
            throw new IllegalArgumentException("AOF requires a positive buffer size and at least two buffers");
        }

        this.commandName = commandName.clone();
        this.ownerEventLoop = ownerEventLoop;
        this.active = ByteBuffer.allocate(BUFFER_SIZE);
        for (int i = 1; i < bufferCount; i++) {
            freeBuffers.add(ByteBuffer.allocate(BUFFER_SIZE));
        }

        flushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "mut-aof-flush-requester");
            thread.setDaemon(true);
            return thread;
        });
        flushScheduler.scheduleAtFixedRate(this::requestFlush,
                FLUSH_INTERVAL_MILLIS, FLUSH_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    }

    /** Uses the configured AOF buffer count (at least two). */
    public AOFListener4(byte[] commandName, EventLoop ownerEventLoop) {
        this(commandName, ownerEventLoop, Math.max(2, MutConfiguration.AOF.BUFFER_COUNT));
    }

    /**
     * Appends one complete AOF record.  It must be invoked by the command-owner
     * EventLoop, after the command has successfully changed in-memory state.
     *
     * @return false when no replacement buffer is available; the caller should
     *         apply backpressure/retry rather than write into a buffer being flushed
     */
    public boolean addCommand(byte[][] args, long commandTime) {
        assertOwnerThread();
        if (closed) {
            throw new IllegalStateException("AOF listener is closed");
        }

        int recordSize = recordSize(args);
        if (recordSize > active.capacity()) {
            throw new MutPersistenceException("AOF command exceeds a single buffer capacity");
        }

        if (active.remaining() < recordSize && !rotateActiveBuffer()) {
            // Keep the request set.  When the writer returns a buffer, the owner
            // event loop will perform the deferred rotation.
            flushRequested.set(true);
            return false;
        }

        writeRecord(active, args, commandTime);
        if (flushRequested.get()) {
            handleFlushRequest();
        }
        return true;
    }

    /** May be called by a timer, threshold check, or shutdown code from any thread. */
    public void requestFlush() {
        if (closed || !flushRequested.compareAndSet(false, true)) {
            return;
        }
        ownerEventLoop.execute(this::handleFlushRequest);
    }

    /** Runs only on the owner EventLoop, between complete command records. */
    private void handleFlushRequest() {
        assertOwnerThread();
        if (!flushRequested.get() || active.position() == 0) {
            flushRequested.set(false);
            return;
        }
        if (rotateActiveBuffer()) {
            flushRequested.set(false);
        }
        // No FREE buffer: leave the flag set. onBufferReturned will retry.
    }

    /**
     * Owner-side ownership transfer: old ACTIVE becomes SEALED and is queued for
     * the writer; a previously FREE buffer becomes the new ACTIVE buffer.
     */
    private boolean rotateActiveBuffer() {
        assertOwnerThread();
        ByteBuffer next = freeBuffers.pollFirst();
        if (next == null) {
            return false;
        }

        ByteBuffer sealed = active;
        active = next;                 // Future commands now write only to next.
        sealed.flip();                 // No owner write to sealed after this line.
        FLUSH_QUEUE.offer(new FlushTask(this, sealed));
        return true;
    }

    private void onBufferReturned(ByteBuffer buffer) {
        assertOwnerThread();
        freeBuffers.addLast(buffer);
        if (flushRequested.get() && active.position() > 0) {
            handleFlushRequest();
        }
    }

    private static void writerLoop() {
        while (true) {
            try {
                FlushTask task = FLUSH_QUEUE.take();
                ByteBuffer sealed = task.buffer;
                try (FileChannel channel = new FileOutputStream(FILE_NAME, true).getChannel()) {
                    while (sealed.hasRemaining()) {
                        channel.write(sealed);
                    }
                }
                sealed.clear();
                task.listener.ownerEventLoop.execute(() -> task.listener.onBufferReturned(sealed));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            } catch (IOException ioException) {
                log.error("AOF writer failed; the sealed buffer was not returned", ioException);
            } catch (RuntimeException exception) {
                log.error("Unexpected AOF writer failure", exception);
            }
        }
    }

    private int recordSize(byte[][] args) {
        if (args == null) {
            throw new IllegalArgumentException("args must not be null");
        }
        long size = Long.BYTES + Integer.BYTES + commandName.length + Integer.BYTES;
        for (byte[] arg : args) {
            if (arg == null) {
                throw new IllegalArgumentException("AOF arguments must not contain null");
            }
            size += Integer.BYTES + arg.length;
        }
        if (size > Integer.MAX_VALUE) {
            throw new MutPersistenceException("AOF command is too large");
        }
        return (int) size;
    }

    private void writeRecord(ByteBuffer target, byte[][] args, long commandTime) {
        target.putLong(commandTime);
        target.putInt(commandName.length);
        target.put(commandName);
        target.putInt(args.length);
        for (byte[] arg : args) {
            target.putInt(arg.length);
            target.put(arg);
        }
    }

    public List<AOFReadVO> readAOF() {
        List<AOFReadVO> records = new ArrayList<>();
        try (DataInputStream input = new DataInputStream(new FileInputStream(FILE_NAME))) {
            while (input.available() > 0) {
                long commandTime = input.readLong();
                byte[] command = input.readNBytes(input.readInt());
                int argCount = input.readInt();
                byte[][] args = new byte[argCount][];
                for (int i = 0; i < argCount; i++) {
                    args[i] = input.readNBytes(input.readInt());
                }
                records.add(new AOFReadVO(commandTime, command, args));
            }
        } catch (IOException exception) {
            log.error("Unable to read AOF", exception);
        }
        Collections.sort(records);
        return records;
    }

    @Override
    public void close() {
        flushScheduler.shutdown();
        requestFlush();
        closed = true;
    }

    private void assertOwnerThread() {
        if (!ownerEventLoop.inEventLoop()) {
            throw new IllegalStateException("AOF buffer ownership operation must run on its owner EventLoop");
        }
    }

    private record FlushTask(AOFListener4 listener, ByteBuffer buffer) {
    }
}
