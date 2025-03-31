package tech.ydb.app;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.core.Issue;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.topic.read.DeferredCommitter;
import tech.ydb.topic.read.Message;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YqlWriter implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(YqlWriter.class);

    private final YdbService ydb;
    private final int errorThreshold;

    private final List<Writer> writers;

    private volatile Instant lastReaded;
    private volatile Instant lastWrited;

    private final AtomicLong lastPrinted = new AtomicLong();
    private final AtomicLong writtenCount = new AtomicLong();

    public YqlWriter(YdbService ydb, Supplier<CdcMsgParser> parser, XmlConfig.Cdc config) {
        this.ydb = ydb;
        this.errorThreshold = config.getErrorThreshold();

        this.lastWrited = null;
        this.lastReaded = null;
        this.writers = new ArrayList<>(config.getThreadsCount());

        for (int idx = 1; idx <= config.getThreadsCount(); idx++) {
            String name = "writer-" + config.getConsumer() + "[" + idx + "]";
            writers.add(new Writer(parser.get(), config.getBatchSize(), name));
        }
    }

    public Status getLastStatus() {
        for (int idx = 0; idx < writers.size(); idx++) {
            Status last = writers.get(idx).lastStatus;
            if (!last.isSuccess()) {
                return last;
            }
        }
        return Status.SUCCESS;
    }

    public Instant getLastWrited() {
        return lastWrited;
    }

    public Instant getLastReaded() {
        return lastReaded;
    }

    public void start() {
        lastPrinted.set(System.currentTimeMillis());
        writers.forEach(Writer::start);
    }

    @Override
    public void close() {
        writers.forEach(Writer::stop);

        try {
            for (Writer writer: writers) {
                writer.join();
            }
        } catch (InterruptedException ex) {
            logger.error("unexpected interrupt", ex);
        }
    }

    public void addMessage(long partitionId, Message msg) {
        int idx = (int) partitionId % writers.size();
        writers.get(idx).addMesssage(msg);
    }

    private class Writer implements Runnable {
        private final BlockingQueue<Message> queue;
        private final Thread thread;
        private final CdcMsgParser parser;
        private volatile Status lastStatus = Status.SUCCESS;

        public Writer(CdcMsgParser parser, int batchSize, String threadName) {
            this.parser = parser;
            this.queue = new ArrayBlockingQueue<>(2 * batchSize);
            this.thread = new Thread(this, threadName);
        }

        public void start() {
            thread.start();
            logger.info("writer {} started", thread.getName());
        }

        public void stop() {
            thread.interrupt();
            logger.info("writer {} stopped", thread.getName());
        }

        public void join() throws InterruptedException {
            thread.join();
            logger.info("writer {} finished", thread.getName());
        }

        public void addMesssage(Message msg) {
            try {
                while (!queue.offer(msg, 5, TimeUnit.SECONDS)) {
                    if (!thread.isAlive() || thread.isInterrupted()) {
                        return;
                    }
                }
                lastReaded = msg.getWrittenAt();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                logger.warn("worker thread was interrupted");
            }
        }

        @Override
        @SuppressWarnings("SleepWhileInLoop")
        public void run() {
            try {
                Random rnd = new Random();
                YqlQuery query = null;

                while (!Thread.interrupted()) {
                    long now = System.currentTimeMillis();
                    long printedAt = lastPrinted.get();
                    if (printedAt > 0L && (now - printedAt > 1000L) 
                            && lastPrinted.compareAndSet(printedAt, now)) {
                        long ms = now - printedAt;
                        long written = writtenCount.getAndSet(0);
                        double avg = 1000.0d * written / ms;
                        String w = String.format("%7d", written);
                        String a = String.format("%10.2f", avg);
                        logger.debug("written {} rows, {} rps", w, a);
                    }

                    Message msg = queue.poll();
                    if (msg == null) {
                        Thread.sleep(1000L);
                        continue;
                    }

                    DeferredCommitter committer = DeferredCommitter.newInstance();
                    Instant last = msg.getCreatedAt();

                    while (msg != null) {
                        YqlQuery nextQuery = parser.parseJsonMessage(msg.getData());
                        if (nextQuery != null) {
                            if (query != nextQuery) {
                                write(rnd, query, last);
                                committer.commit();
                                committer = DeferredCommitter.newInstance();
                            }
                            query = nextQuery;
                        }

                        last = msg.getCreatedAt();
                        committer.add(msg);

                        if (query != null && query.isFull()) {
                            break;
                        }

                        msg = queue.poll();
                    }

                    write(rnd, query, last);
                    committer.commit();
                }
            } catch (IOException ex) {
                logger.error("writer has stopped by exception", ex);
                lastStatus = Status.of(StatusCode.CLIENT_INTERNAL_ERROR, ex,
                        Issue.of(ex.getMessage(), Issue.Severity.ERROR));
            } catch (InterruptedException ex) {
                // stoppping
            }
        }

        @SuppressWarnings("SleepWhileInLoop")
        public void write(Random rnd, YqlQuery query, Instant lastMsgCreated) throws InterruptedException {
            if (query == null || query.isEmpty()) {
                return;
            }

            writtenCount.addAndGet(query.batchSize());
            long now = System.currentTimeMillis();
            lastStatus = query.execute(ydb);
            long ms = System.currentTimeMillis() - now;

            int retry = 0;
            while (!lastStatus.isSuccess()) {
                retry++;
                long delay = 25 << Math.min(retry, 8);
                delay = delay + rnd.nextLong(delay);
                if (retry > errorThreshold) {
                    logger.warn("got error {} after {} ms", lastStatus, ms);
                    logger.warn("retry #{} in {} ms", retry, delay);
                } else {
                    logger.trace("got error {} after {} ms", lastStatus, ms);
                    logger.trace("retry #{} in {} ms", retry, delay);
                }

                Thread.sleep(delay);

                now = System.currentTimeMillis();
                lastStatus = query.execute(ydb);
                ms = System.currentTimeMillis() - now;
            }

            query.clear();
            lastWrited = lastMsgCreated;
        }

    }
}
