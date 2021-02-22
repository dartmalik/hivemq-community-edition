package com.hivemq.persistence;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.*;
import com.hivemq.configuration.service.InternalConfigurations;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.persistence.local.xodus.bucket.BucketUtils;
import com.hivemq.util.ThreadFactoryUtil;
import io.netty.util.concurrent.EventExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.hivemq.persistence.SingleWriterServiceImpl.Task;


/**
 * @author Daniel Krüger
 */
public class NettyEventLoopProducerQueuesImpl implements ProducerQueues {

    @VisibleForTesting
    final int bucketsPerQueue;
    private final @NotNull AtomicLong taskCount = new AtomicLong(0);
    private final int amountOfQueues;
    private final @NotNull NettyEventLoopSingleWriterImpl singleWriter;

    private final @NotNull ImmutableList<ImmutableList<Integer>> queueBucketIndexes;

    private final int shutdownGracePeriod;

    private final EventExecutor @NotNull [] eventExecutors;

    private final @NotNull AtomicBoolean shutdown = new AtomicBoolean(false);

    private @Nullable ListenableFuture<Void> closeFuture;
    private long shutdownStartTime = Long.MAX_VALUE; // Initialized as long max value, to ensure the the grace period condition is not met, when shutdown is true but the start time is net yet set.

    public NettyEventLoopProducerQueuesImpl(final @NotNull NettyEventLoopSingleWriterImpl singleWriterService, final int amountOfQueues, final EventExecutor @NotNull [] eventExecutors) {
        this.singleWriter = singleWriterService;
        this.eventExecutors = eventExecutors;
        final int bucketCount = singleWriter.getPersistenceBucketCount();
        shutdownGracePeriod = InternalConfigurations.PERSISTENCE_SHUTDOWN_GRACE_PERIOD.get();
        this.amountOfQueues = amountOfQueues;
        bucketsPerQueue = bucketCount / amountOfQueues;

        final ImmutableList.Builder<ImmutableList<Integer>> bucketIndexListBuilder = ImmutableList.builder();
        final ImmutableList.Builder<AtomicLong> counterBuilder = ImmutableList.builder();

        for (int i = 0; i < amountOfQueues; i++) {
            counterBuilder.add(new AtomicLong(0));
            bucketIndexListBuilder.add(createBucketIndexes(i, bucketsPerQueue));
        }
        @NotNull final ImmutableList<AtomicLong> queueTaskCounter = counterBuilder.build();
        queueBucketIndexes = bucketIndexListBuilder.build();
    }


    @NotNull
    @VisibleForTesting
    ImmutableList<Integer> createBucketIndexes(final int queueIndex, final int bucketsPerQueue) {
        final ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        for (int i = bucketsPerQueue * queueIndex; i < bucketsPerQueue * (queueIndex + 1); i++) {
            builder.add(i);
        }
        return builder.build();
    }

    @NotNull
    public <R> ListenableFuture<R> submit(@NotNull final String key, @NotNull final Task<R> task) {
        //noinspection ConstantConditions (futuer is never null if the callbacks are null)
        return submit(getBucket(key), task, null, null);
    }

    @NotNull
    public <R> ListenableFuture<R> submit(final int bucketIndex,
                                          @NotNull final Task<R> task) {
        //noinspection ConstantConditions (futuer is never null if the callbacks are null)
        return submit(bucketIndex, task, null, null);
    }


    @Nullable
    public <R> ListenableFuture<R> submit(final int bucketIndex,
                                          @NotNull final Task<R> task,
                                          @Nullable final SingleWriterService.SuccessCallback<R> successCallback,
                                          @Nullable final SingleWriterService.FailedCallback failedCallback) {

        return submit(bucketIndex, task, successCallback, failedCallback, false);
    }

    @Nullable
    public <R> ListenableFuture<R> submit(final int bucketIndex,
                                          @NotNull final Task<R> task,
                                          @Nullable final SingleWriterServiceImpl.SuccessCallback<R> successCallback,
                                          @Nullable final SingleWriterServiceImpl.FailedCallback failedCallback,
                                          final boolean ignoreShutdown) {
        final int queueIndex = bucketIndex / bucketsPerQueue;
        final EventExecutor eventExecutor = eventExecutors[queueIndex % eventExecutors.length];
        //TODO correct shutdown handling
        if ((eventExecutor.isShuttingDown() || eventExecutor.isShutdown()) && !ignoreShutdown) {
            return SettableFuture.create(); // Future will never return since we are shutting down.
        }
        if (!ignoreShutdown && shutdown.get() && System.currentTimeMillis() - shutdownStartTime > shutdownGracePeriod) {
            return SettableFuture.create(); // Future will never return since we are shutting down.
        }
        final SettableFuture<R> resultFuture;
        if (successCallback == null) {
            resultFuture = SettableFuture.create();
        } else {
            resultFuture = null;
        }
        if ((eventExecutor.isShuttingDown() || eventExecutor.isShutdown()) && ignoreShutdown) {
            // eventloop is terminated, but we need to clear persistences and so on
            try {
                final R result = task.doTask(bucketIndex, queueBucketIndexes.get(queueIndex), queueIndex);
                if (resultFuture != null) {
                    resultFuture.set(result);
                } else {
                    if (successCallback != null) {
                        successCallback.afterTask(result);
                    }
                }
            } catch (final Exception e) {
                if (resultFuture != null) {
                    resultFuture.setException(e);
                } else {
                    if (failedCallback != null) {
                        failedCallback.afterTask(e);
                    }
                }
            }
        } else {
            eventExecutor.submit(() -> {
                        try {
                            final R result = task.doTask(bucketIndex, queueBucketIndexes.get(queueIndex), queueIndex);
                            if (resultFuture != null) {
                                resultFuture.set(result);
                            } else {
                                if (successCallback != null) {
                                    successCallback.afterTask(result);
                                }
                            }
                        } catch (final Exception e) {
                            if (resultFuture != null) {
                                resultFuture.setException(e);
                            } else {
                                if (failedCallback != null) {
                                    failedCallback.afterTask(e);
                                }
                            }
                        }
                    }
            );
        }
        return resultFuture;
    }

    @NotNull
    public <R> List<ListenableFuture<R>> submitToAllQueues(final @NotNull Task<R> task) {
        return submitToAllQueues(task, false);
    }

    @NotNull
    public <R> ListenableFuture<List<R>> submitToAllQueuesAsList(final @NotNull Task<R> task) {
        return Futures.allAsList(submitToAllQueues(task, false));
    }

    @NotNull
    private <R> List<ListenableFuture<R>> submitToAllQueues(final @NotNull Task<R> task, final boolean ignoreShutdown) {
        if (!ignoreShutdown && shutdown.get() && System.currentTimeMillis() - shutdownStartTime > shutdownGracePeriod) {
            return Collections.singletonList(SettableFuture.create()); // Future will never return since we are shutting down.
        }
        final ImmutableList.Builder<ListenableFuture<R>> builder = ImmutableList.builder();
        final List<ListenableFuture<R>> futures = new ArrayList<>();
        for (int i = 0; i < amountOfQueues; i++) {
            final ListenableFuture<R> future = submit(i * bucketsPerQueue, task, null, null, ignoreShutdown);
            futures.add(future);
        }
        return futures;
    }


    public int getBucket(@NotNull final String key) {
        return BucketUtils.getBucket(key, singleWriter.getPersistenceBucketCount());
    }

    public void execute(final @NotNull SplittableRandom random) {

    }

    @NotNull
    public ListenableFuture<Void> shutdown(final @Nullable Task<Void> finalTask) {
        if (shutdown.getAndSet(true)) {
            //guard from being called twice
            //needed for integration tests because shutdown hooks for every Embedded HiveMQ are added to the JVM
            //if the persistence is stopped manually this would result in errors, because the shutdown hook might be called twice.
            if (closeFuture != null) {
                return closeFuture;
            }
            return Futures.immediateFuture(null);
        }

        shutdownStartTime = System.currentTimeMillis();
        // We create a temporary single thread executor when we shut down, so we don't waste a thread at runtime.
        final ThreadFactory threadFactory = ThreadFactoryUtil.create("persistence-shutdown-%d");
        final ListeningScheduledExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor(threadFactory));


        closeFuture = executorService.schedule(() -> {
            // Even if no task has to be executed on shutdown, we still have to delay the success of the close future by the shutdown grace period.
            if (finalTask != null) {
                Futures.allAsList(submitToAllQueues(finalTask, true)).get();
            } else {
                Futures.allAsList(submitToAllQueues((Task<Void>) (bucketIndex, queueBuckets, queueIndex) -> null, true)).get();
            }
            return null;
        }, shutdownGracePeriod + 50, TimeUnit.MILLISECONDS); // We may have to delay the task for some milliseconds, because a task could just get enqueued.

        Futures.addCallback(closeFuture, new FutureCallback<>() {
            @Override
            public void onSuccess(@Nullable final Void aVoid) {
                executorService.shutdown();
            }

            @Override
            public void onFailure(final @NotNull Throwable throwable) {
                executorService.shutdown();
            }
        }, executorService);
        return closeFuture;
    }

    @NotNull
    public AtomicLong getTaskCount() {
        return taskCount;
    }
}
