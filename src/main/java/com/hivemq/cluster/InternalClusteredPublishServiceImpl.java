package com.hivemq.cluster;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.hivemq.bootstrap.ioc.lazysingleton.LazySingleton;
import com.hivemq.cluster.rpc.PublishRequest;
import com.hivemq.cluster.rpc.PublishResponse;
import com.hivemq.cluster.rpc.PublishServiceGrpc;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.mqtt.handler.publish.PublishReturnCode;
import com.hivemq.mqtt.message.publish.PUBLISH;
import com.hivemq.mqtt.services.InternalPublishService;
import com.hivemq.mqtt.services.InternalPublishServiceImpl;
import com.hivemq.persistence.SingleWriterService;
import com.hivemq.cluster.rpc.Adapters;
import com.hivemq.cluster.rpc.GRPCChannelRegistry;
import com.hivemq.persistence.payload.PublishPayloadPersistence;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

@LazySingleton
public class InternalClusteredPublishServiceImpl
        extends PublishServiceGrpc.PublishServiceImplBase
        implements InternalPublishService {
    private static final Logger log = LoggerFactory.getLogger(InternalClusteredPublishServiceImpl.class);

    @NotNull
    private final PublishPayloadPersistence payloadPersistence;
    @NotNull
    private final InternalPublishServiceImpl localPublishService;
    @NotNull
    private final SingleWriterService singleWriterService;
    @NotNull
    private final ClusteringService clusteringService;
    @NotNull
    private final GRPCChannelRegistry channelRegistry;

    @Inject
    public InternalClusteredPublishServiceImpl(
            @NotNull final PublishPayloadPersistence payloadPersistence,
            @NotNull final InternalPublishServiceImpl localPublishService,
            @NotNull final SingleWriterService singleWriterService,
            @NotNull final ClusteringService clusteringService,
            @NotNull final GRPCChannelRegistry channelRegistry) {
        this.payloadPersistence = payloadPersistence;
        this.localPublishService = localPublishService;
        this.singleWriterService = singleWriterService;
        this.clusteringService = clusteringService;
        this.channelRegistry = channelRegistry;
    }

    @Override
    public @NotNull ListenableFuture<PublishReturnCode> publish(
            @NotNull final PUBLISH publish,
            @NotNull final ExecutorService executorService,
            @Nullable final String sender) {
        final CompletableFuture<Set<InetSocketAddress>> nodesFetched =
                clusteringService.getSubscribedNodes(publish.getTopic());

        final CompletableFuture<PublishReturnCode> published =
                nodesFetched.thenCompose(nodes -> publishToNodes(publish, sender, nodes));

        return Adapters.adapt(published);
    }

    @Override
    public void publish(
            final PublishRequest request,
            final StreamObserver<PublishResponse> responseObserver) {
        log.debug("rpc:publish");

        final PUBLISH publish = Adapters.adapt(request.getPublish(), payloadPersistence);

        final ListenableFuture<PublishReturnCode> published = localPublishService.publish(
                publish, singleWriterService.callbackExecutor(request.getSender()), request.getSender()
        );

        Futures.addCallback(
                published,
                new FutureCallback<>() {
                    @Override
                    public void onSuccess(@Nullable final PublishReturnCode code) {
                        responseObserver.onNext(PublishResponse.newBuilder().setCode(code.getId()).build());
                        responseObserver.onCompleted();
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        responseObserver.onError(t);
                    }
                },
                singleWriterService.callbackExecutor(request.getSender())
        );
    }

    private CompletableFuture<PublishReturnCode> publishToNodes(
            @NotNull final PUBLISH publish,
            @NotNull final String sender,
            @NotNull final Set<InetSocketAddress> nodes) {
        if (Objects.isNull(nodes) || nodes.size() <= 0) {
            return CompletableFuture.completedFuture(PublishReturnCode.DELIVERED);
        }

        final PublishRequest request = PublishRequest.newBuilder()
                .setPublish(Adapters.adapt(publish))
                .setSender(sender)
                .build();

        final List<CompletableFuture<PublishResponse>> futures = new ArrayList<>();

        for (final InetSocketAddress node: nodes) {
            final ListenableFuture<PublishResponse> sent = client(node).publish(request);
            futures.add(Adapters.adapt(sent, singleWriterService.callbackExecutor(sender)));
        }

        final CompletableFuture<Void> allCompleted = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

        return allCompleted.thenApply(v -> {
            try {
                final Set<PublishReturnCode> codes = new HashSet<>();

                for (final CompletableFuture<PublishResponse> future : futures) {
                    final PublishReturnCode code = PublishReturnCode.valueOf(future.get().getCode());
                    codes.add(code);
                }

                return aggregate(codes);
            }
            catch (final InterruptedException | ExecutionException ex) {
                log.error(ex.getMessage());
                return PublishReturnCode.FAILED;
            }
        });
    }

    private PublishReturnCode aggregate(final Set<PublishReturnCode> codes) {
        if (codes.contains(PublishReturnCode.FAILED)) {
            return PublishReturnCode.FAILED;
        }

        if (codes.equals(Collections.singleton(PublishReturnCode.NO_MATCHING_SUBSCRIBERS))) {
            return PublishReturnCode.NO_MATCHING_SUBSCRIBERS;
        }

        return PublishReturnCode.DELIVERED;
    }

    private PublishServiceGrpc.PublishServiceFutureStub client(final InetSocketAddress node) {
        final ManagedChannel channel = channelRegistry.get(node.getHostName(), node.getPort());

        return PublishServiceGrpc.newFutureStub(channel);
    }
}
