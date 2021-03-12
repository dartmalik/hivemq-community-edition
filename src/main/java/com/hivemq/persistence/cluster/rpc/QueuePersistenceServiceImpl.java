package com.hivemq.persistence.cluster.rpc;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.hivemq.cluster.rpc.AddMessageRequest;
import com.hivemq.cluster.rpc.AddMessageResponse;
import com.hivemq.cluster.rpc.PublishModel;
import com.hivemq.cluster.rpc.QueuePersistenceServiceGrpc;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.mqtt.message.publish.PUBLISH;
import com.hivemq.persistence.SingleWriterService;
import com.hivemq.persistence.clientqueue.ClientQueuePersistence;
import com.hivemq.persistence.payload.PublishPayloadPersistence;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;

public class QueuePersistenceServiceImpl extends QueuePersistenceServiceGrpc.QueuePersistenceServiceImplBase {
    private final ClientQueuePersistence persistence;
    private final PublishPayloadPersistence payloadPersistence;
    private final SingleWriterService singleWriterService;

    public QueuePersistenceServiceImpl(@NotNull final ClientQueuePersistence persistence,
                                       @NotNull final PublishPayloadPersistence payloadPersistence,
                                       @NotNull final SingleWriterService singleWriterService) {
        this.persistence = persistence;
        this.payloadPersistence = payloadPersistence;
        this.singleWriterService = singleWriterService;
    }

    @Override
    public void add(final AddMessageRequest request, final StreamObserver<AddMessageResponse> responseObserver) {
        final List<PUBLISH> publishes = new ArrayList<>();
        for (final PublishModel model: request.getPublishedList()) {
            publishes.add(Adapters.adapt(model, payloadPersistence));
        }

        final ListenableFuture<Void> added = persistence.add(
                request.getQueueId(), request.getShared(), publishes, request.getRetained(), request.getQueueLimit()
        );

        Futures.addCallback(
                added,
                new FutureCallback<>() {
                    @Override
                    public void onSuccess(Void result) {
                        responseObserver.onNext(AddMessageResponse.newBuilder().build());
                        responseObserver.onCompleted();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        responseObserver.onError(t);
                    }
                },
                singleWriterService.callbackExecutor(request.getQueueId())
        );
    }
}
