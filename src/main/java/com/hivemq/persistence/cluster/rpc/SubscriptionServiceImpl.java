package com.hivemq.persistence.cluster.rpc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.hivemq.cluster.rpc.*;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.mqtt.message.subscribe.Topic;
import com.hivemq.persistence.SingleWriterService;
import com.hivemq.persistence.clientsession.ClientSessionSubscriptionPersistence;
import com.hivemq.persistence.clientsession.callback.SubscriptionResult;
import io.grpc.stub.StreamObserver;

import java.util.Set;
import java.util.stream.Collectors;

public class SubscriptionServiceImpl extends SubscriptionServiceGrpc.SubscriptionServiceImplBase {
    @NotNull
    private final ClientSessionSubscriptionPersistence persistence;
    @NotNull
    private final SingleWriterService singleWriterService;

    public SubscriptionServiceImpl(@NotNull final ClientSessionSubscriptionPersistence persistence,
                                   @NotNull final SingleWriterService singleWriterService) {
        this.persistence = persistence;
        this.singleWriterService = singleWriterService;
    }

    @Override
    public void addSubscriptions(final AddSubscriptionsRequest request,
                                 final StreamObserver<AddSubscriptionsResponse> responseObserver) {
        final ImmutableSet.Builder<Topic> builder = ImmutableSet.builder();

        for (final TopicModel model: request.getTopicList()) {
            builder.add(Adapters.adapt(model));
        }

        final ListenableFuture<ImmutableList<SubscriptionResult>> added =
                    persistence.addSubscriptions(request.getClient(), builder.build());

        Futures.addCallback(added, new FutureCallback<>() {
            @Override
            public void onSuccess(final ImmutableList<SubscriptionResult> results) {
                final AddSubscriptionsResponse.Builder responseBuilder = AddSubscriptionsResponse.newBuilder();

                for (final SubscriptionResult res: results) {
                    final SubscriptionResultModel model = SubscriptionResultModel.newBuilder()
                            .setTopic(Adapters.adapt(res.getTopic()))
                            .setShareName(res.getShareName())
                            .setSubscriptionAlreadyExisted(res.subscriptionAlreadyExisted())
                            .build();

                    responseBuilder.addResults(model);
                }

                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
            }

            @Override
            public void onFailure(final Throwable t) {
                responseObserver.onError(t);
            }
        }, singleWriterService.callbackExecutor(request.getClient()));
    }

    @Override
    public void getSubscriptions(final GetSubscriptionsRequest request,
                                 final StreamObserver<GetSubscriptionsResponse> responseObserver) {
        final ImmutableSet<Topic> topics = persistence.getSubscriptions(request.getClient());
        final GetSubscriptionsResponse.Builder responseBuilder = GetSubscriptionsResponse.newBuilder();

        for (final Topic topic: topics) {
            responseBuilder.addTopics(Adapters.adapt(topic));
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void removeSubscriptions(final RemoveSubscriptionsRequest request,
                                    final StreamObserver<RemoveSubscriptionsResponse> responseObserver) {
        final Set<String> topics = request.getTopicList()
                .stream()
                .map(TopicModel::getTopic)
                .collect(Collectors.toSet());

        final ImmutableSet<String> topicsSet = ImmutableSet.<String>builder()
                .addAll(topics)
                .build();

        final ListenableFuture<Void> removed = persistence.removeSubscriptions(request.getClient(), topicsSet);

        Futures.addCallback(removed, new FutureCallback<Void>() {
            @Override
            public void onSuccess(final Void result) {
                responseObserver.onNext(RemoveSubscriptionsResponse.newBuilder().build());
                responseObserver.onCompleted();
            }

            @Override
            public void onFailure(Throwable t) {
                responseObserver.onError(t);
            }
        }, singleWriterService.callbackExecutor(request.getClient()));
    }
}
