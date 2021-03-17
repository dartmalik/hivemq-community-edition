package com.hivemq.persistence.cluster.rpc;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.ImmutableIntArray;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.hivemq.cluster.rpc.*;
import com.hivemq.codec.encoder.mqtt5.Mqtt5PayloadFormatIndicator;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.mqtt.message.MessageType;
import com.hivemq.mqtt.message.MessageWithID;
import com.hivemq.mqtt.message.QoS;
import com.hivemq.mqtt.message.mqtt5.Mqtt5RetainHandling;
import com.hivemq.mqtt.message.mqtt5.Mqtt5UserProperties;
import com.hivemq.mqtt.message.mqtt5.MqttUserProperty;
import com.hivemq.mqtt.message.publish.PUBLISH;
import com.hivemq.mqtt.message.publish.PUBLISHFactory;
import com.hivemq.mqtt.message.subscribe.Topic;
import com.hivemq.persistence.clientsession.callback.SubscriptionResult;
import com.hivemq.persistence.ioc.annotation.PayloadPersistence;
import com.hivemq.persistence.payload.PublishPayloadPersistence;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public class Adapters {
    private static class ParsedMessageWithId extends MessageWithID {
        private MessageType type;
        private int encodedLength;
        private int remainingLength;
        private int propertyLength;
        private int omittedProperties;

        public ParsedMessageWithId(
                final int packetId,
                final MessageType type,
                final int encodedLength,
                final int remainingLength,
                final int propertyLength,
                final int omittedProperties) {
            super(packetId);
            this.type = type;
            this.encodedLength = encodedLength;
            this.remainingLength = remainingLength;
            this.propertyLength = propertyLength;
            this.omittedProperties = omittedProperties;
        }

        @Override
        public @NotNull MessageType getType() {
            return type;
        }

        @Override
        public void setEncodedLength(final int length) {
            encodedLength = length;
        }

        @Override
        public int getEncodedLength() {
            return encodedLength;
        }

        @Override
        public void setRemainingLength(final int length) {
            remainingLength = length;
        }

        @Override
        public int getRemainingLength() {
            return remainingLength;
        }

        @Override
        public void setPropertyLength(final int length) {
            propertyLength = length;
        }

        @Override
        public int getPropertyLength() {
            return propertyLength;
        }

        @Override
        public void setOmittedProperties(final int omittedProperties) {
            this.omittedProperties = omittedProperties;
        }

        @Override
        public int getOmittedProperties() {
            return omittedProperties;
        }
    }

    public static Topic adapt(@NotNull final TopicModel model) {
        return new Topic(model.getTopic(),
                QoS.valueOf(model.getQos()),
                model.getNoLocal(),
                model.getRetainAsPublished(),
                Mqtt5RetainHandling.fromCode(model.getRetainHandling()),
                model.hasSubscriptionId() ? model.getSubscriptionId().getValue() : null);
    }

    public static TopicModel adapt(@NotNull final Topic topic) {
        final TopicModel.Builder builder = TopicModel.newBuilder()
                .setTopic(topic.getTopic())
                .setQos(topic.getQoS().getQosNumber())
                .setNoLocal(topic.isNoLocal())
                .setRetainAsPublished(topic.isRetainAsPublished())
                .setRetainHandling(topic.getRetainHandling().getCode());

        if (Objects.nonNull(topic.getSubscriptionIdentifier())) {
            builder.setSubscriptionId(from(topic.getSubscriptionIdentifier()));
        }

        return builder.build();
    }

    public static Int32Wrapper from(final Integer value) {
        return Int32Wrapper.newBuilder().setValue(value).build();
    }

    public static PublishModel adapt(@NotNull final PUBLISH publish) {
        final PublishModel.Builder builder = PublishModel.newBuilder()
                .setTopic(publish.getTopic())
                .setQos(publish.getQoS().getQosNumber())
                .setIsRetain(publish.isRetain())
                .setMessageExpiryInterval(publish.getMessageExpiryInterval())
                .setUserProperties(adapt(publish.getUserProperties()))
                .setPacketId(publish.getPacketIdentifier())
                .setIsDup(publish.isDuplicateDelivery())
                .setIsNewTopicAlias(publish.isNewTopicAlias())
                .setTimestamp(publish.getTimestamp())
                .setPublishId(publish.getPublishId());

        if (Objects.nonNull(publish.getPayload())) {
            builder.setPayload(ByteString.copyFrom(publish.getPayload()));
        }
        if (Objects.nonNull(publish.getPayloadFormatIndicator())) {
            builder.setPayloadFormatIndicator(publish.getPayloadFormatIndicator().getCode());
        }
        if (Objects.nonNull(publish.getContentType())) {
            builder.setContentType(publish.getContentType());
        }
        if (Objects.nonNull(publish.getResponseTopic())) {
            builder.setResponseTopic(publish.getResponseTopic());
            builder.setResponseTopic(publish.getResponseTopic());
        }
        if (Objects.nonNull(publish.getCorrelationData())) {
            builder.setCorrelationData(ByteString.copyFrom(publish.getCorrelationData()));
        }

        if (Objects.nonNull(publish.getSubscriptionIdentifiers())) {
            for (final Integer subId : publish.getSubscriptionIdentifiers().asList()) {
                builder.addSubscriptionIdentifiers(subId);
            }
        }

        return builder.build();
    }

    public static PUBLISH adapt(@NotNull final PublishModel model,
                                @NotNull final PublishPayloadPersistence payloadPersistence) {
        final PUBLISHFactory.Mqtt5Builder builder = new PUBLISHFactory.Mqtt5Builder();

        return builder.withHivemqId(model.getHivemqId())
                    .withTopic(model.getTopic())
                    .withPayload(model.getPayload().toByteArray())
                    .withQoS(QoS.valueOf(model.getQos()))
                    .withRetain(model.getIsRetain())
                    .withMessageExpiryInterval(model.getMessageExpiryInterval())
                    .withPayloadFormatIndicator(Mqtt5PayloadFormatIndicator.fromCode(model.getPayloadFormatIndicator()))
                    .withContentType(model.getContentType())
                    .withResponseTopic(model.getResponseTopic())
                    .withCorrelationData(model.getCorrelationData().toByteArray())
                    .withUserProperties(adapt(model.getUserProperties()))
                    .withPacketIdentifier(model.getPacketId())
                    .withDuplicateDelivery(model.getIsDup())
                    .withNewTopicAlias(model.getIsNewTopicAlias())
                    .withSubscriptionIdentifiers(ImmutableIntArray.copyOf(model.getSubscriptionIdentifiersList()))
                    .withPersistence(payloadPersistence)
                    .withTimestamp(model.getTimestamp())
                    .withPublishId(model.getPublishId())
                    .build();
    }

    public static MqttUserPropertiesModel adapt(@NotNull final Mqtt5UserProperties properties) {
        final MqttUserPropertiesModel.Builder builder = MqttUserPropertiesModel.newBuilder();

        if (Objects.isNull(properties)) {
            return builder.build();
        }

        for (final MqttUserProperty property: properties.asList()) {
            final MqttUserPropertyModel model = MqttUserPropertyModel.newBuilder()
                    .setName(property.getName())
                    .setValue(property.getValue())
                    .build();

            builder.addUserProperties(model);
        }

        return builder.build();
    }

    public static Mqtt5UserProperties adapt(@NotNull final MqttUserPropertiesModel model) {
        if (Objects.isNull(model)) {
            return Mqtt5UserProperties.NO_USER_PROPERTIES;
        }

        final ImmutableList.Builder<MqttUserProperty> builder = ImmutableList.builder();
        for (final MqttUserPropertyModel propertyModel: model.getUserPropertiesList()) {
            builder.add(new MqttUserProperty(propertyModel.getName(), propertyModel.getValue()));
        }

        return Mqtt5UserProperties.build(builder);
    }

    public static MessageWithID adapt(
            @NotNull final MessageWithIdModel model,
            @NotNull final PublishPayloadPersistence persistence) {
        if (model.hasPublish()) {
            return Adapters.adapt(model.getPublish(), persistence);
        } else {
            return new ParsedMessageWithId(
                    model.getDummy().getPacketId(),
                    MessageType.valueOf(model.getDummy().getType()),
                    model.getDummy().getEncodedLength(),
                    model.getDummy().getRemainingLength(),
                    model.getDummy().getPropertyLength(),
                    model.getDummy().getOmittedProperties()
            );
        }
    }

    public static MessageWithIdModel adapt(@NotNull final MessageWithID message) {
        final MessageWithIdModel.Builder builder = MessageWithIdModel.newBuilder();

        if (message instanceof PUBLISH) {
            builder.setPublish(Adapters.adapt((PUBLISH)message));
        } else {
            final DummyMessageWithIdModel dummy = DummyMessageWithIdModel.newBuilder()
                    .setPacketId(message.getPacketIdentifier())
                    .setType(message.getType().getType())
                    .setEncodedLength(message.getEncodedLength())
                    .setRemainingLength(message.getRemainingLength())
                    .setPropertyLength(message.getPropertyLength())
                    .setOmittedProperties(message.getOmittedProperties())
                    .build();
            builder.setDummy(dummy);
        }

        return builder.build();
    }

    public static SubscriptionResult adapt(@NotNull final SubscriptionResultModel model) {
        return new SubscriptionResult(
                adapt(model.getTopic()), model.getSubscriptionAlreadyExisted(), model.getShareName()
        );
    }

    public static SubscriptionResultModel adapt(@NotNull final SubscriptionResult result) {
        final SubscriptionResultModel.Builder builder = SubscriptionResultModel.newBuilder()
                .setTopic(Adapters.adapt(result.getTopic()))
                .setSubscriptionAlreadyExisted(result.subscriptionAlreadyExisted());

        if (Objects.nonNull(result.getShareName())) {
            builder.setShareName(result.getShareName());
        }

        return builder.build();
    }

    public static <T> CompletableFuture<T> adapt(
            @NotNull final ListenableFuture<T> future,
            @NotNull final ExecutorService executorService) {
        final CompletableFuture<T> result = new CompletableFuture<>();

        Futures.addCallback(
                future,
                new FutureCallback<T>() {
                    @Override
                    public void onSuccess(final T v) {
                        result.complete(v);
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        result.completeExceptionally(t);
                    }
                },
                executorService
        );

        return result;
    }

    public static <T> ListenableFuture<T> adapt(@NotNull final CompletableFuture<T> future) {
        final SettableFuture<T> result = SettableFuture.create();

        future.whenComplete((v, ex) -> {
            if (Objects.nonNull(ex)) {
                result.setException(ex);
            } else {
                result.set(v);
            }
        });

        return result;
    }
}
