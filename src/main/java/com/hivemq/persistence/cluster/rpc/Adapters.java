package com.hivemq.persistence.cluster.rpc;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.ImmutableIntArray;
import com.hivemq.cluster.rpc.MqttUserPropertiesModel;
import com.hivemq.cluster.rpc.MqttUserPropertyModel;
import com.hivemq.cluster.rpc.PublishModel;
import com.hivemq.cluster.rpc.TopicModel;
import com.hivemq.codec.encoder.mqtt5.Mqtt5PayloadFormatIndicator;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.mqtt.message.QoS;
import com.hivemq.mqtt.message.mqtt5.Mqtt5RetainHandling;
import com.hivemq.mqtt.message.mqtt5.Mqtt5UserProperties;
import com.hivemq.mqtt.message.mqtt5.MqttUserProperty;
import com.hivemq.mqtt.message.publish.PUBLISH;
import com.hivemq.mqtt.message.publish.PUBLISHFactory;
import com.hivemq.mqtt.message.subscribe.Topic;
import com.hivemq.persistence.payload.PublishPayloadPersistence;

import java.util.Objects;

public class Adapters {
    public static Topic adapt(@NotNull final TopicModel model) {
        return new Topic(model.getTopic(),
                QoS.valueOf(model.getQos()),
                model.getNoLocal(),
                model.getRetainAsPublished(),
                Mqtt5RetainHandling.fromCode(model.getRetainHandling()),
                model.getSubscriptionId());
    }

    public static TopicModel adapt(@NotNull final Topic topic) {
        return TopicModel.newBuilder()
                .setTopic(topic.getTopic())
                .setQos(topic.getQoS().getQosNumber())
                .setNoLocal(topic.isNoLocal())
                .setRetainAsPublished(topic.isRetainAsPublished())
                .setRetainHandling(topic.getRetainHandling().getCode())
                .setSubscriptionId(topic.getSubscriptionIdentifier())
                .build();
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

    public static Mqtt5UserProperties adapt(final MqttUserPropertiesModel model) {
        if (Objects.isNull(model)) {
            return Mqtt5UserProperties.NO_USER_PROPERTIES;
        }

        final ImmutableList.Builder<MqttUserProperty> builder = ImmutableList.builder();
        for (final MqttUserPropertyModel propertyModel: model.getUserPropertiesList()) {
            builder.add(new MqttUserProperty(propertyModel.getName(), propertyModel.getValue()));
        }

        return Mqtt5UserProperties.build(builder);
    }
}
