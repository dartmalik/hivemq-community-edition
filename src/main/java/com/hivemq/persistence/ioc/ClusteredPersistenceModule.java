/*
 * Copyright 2019-present HiveMQ GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hivemq.persistence.ioc;

import com.google.inject.Injector;
import com.hivemq.bootstrap.ioc.SingletonModule;
import com.hivemq.bootstrap.ioc.lazysingleton.LazySingleton;
import com.hivemq.cluster.ClusteringService;
import com.hivemq.cluster.persistence.ClusteredClientQueuePersistenceImpl;
import com.hivemq.cluster.persistence.ClusteredClientSessionPersistenceImpl;
import com.hivemq.cluster.persistence.ClusteredClientSubscriptionPersistenceImpl;
import com.hivemq.cluster.provider.ClusteringServiceProvider;
import com.hivemq.cluster.provider.GRPCServerProvider;
import com.hivemq.cluster.rpc.*;
import com.hivemq.configuration.service.PersistenceConfigurationService;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.mqtt.topic.tree.LocalTopicTree;
import com.hivemq.mqtt.topic.tree.TopicTreeImpl;
import com.hivemq.persistence.ChannelPersistence;
import com.hivemq.persistence.ChannelPersistenceImpl;
import com.hivemq.persistence.PersistenceStartup;
import com.hivemq.persistence.clientqueue.ClientQueuePersistence;
import com.hivemq.persistence.clientsession.ClientSessionPersistence;
import com.hivemq.persistence.clientsession.ClientSessionSubscriptionPersistence;
import com.hivemq.persistence.clientsession.SharedSubscriptionService;
import com.hivemq.persistence.clientsession.SharedSubscriptionServiceImpl;
import com.hivemq.persistence.ioc.provider.local.IncomingMessageFlowPersistenceLocalProvider;
import com.hivemq.persistence.local.IncomingMessageFlowLocalPersistence;
import com.hivemq.persistence.payload.PublishPayloadNoopPersistenceImpl;
import com.hivemq.persistence.payload.PublishPayloadPersistence;
import com.hivemq.persistence.payload.PublishPayloadPersistenceImpl;
import com.hivemq.persistence.qos.IncomingMessageFlowPersistence;
import com.hivemq.persistence.qos.IncomingMessageFlowPersistenceImpl;
import com.hivemq.persistence.retained.RetainedMessagePersistence;
import com.hivemq.persistence.retained.RetainedMessagePersistenceProvider;

import javax.inject.Singleton;

/**
 * @author Dominik Obermaier
 */
class ClusteredPersistenceModule extends SingletonModule<Class<ClusteredPersistenceModule>> {

    private final @NotNull Injector persistenceInjector;
    private final @NotNull PersistenceConfigurationService persistenceConfigurationService;

    public ClusteredPersistenceModule(
            @NotNull final Injector persistenceInjector,
            @NotNull final PersistenceConfigurationService persistenceConfigurationService) {
        super(ClusteredPersistenceModule.class);
        this.persistenceInjector = persistenceInjector;
        this.persistenceConfigurationService = persistenceConfigurationService;
    }

    @Override
    protected void configure() {
        /* Local */
        if (persistenceConfigurationService.getMode() == PersistenceConfigurationService.PersistenceMode.FILE) {
            install(new LocalPersistenceFileModule(persistenceInjector));
        } else {
            install(new LocalPersistenceMemoryModule(persistenceInjector));
        }

        /* Retained Message */
        bind(RetainedMessagePersistence.class).toProvider(RetainedMessagePersistenceProvider.class)
                .in(LazySingleton.class);

        /* Channel */
        bind(ChannelPersistence.class).to(ChannelPersistenceImpl.class).in(Singleton.class);

        /* Client Session */
        bind(ClusteredClientSessionPersistenceImpl.class).in(LazySingleton.class);
        bind(ClientSessionPersistence.class).to(ClusteredClientSessionPersistenceImpl.class);

        /* Client Session Queue */
        bind(ClientQueuePersistence.class)
                .to(ClusteredClientQueuePersistenceImpl.class)
                .in(LazySingleton.class);

        /* Client Session Subscription */
        bind(ClusteredClientSubscriptionPersistenceImpl.class).in(LazySingleton.class);
        bind(ClientSessionSubscriptionPersistence.class).to(ClusteredClientSubscriptionPersistenceImpl.class);

        /* Clustering */
        bind(ClusteringService.class).toProvider(ClusteringServiceProvider.class).in(Singleton.class);
        bind(GRPCChannelRegistry.class).in(LazySingleton.class);

        /* Cluster RPC */
        bind(SessionPersistenceServiceGrpc.SessionPersistenceServiceImplBase.class)
                .to(ClusteredClientSessionPersistenceImpl.class);
        bind(QueuePersistenceServiceGrpc.QueuePersistenceServiceImplBase.class)
                .to(ClusteredClientQueuePersistenceImpl.QueuePersistenceServiceImpl.class)
                .in(LazySingleton.class);
        bind(SubscriptionPersistenceServiceGrpc.SubscriptionPersistenceServiceImplBase.class)
                .to(ClusteredClientSubscriptionPersistenceImpl.class);
        bind(GRPCServer.class).toProvider(GRPCServerProvider.class).in(Singleton.class);

        /* Client Session Sub */
        bind(SharedSubscriptionService.class).to(SharedSubscriptionServiceImpl.class).in(LazySingleton.class);

        /* Topic Tree */
        bind(LocalTopicTree.class).to(TopicTreeImpl.class).in(Singleton.class);

        /* QoS Handling */
        bind(IncomingMessageFlowPersistence.class).to(IncomingMessageFlowPersistenceImpl.class);
        bind(IncomingMessageFlowLocalPersistence.class).toProvider(IncomingMessageFlowPersistenceLocalProvider.class)
                .in(LazySingleton.class);

        /* Payload Persistence */
        if (persistenceConfigurationService.getMode() == PersistenceConfigurationService.PersistenceMode.IN_MEMORY) {
            bind(PublishPayloadPersistence.class).toInstance(persistenceInjector.getInstance(PublishPayloadNoopPersistenceImpl.class));
        } else {
            bind(PublishPayloadPersistence.class).toInstance(persistenceInjector.getInstance(PublishPayloadPersistence.class));
            bind(PublishPayloadPersistenceImpl.class).toInstance(persistenceInjector.getInstance(
                    PublishPayloadPersistenceImpl.class));
        }

        /* Startup */
        bind(PersistenceStartup.class).toInstance(persistenceInjector.getInstance(PersistenceStartup.class));
    }
}
