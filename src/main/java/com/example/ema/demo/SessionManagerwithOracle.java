package com.example.emasessionmanager;

import com.refinitiv.ema.access.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.transaction.Transactional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class SessionManagerApplication {

    public static void main(String[] args) {
        SpringApplication.run(SessionManagerApplication.class, args);
    }

    @Configuration
    public static class EmaConfig {

        @Value("${ema.host:localhost}")
        private String host;

        @Value("${ema.port:14002}")
        private String port;

        @Value("${ema.service:ELEKTRON_DD}")
        private String serviceName;

        @Value("${ema.item:IBM.N}")
        private String itemName;

        @Bean
        public OmmConsumerConfig ommConsumerConfig() {
            OmmConsumerConfig config = OmmConsumerConfig.create();
            config.addChannel("Channel_1", Map.create()
                    .add(MapEntry.create("ChannelType", OmmConsumerConfig.ChannelTypeEnum.RSSL_SOCKET.getValue())
                            .add(MapEntry.create("Host", host))
                            .add(MapEntry.create("Port", port))));
            config.operationModel(OmmConsumerConfig.OperationModel.USER_DISPATCH);
            return config;
        }

        @Bean
        public SessionManager sessionManager(OmmConsumerConfig config, ActiveHostRepository activeHostRepository) {
            return new SessionManager(config, serviceName, itemName, activeHostRepository);
        }
    }

    @Entity
    @Table(name = "EMA_ACTIVE_HOST")
    public static class ActiveHost {
        @Id
        private String hostId;
        private long heartbeatTimestamp;

        // Getters and setters
        public String getHostId() {
            return hostId;
        }

        public void setHostId(String hostId) {
            this.hostId = hostId;
        }

        public long getHeartbeatTimestamp() {
            return heartbeatTimestamp;
        }

        public void setHeartbeatTimestamp(long heartbeatTimestamp) {
            this.heartbeatTimestamp = heartbeatTimestamp;
        }
    }

    @Repository
    public interface ActiveHostRepository extends JpaRepository<ActiveHost, String> {

        @Transactional
        @Modifying
        @Query("UPDATE ActiveHost SET hostId = :hostId, heartbeatTimestamp = :timestamp WHERE hostId = :currentHostId OR heartbeatTimestamp < :staleTimestamp")
        int updateActiveHost(String hostId, long timestamp, String currentHostId, long staleTimestamp);

        @Query("SELECT a FROM ActiveHost a WHERE a.hostId = :hostId")
        ActiveHost findByHostId(String hostId);
    }

    public static class SessionManager implements OmmConsumerClient {

        private final OmmConsumerConfig config;
        private final String serviceName;
        private final String itemName;
        private final ActiveHostRepository activeHostRepository;
        private OmmConsumer consumer;
        private final AtomicBoolean isActive = new AtomicBoolean(false);
        private long itemHandle;
        private final String hostId = System.getenv("HOSTNAME") != null ? System.getenv("HOSTNAME") : "host-" + System.currentTimeMillis();
        private static final long HEARTBEAT_INTERVAL = 5000; // 5 seconds
        private static final long HEARTBEAT_TIMEOUT = 15000; // 15 seconds
        private static final String NONE_HOST = "NONE";

        public SessionManager(OmmConsumerConfig config, String serviceName, String itemName, ActiveHostRepository activeHostRepository) {
            this.config = config;
            this.serviceName = serviceName;
            this.itemName = itemName;
            this.activeHostRepository = activeHostRepository;
        }

        @PostConstruct
        public void init() {
            new Thread(this::coordinateSession).start();
        }

        private void coordinateSession() {
            while (!Thread.interrupted()) {
                try {
                    // Attempt to become active
                    long currentTime = System.currentTimeMillis();
                    long staleTime = currentTime - HEARTBEAT_TIMEOUT;
                    int updated = activeHostRepository.updateActiveHost(hostId, currentTime, hostId, staleTime);
                    if (updated > 0 && !isActive.get()) {
                        // Successfully became active
                        startConsumer();
                    } else {
                        // Check if another host is active and alive
                        ActiveHost activeHost = activeHostRepository.findByHostId(hostId);
                        if (activeHost == null || activeHost.getHostId().equals(NONE_HOST) || activeHost.getHeartbeatTimestamp() < staleTime) {
                            // No active host or heartbeat is stale, try again
                            continue;
                        }
                        if (isActive.get()) {
                            stopConsumer();
                        }
                    }

                    if (isActive.get() && consumer != null) {
                        consumer.dispatch(1000);
                        // Update heartbeat
                        activeHostRepository.updateActiveHost(hostId, System.currentTimeMillis(), hostId, staleTime);
                    }

                    Thread.sleep(HEARTBEAT_INTERVAL);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    System.err.println("Error in coordination for host " + hostId + ": " + e.getMessage());
                }
            }
        }

        private void startConsumer() {
            try {
                consumer = OmmConsumer.create(config.consumerName("Consumer_" + hostId)
                        .channel("Channel_1"), this);
                itemHandle = consumer.registerClient(ReqMsg.create()
                        .serviceName(serviceName)
                        .name(itemName), this);
                isActive.set(true);
                System.out.println("Host " + hostId + " is active consumer on " + config.getConfig().get("Channel_1", "Host") + ":" + config.getConfig().get("Channel_1", "Port"));
            } catch (OmmException e) {
                System.err.println("Failed to start consumer for host " + hostId + ": " + e.getMessage());
                isActive.set(false);
                activeHostRepository.updateActiveHost(NONE_HOST, System.currentTimeMillis(), hostId, 0);
            }
        }

        private void stopConsumer() {
            if (consumer != null) {
                consumer.uninitialize();
                consumer = null;
                isActive.set(false);
                System.out.println("Host " + hostId + " stopped consumer");
            }
        }

        @Override
        public void onRefreshMsg(RefreshMsg refreshMsg, OmmConsumerEvent event) {
            System.out.println("Host " + hostId + " received RefreshMsg: " + refreshMsg);
            if (refreshMsg.hasState() && refreshMsg.state().streamState() == OmmState.StreamState.OPEN &&
                    refreshMsg.state().dataState() == OmmState.DataState.OK) {
                System.out.println("Host " + hostId + " connection is up");
            }
        }

        @Override
        public void onUpdateMsg(UpdateMsg updateMsg, OmmConsumerEvent event) {
            System.out.println("Host " + hostId + " received UpdateMsg: " + updateMsg);
        }

        @Override
        public void onStatusMsg(StatusMsg statusMsg, OmmConsumerEvent event) {
            System.out.println("Host " + hostId + " received StatusMsg: " + statusMsg);
            if (statusMsg.hasState() && statusMsg.state().streamState() != OmmState.StreamState.OPEN) {
                System.err.println("Host " + hostId + " connection down: " + statusMsg.state());
                stopConsumer();
                activeHostRepository.updateActiveHost(NONE_HOST, System.currentTimeMillis(), hostId, 0);
            }
        }

        ទ

        System: @PreDestroy
        public void shutdown() {
            stopConsumer();
            activeHostRepository.updateActiveHost(NONE_HOST, System.currentTimeMillis(), hostId, 0);
            System.out.println("Host " + hostId + " shut down");
        }
    }
}