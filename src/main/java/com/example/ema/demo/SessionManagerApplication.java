package com.example.emasessionmanager;

import com.refinitiv.ema.access.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
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
        public StringRedisTemplate redisTemplate(RedisConnectionFactory connectionFactory) {
            return new StringRedisTemplate(connectionFactory);
        }

        @Bean
        public SessionManager sessionManager(OmmConsumerConfig config, StringRedisTemplate redisTemplate) {
            return new SessionManager(config, serviceName, itemName, redisTemplate);
        }
    }

    public static class SessionManager implements OmmConsumerClient {

        private final OmmConsumerConfig config;
        private final String serviceName;
        private final String itemName;
        private final StringRedisTemplate redisTemplate;
        private OmmConsumer consumer;
        private final AtomicBoolean isActive = new AtomicBoolean(false);
        private long itemHandle;
        private final String hostId = System.getenv("HOSTNAME") != null ? System.getenv("HOSTNAME") : "host-" + System.currentTimeMillis();
        private static final String ACTIVE_HOST_KEY = "ema:activeHost";
        private static final String HEARTBEAT_KEY = "ema:heartbeat:";
        private static final long HEARTBEAT_INTERVAL = 5000; // 5 seconds
        private static final long HEARTBEAT_TIMEOUT = 15000; // 15 seconds

        public SessionManager(OmmConsumerConfig config, String serviceName, String itemName, StringRedisTemplate redisTemplate) {
            this.config = config;
            this.serviceName = serviceName;
            this.itemName = itemName;
            this.redisTemplate = redisTemplate;
        }

        @PostConstruct
        public void init() {
            new Thread(this::coordinateSession).start();
        }

        private void coordinateSession() {
            while (!Thread.interrupted()) {
                try {
                    // Attempt to become active
                    Boolean acquired = redisTemplate.opsForValue().setIfAbsent(ACTIVE_HOST_KEY, hostId, 30, TimeUnit.SECONDS);
                    if (Boolean.TRUE.equals(acquired)) {
                        if (!isActive.get()) {
                            startConsumer();
                        }
                        // Update heartbeat
                        redisTemplate.opsForValue().set(HEARTBEAT_KEY + hostId, String.valueOf(System.currentTimeMillis()), HEARTBEAT_TIMEOUT, TimeUnit.MILLISECONDS);
                    } else {
                        // Check if active host is alive
                        String activeHost = redisTemplate.opsForValue().get(ACTIVE_HOST_KEY);
                        if (activeHost == null || !isActiveHostAlive(activeHost)) {
                            // Active host is down, try to take over
                            redisTemplate.delete(ACTIVE_HOST_KEY);
                            if (isActive.get()) {
                                stopConsumer();
                            }
                            continue;
                        }
                        if (isActive.get()) {
                            stopConsumer();
                        }
                    }
                    if (isActive.get() && consumer != null) {
                        consumer.dispatch(1000);
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

        private boolean isActiveHostAlive(String activeHost) {
            if (activeHost == null) {
                return false;
            }
            String heartbeat = redisTemplate.opsForValue().get(HEARTBEAT_KEY + activeHost);
            if (heartbeat == null) {
                return false;
            }
            try {
                long lastHeartbeat = Long.parseLong(heartbeat);
                return System.currentTimeMillis() - lastHeartbeat <= HEARTBEAT_TIMEOUT;
            } catch (NumberFormatException e) {
                return false;
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
                redisTemplate.delete(ACTIVE_HOST_KEY);
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
                redisTemplate.delete(ACTIVE_HOST_KEY);
            }
        }

        @PreDestroy
        public void shutdown() {
            stopConsumer();
            redisTemplate.delete(HEARTBEAT_KEY + hostId);
            if (Boolean.TRUE.equals(redisTemplate.hasKey(ACTIVE_HOST_KEY)) && hostId.equals(redisTemplate.opsForValue().get(ACTIVE_HOST_KEY))) {
                redisTemplate.delete(ACTIVE_HOST_KEY);
            }
            System.out.println("Host " + hostId + " shut down");
        }
    }
}