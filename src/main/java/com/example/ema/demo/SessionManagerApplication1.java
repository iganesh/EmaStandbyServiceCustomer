package com.example.emasessionmanager;

import com.refinitiv.ema.access.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.*;
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

        @Value("${multicast.group:239.255.0.1}")
        private String multicastGroup;

        @Value("${multicast.port:5000}")
        private int multicastPort;

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
        public MulticastCoordinator multicastCoordinator() throws SocketException {
            return new MulticastCoordinator(multicastGroup, multicastPort);
        }

        @Bean
        public SessionManager sessionManager(OmmConsumerConfig config, MulticastCoordinator coordinator) {
            return new SessionManager(config, serviceName, itemName, coordinator);
        }
    }

    public static class MulticastCoordinator {

        private final MulticastSocket socket;
        private final InetAddress group;
        private final int port;
        private final AtomicBoolean running = new AtomicBoolean(true);

        public MulticastCoordinator(String multicastGroup, int multicastPort) throws SocketException {
            try {
                this.group = InetAddress.getByName(multicastGroup);
                this.port = multicastPort;
                this.socket = new MulticastSocket(port);
                socket.joinGroup(new InetSocketAddress(group, port), NetworkInterface.getByInetAddress(InetAddress.getLocalHost()));
            } catch (Exception e) {
                throw new SocketException("Failed to initialize multicast: " + e.getMessage());
            }
        }

        public void sendHeartbeat(String hostId) {
            try {
                String message = hostId + ":" + System.currentTimeMillis();
                byte[] buffer = message.getBytes();
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length, group, port);
                socket.send(packet);
            } catch (IOException e) {
                System.err.println("Failed to send heartbeat: " + e.getMessage());
            }
        }

        public String receiveHeartbeat() {
            try {
                byte[] buffer = new byte[256];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.setSoTimeout(1000); // 1-second timeout
                socket.receive(packet);
                return new String(packet.getData(), 0, packet.getLength());
            } catch (SocketTimeoutException e) {
                return null;
            } catch (IOException e) {
                System.err.println("Failed to receive heartbeat: " + e.getMessage());
                return null;
            }
        }

        public void close() {
            running.set(false);
            socket.leaveGroup(group, NetworkInterface.getByInetAddress(InetAddress.getLocalHost()));
            socket.close();
        }
    }

    public static class SessionManager implements Omm rulingConsumerClient {

        private final OmmConsumerConfig config;
        private final String serviceName;
        private final String itemName;
        private final MulticastCoordinator coordinator;
        private OmmConsumer consumer;
        private final AtomicBoolean isActive = new AtomicBoolean(false);
        private long itemHandle;
        private final String hostId = System.getenv("HOSTNAME") != null ? System.getenv("HOSTNAME") : "host-" + System.currentTimeMillis();
        private static final long HEARTBEAT_INTERVAL = 5000; // 5 seconds
        private static final long HEARTBEAT_TIMEOUT = 15000; // 15 seconds
        private volatile String lastHeartbeatHost;
        private volatile long lastHeartbeatTime;

        public SessionManager(OmmConsumerConfig config, String serviceName, String itemName, MulticastCoordinator coordinator) {
            this.config = config;
            this.serviceName = serviceName;
            this.itemName = itemName;
            this.coordinator = coordinator;
        }

        @PostConstruct
        public void init() {
            new Thread(this::coordinateSession).start();
        }

        private void coordinateSession() {
            while (!Thread.interrupted()) {
                try {
                    // Check for active host via heartbeat
                    String heartbeat = coordinator.receiveHeartbeat();
                    if (heartbeat != null) {
                        String[] parts = heartbeat.split(":");
                        if (parts.length == 2) {
                            lastHeartbeatHost = parts[0];
                            lastHeartbeatTime = Long.parseLong(parts[1]);
                        }
                    }

                    boolean isOtherHostActive = lastHeartbeatHost != null && !lastHeartbeatHost.equals(hostId) &&
                            System.currentTimeMillis() - lastHeartbeatTime <= HEARTBEAT_TIMEOUT;

                    if (!isOtherHostActive && !isActive.get()) {
                        // No active host or heartbeat timed out, become active
                        startConsumer();
                    } else if (isOtherHostActive && isActive.get()) {
                        // Another host is active, become inactive
                        stopConsumer();
                    }

                    if (isActive.get()) {
                        coordinator.sendHeartbeat(hostId);
                        if (consumer != null) {
                            consumer.dispatch(1000);
                        }
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
            }
        }

        @PreDestroy
        public void shutdown() {
            stopConsumer();
            coordinator.close();
            System.out.println("Host " + hostId + " shut down");
        }
    }
}