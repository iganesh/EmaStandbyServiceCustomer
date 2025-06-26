package com.example.emasessionmanager;

import com.refinitiv.ema.access.*;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.atomic.AtomicBoolean;

@SpringBootApplication
public class SessionManagerApplication {

    public static void main(String[] args) {
        SpringApplication.run(SessionManagerApplication.class, args);
    }

    @Configuration
    public static class EmaConfig {

        @Value("${ema.active.host:localhost}")
        private String activeHost;

        @Value("${ema.active.port:14002}")
        private String activePort;

        @Value("${ema.standby.host:localhost}")
        private String standbyHost;

        @Value("${ema.standby.port:14003}")
        private String standbyPort;

        @Value("${ema.service:ELEKTRON_DD}")
        private String serviceName;

        @Value("${ema.item:IBM.N}")
        private String itemName;

        @Value("${jgroups.cluster.name:ema-cluster}")
        private String clusterName;

        @Bean
        public OmmConsumerConfig ommConsumerConfig() {
            OmmConsumerConfig config = OmmConsumerConfig.create();
            config.addChannel("Channel_Active", Map.create()
                    .add(MapEntry.create("ChannelType", OmmConsumerConfig.ChannelTypeEnum.RSSL_SOCKET.getValue())
                            .add(MapEntry.create("Host", activeHost))
                            .add(MapEntry.create("Port", activePort))));
            config.addChannel("Channel_Standby", Map.create()
                    .add(MapEntry.create("ChannelType", OmmConsumerConfig.ChannelTypeEnum.RSSL_SOCKET.getValue())
                            .add(MapEntry.create("Host", standbyHost))
                            .add(MapEntry.create("Port", standbyPort))));
            config.addChannelSet("ChannelSet_1", new String[]{"Channel_Active", "Channel_Standby"});
            config.operationModel(OmmConsumerConfig.OperationModel.USER_DISPATCH);
            return config;
        }

        @Bean
        public JChannel jChannel() throws Exception {
            JChannel channel = new JChannel("udp.xml"); // Use default UDP configuration
            return channel;
        }

        @Bean
        public JGroupsCoordinator jGroupsCoordinator(JChannel jChannel) {
            return new JGroupsCoordinator(jChannel, clusterName);
        }

        @Bean
        public SessionManager sessionManager(OmmConsumerConfig config, JGroupsCoordinator coordinator) {
            return new SessionManager(config, serviceName, itemName, coordinator);
        }
    }

    public static class JGroupsCoordinator implements Receiver {

        private final JChannel channel;
        private final String clusterName;
        private final AtomicBoolean isCoordinator = new AtomicBoolean(false);
        private final String hostId = System.getenv("HOSTNAME") != null ? System.getenv("HOSTNAME") : "host-" + System.currentTimeMillis();
        private volatile boolean running = true;

        public JGroupsCoordinator(JChannel channel, String clusterName) {
            this.channel = channel;
            this.clusterName = clusterName;
        }

        public void start() throws Exception {
            channel.setReceiver(this);
            channel.connect(clusterName);
            checkCoordinator();
        }

        public boolean isCoordinator() {
            return isCoordinator.get();
        }

        public void stop() {
            running = false;
            channel.disconnect();
            channel.close();
        }

        @Override
        public void viewAccepted(View newView) {
            System.out.println("Host " + hostId + " received new view: " + newView);
            checkCoordinator();
        }

        private void checkCoordinator() {
            View view = channel.getView();
            if (view != null && !view.getMembers().isEmpty()) {
                boolean isNewCoordinator = channel.getAddress().equals(view.getCoord());
                isCoordinator.set(isNewCoordinator);
                System.out.println("Host " + hostId + " is " + (isNewCoordinator ? "coordinator" : "non-coordinator"));
            }
        }

        @Override
        public void receive(Message msg) {
            // Optional: Handle messages if needed for additional coordination
        }
    }

    public static class SessionManager implements OmmConsumerClient {

        private final OmmConsumerConfig config;
        private final String serviceName;
        private final String itemName;
        private final JGroupsCoordinator coordinator;
        private OmmConsumer consumer;
        private final AtomicBoolean isActive = new AtomicBoolean(false);
        private long itemHandle;
        private final String hostId = System.getenv("HOSTNAME") != null ? System.getenv("HOSTNAME") : "host-" + System.currentTimeMillis();
        private static final long DISPATCH_INTERVAL = 1000; // 1 second

        public SessionManager(OmmConsumerConfig config, String serviceName, String itemName, JGroupsCoordinator coordinator) {
            this.config = config;
            this.serviceName = serviceName;
            this.itemName = itemName;
            this.coordinator = coordinator;
        }

        @PostConstruct
        public void init() throws Exception {
            coordinator.start();
            new Thread(this::coordinateSession).start();
        }

        private void coordinateSession() {
            while (!Thread.interrupted()) {
                try {
                    if (coordinator.isCoordinator() && !isActive.get()) {
                        startConsumer();
                    } else if (!coordinator.isCoordinator() && isActive.get()) {
                        stopConsumer();
                    }

                    if (isActive.get() && consumer != null) {
                        consumer.dispatch(DISPATCH_INTERVAL);
                    }

                    Thread.sleep(DISPATCH_INTERVAL);
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
                        .channelSet("ChannelSet_1"), this);
                itemHandle = consumer.registerClient(ReqMsg.create()
                        .serviceName(serviceName)
                        .name(itemName), this);
                isActive.set(true);
                System.out.println("Host " + hostId + " is active consumer on ChannelSet_1 (Active: " + config.getConfig().get("Channel_Active", "Host") + ":" + config.getConfig().get("Channel_Active", "Port") + ", Standby: " + config.getConfig().get("Channel_Standby", "Host") + ":" + config.getConfig().get("Channel_Standby", "Port") + ")");
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
            coordinator.stop();
            System.out.println("Host " + hostId + " shut down");
        }
    }
}