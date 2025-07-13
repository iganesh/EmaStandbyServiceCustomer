/* src/test/java/com/example/refinitivtreasury/RefinitivTreasuryOmmApplicationTests.java */
package com.example.refinitivtreasury;

import com.example.refinitivtreasury.config.ThreadPoolConfig;
import com.example.refinitivtreasury.dto.TreasuryPriceDto;
import com.example.refinitivtreasury.entity.TreasuryData;
import com.example.refinitivtreasury.repository.TreasuryDataRepository;
import com.example.refinitivtreasury.service.LeaderElectionService;
import com.example.refinitivtreasury.service.RefinitivOmmConsumer;
import com.example.refinitivtreasury.service.TreasuryDataService;
import com.refinitiv.ema.access.*;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.scheduling.support.ScheduledMethodRunnable;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.StoredProcedureQuery;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class RefinitivTreasuryOmmApplicationTests {

    @Mock
    private MeterRegistry meterRegistry;

    @Mock
    private ResourceLoader resourceLoader;

    @Mock
    private Environment environment;

    @Mock
    private OmmConsumer ommConsumer;

    @Mock
    private OmmConsumerConfig ommConsumerConfig;

    @Mock
    private EntityManager entityManager;

    @Mock
    private CuratorFramework curatorFramework;

    @Mock
    private LeaderLatch leaderLatch;

    @Mock
    private TreasuryDataRepository treasuryDataRepository;

    @InjectMocks
    private RefinitivOmmConsumer refinitivOmmConsumer;

    @InjectMocks
    private LeaderElectionService leaderElectionService;

    @InjectMocks
    private TreasuryDataService treasuryDataService;

    @InjectMocks
    private ThreadPoolConfig threadPoolConfig;

    @InjectMocks
    private RefinitivTreasuryOmmApplication refinitivTreasuryOmmApplication;

    @Mock
    private Counter counter;

    @Mock
    private Timer timer;

    @Mock
    private Gauge gauge;

    @Mock
    private Logger logger;

    @BeforeEach
    void setUp() throws Exception {
        // Mock MeterRegistry counters and gauges
        when(meterRegistry.counter(anyString(), any())).thenReturn(counter);
        when(meterRegistry.timer(anyString(), any())).thenReturn(timer);
        when(meterRegistry.gauge(anyString(), any(), any())).thenReturn(gauge);

        // Set default timezone to Asia/Kolkata
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Kolkata"));

        // Mock EmaConfig.xml
        Resource resource = mock(Resource.class);
        File mockFile = File.createTempFile("EmaConfig", ".xml");
        String xmlContent = """
            <?xml version="1.0" encoding="UTF-8"?>
            <EmaConfig>
                <Consumer>
                    <Name value="Consumer_1"/>
                    <Channel value="Channel_1"/>
                </Consumer>
                <ChannelGroup>
                    <Channel>
                        <Name value="Channel_1"/>
                        <ChannelType value="ChannelType::RSSL_SOCKET"/>
                        <Host value="mock-host"/>
                        <Port value="14002"/>
                        <CompressionType value="CompressionType::None"/>
                        <GuaranteedOutputBuffers value="5000"/>
                        <ConnectionPingTimeout value="30000"/>
                    </Channel>
                </ChannelGroup>
            </EmaConfig>
        """;
        Files.write(mockFile.toPath(), xmlContent.getBytes());
        when(resource.exists()).thenReturn(true);
        when(resource.getFile()).thenReturn(mockFile);
        when(resourceLoader.getResource("classpath:EmaConfig.xml")).thenReturn(resource);

        // Mock environment properties
        when(environment.getProperty("refinitiv.host")).thenReturn("mock-host");
        when(environment.getProperty("refinitiv.port")).thenReturn("14002");

        // Mock Refinitiv EMA
        when(EmaFactory.createOmmConsumerConfig()).thenReturn(ommConsumerConfig);
        when(ommConsumerConfig.config(any(DataBuffer.class))).thenReturn(ommConsumerConfig);
        when(EmaFactory.createOmmConsumer(any())).thenReturn(ommConsumer);

        // Mock logger
        Field loggerField = RefinitivOmmConsumer.class.getDeclaredField("logger");
        loggerField.setAccessible(true);
        loggerField.set(null, logger);
        loggerField = LeaderElectionService.class.getDeclaredField("logger");
        loggerField.setAccessible(true);
        loggerField.set(null, logger);
        loggerField = TreasuryDataService.class.getDeclaredField("logger");
        loggerField.setAccessible(true);
        loggerField.set(null, logger);
        loggerField = TreasuryDataRepository.class.getDeclaredField("logger");
        loggerField.setAccessible(true);
        loggerField.set(null, logger);
        loggerField = ThreadPoolConfig.class.getDeclaredField("logger");
        loggerField.setAccessible(true);
        loggerField.set(null, logger);
    }

    // RefinitivTreasuryOmmApplication Tests
    @Test
    void testInit() {
        refinitivTreasuryOmmApplication.init();
        assertThat(TimeZone.getDefault()).isEqualTo(TimeZone.getTimeZone("Asia/Kolkata"));
    }

    @Test
    void testMain() {
        String[] args = new String[]{};
        SpringApplication springApplication = mock(SpringApplication.class);
        try (MockedStatic<SpringApplication> mocked = mockStatic(SpringApplication.class)) {
            mocked.when(() -> SpringApplication.run(RefinitivTreasuryOmmApplication.class, args))
                  .thenReturn(null);
            RefinitivTreasuryOmmApplication.main(args);
            mocked.verify(() -> SpringApplication.run(RefinitivTreasuryOmmApplication.class, args));
        }
    }

    // TreasuryData Tests
    @Test
    void testTreasuryDataGettersAndSetters() {
        TreasuryData data = new TreasuryData();
        LocalDateTime now = LocalDateTime.now();
        data.setRic("RIC1");
        data.setTimestamp(now);
        data.setBidYield(1.5);
        data.setAskYield(1.6);
        data.setBidPrice(100.0);
        data.setAskPrice(101.0);
        data.setUpdateType("UPDATE");
        data.setLastUpdate(now);

        assertThat(data.getRic()).isEqualTo("RIC1");
        assertThat(data.getTimestamp()).isEqualTo(now);
        assertThat(data.getBidYield()).isEqualTo(1.5);
        assertThat(data.getAskYield()).isEqualTo(1.6);
        assertThat(data.getBidPrice()).isEqualTo(100.0);
        assertThat(data.getAskPrice()).isEqualTo(101.0);
        assertThat(data.getUpdateType()).isEqualTo("UPDATE");
        assertThat(data.getLastUpdate()).isEqualTo(now);
    }

    // TreasuryPriceDto Tests
    @Test
    void testTreasuryPriceDtoGettersAndSetters() {
        TreasuryPriceDto dto = new TreasuryPriceDto();
        LocalDateTime now = LocalDateTime.now();
        dto.setRic("RIC1");
        dto.setTimestamp(now);
        dto.setBidYield(1.5);
        dto.setAskYield(1.6);
        dto.setBidPrice(100.0);
        dto.setAskPrice(101.0);
        dto.setUpdateType("UPDATE");
        dto.setLastUpdate(now);

        assertThat(dto.getRic()).isEqualTo("RIC1");
        assertThat(dto.getTimestamp()).isEqualTo(now);
        assertThat(dto.getBidYield()).isEqualTo(1.5);
        assertThat(dto.getAskYield()).isEqualTo(1.6);
        assertThat(dto.getBidPrice()).isEqualTo(100.0);
        assertThat(dto.getAskPrice()).isEqualTo(101.0);
        assertThat(dto.getUpdateType()).isEqualTo("UPDATE");
        assertThat(dto.getLastUpdate()).isEqualTo(now);
    }

    // TreasuryDataRepository Tests
    @Test
    void testUpsertSuccess() throws SQLException {
        List<TreasuryPriceDto> prices = List.of(createSampleDto());
        OracleConnection oracleConnection = mock(OracleConnection.class);
        StructDescriptor structDescriptor = mock(StructDescriptor.class);
        ArrayDescriptor arrayDescriptor = mock(ArrayDescriptor.class);
        STRUCT struct = mock(STRUCT.class);
        ARRAY array = mock(ARRAY.class);
        StoredProcedureQuery query = mock(StoredProcedureQuery.class);

        when(entityManager.unwrap(OracleConnection.class)).thenReturn(oracleConnection);
        when(oracleConnection.createStruct(anyString(), any())).thenReturn(struct);
        when(oracleConnection.createARRAY(anyString(), any())).thenReturn(array);
        when(entityManager.createStoredProcedureQuery(anyString())).thenReturn(query);
        when(query.registerStoredProcedureParameter(anyInt(), any(), any())).thenReturn(query);
        when(query.setParameter(anyInt(), any())).thenReturn(query);

        treasuryDataRepository.upsert(prices);

        verify(query).execute();
        verify(counter, never()).increment();
    }

    @Test
    void testUpsertSQLException() throws SQLException {
        List<TreasuryPriceDto> prices = List.of(createSampleDto());
        OracleConnection oracleConnection = mock(OracleConnection.class);
        when(entityManager.unwrap(OracleConnection.class)).thenReturn(oracleConnection);
        when(oracleConnection.createStruct(anyString(), any())).thenThrow(new SQLException("DB Error"));

        assertThrows(RuntimeException.class, () -> treasuryDataRepository.upsert(prices));
        verify(logger).error(contains("Failed to upsert"), anyInt(), anyString(), anyString(), any());
    }

    @Test
    void testUpsertFallback() {
        List<TreasuryPriceDto> prices = List.of(createSampleDto());
        Exception e = new RuntimeException("Retry failed");

        treasuryDataRepository.upsertFallback(prices, e);

        verify(logger).error(contains("Failed to upsert"), anyInt(), anyString());
    }

    // RefinitivOmmConsumer Tests
    @Test
    void testInitializeSuccess() {
        refinitivOmmConsumer.initialize();
        verify(ommConsumerConfig).config(any(DataBuffer.class));
        verify(ommConsumer).dispatch(anyLong());
        verify(logger).info(contains("OmmConsumer initialized successfully"));
    }

    @Test
    void testInitializeXmlNotFound() throws IOException {
        when(resourceLoader.getResource("classpath:EmaConfig.xml")).thenReturn(mock(Resource.class));
        assertThrows(RuntimeException.class, () -> refinitivOmmConsumer.initialize());
        verify(logger).error(contains("EmaConfig.xml not found"));
    }

    @Test
    void testInitializeMissingProperties() {
        when(environment.getProperty("refinitiv.host")).thenReturn(null);
        assertThrows(RuntimeException.class, () -> refinitivOmmConsumer.initialize());
        verify(logger).error(contains("refinitiv.host or refinitiv.port not set"));
    }

    @Test
    void testInitializeOmmException() throws IOException {
        when(EmaFactory.createOmmConsumer(any())).thenThrow(new OmmException("EMA Error"));
        refinitivOmmConsumer.initialize();
        verify(logger, times(3)).error(contains("Failed to initialize OmmConsumer"));
        verify(logger).error(contains("OmmConsumer initialization failed after 3 attempts"));
    }

    @Test
    void testDispatchEventsSuccess() {
        ReflectionTestUtils.setField(refinitivOmmConsumer, "consumer", ommConsumer);
        refinitivOmmConsumer.dispatchEvents();
        verify(ommConsumer).dispatch(DISPATCH_TIMEOUT_US);
        verify(counter).increment();
    }

    @Test
    void testDispatchEventsConsumerNull() {
        refinitivOmmConsumer.dispatchEvents();
        verify(logger).warn(contains("OmmConsumer not initialized"));
        verify(counter, never()).increment();
    }

    @Test
    void testDispatchEventsOmmException() {
        ReflectionTestUtils.setField(refinitivOmmConsumer, "consumer", ommConsumer);
        doThrow(new OmmException("Dispatch Error")).when(ommConsumer).dispatch(anyLong());
        refinitivOmmConsumer.dispatchEvents();
        verify(logger).error(contains("Failed to dispatch events"));
        verify(counter).increment();
    }

    @Test
    void testSubscribeToChainSuccess() {
        ReflectionTestUtils.setField(refinitivOmmConsumer, "consumer", ommConsumer);
        when(ommConsumer.registerClient(any(), any())).thenReturn(123L);
        refinitivOmmConsumer.subscribeToChain("0#USTSY=");
        assertThat(refinitivOmmConsumer.getActiveSubscriptions()).isEqualTo(1);
        verify(counter).increment();
    }

    @Test
    void testSubscribeToChainAlreadySubscribed() {
        ReflectionTestUtils.setField(refinitivOmmConsumer, "consumer", ommConsumer);
        refinitivOmmConsumer.subscribeToChain("0#USTSY=");
        refinitivOmmConsumer.subscribeToChain("0#USTSY=");
        verify(counter, times(1)).increment();
    }

    @Test
    void testSubscribeToChainOmmException() {
        ReflectionTestUtils.setField(refinitivOmmConsumer, "consumer", ommConsumer);
        when(ommConsumer.registerClient(any(), any())).thenThrow(new OmmException("Subscribe Error"));
        refinitivOmmConsumer.subscribeToChain("0#USTSY=");
        assertThat(refinitivOmmConsumer.getActiveSubscriptions()).isEqualTo(0);
        verify(logger).error(contains("Failed to subscribe to chain"));
    }

    @Test
    void testUnsubscribeAllSuccess() {
        ReflectionTestUtils.setField(refinitivOmmConsumer, "consumer", ommConsumer);
        ReflectionTestUtils.invokeMethod(refinitivOmmConsumer, "itemHandles", new ArrayList<>(List.of(123L)));
        refinitivOmmConsumer.unsubscribeAll();
        verify(ommConsumer).unregister(123L);
        verify(counter).increment(-1.0);
    }

    @Test
    void testUnsubscribeAllOmmException() {
        ReflectionTestUtils.setField(refinitivOmmConsumer, "consumer", ommConsumer);
        ReflectionTestUtils.invokeMethod(refinitivOmmConsumer, "itemHandles", new ArrayList<>(List.of(123L)));
        doThrow(new OmmException("Unsubscribe Error")).when(ommConsumer).unregister(anyLong());
        refinitivOmmConsumer.unsubscribeAll();
        verify(logger).error(contains("Failed to unsubscribe"));
    }

    @Test
    void testShutdownSuccess() {
        ReflectionTestUtils.setField(refinitivOmmConsumer, "consumer", ommConsumer);
        refinitivOmmConsumer.shutdown();
        verify(ommConsumer).uninitialize();
        verify(logger).info(contains("OmmConsumer shutdown successfully"));
    }

    @Test
    void testShutdownOmmException() {
        ReflectionTestUtils.setField(refinitivOmmConsumer, "consumer", ommConsumer);
        doThrow(new OmmException("Shutdown Error")).when(ommConsumer).uninitialize();
        refinitivOmmConsumer.shutdown();
        verify(logger).error(contains("Failed to shutdown OmmConsumer"));
    }

    @Test
    void testOnRefreshMsg() {
        RefreshMsg refreshMsg = mock(RefreshMsg.class);
        OmmConsumerEvent event = mock(OmmConsumerEvent.class);
        when(refreshMsg.name()).thenReturn("RIC1");
        when(refreshMsg.domainType()).thenReturn(EmaRdm.MMT_MARKET_PRICE);
        refinitivOmmConsumer.onRefreshMsg(refreshMsg, event);
        verify(logger).info(contains("Received refresh message"));
    }

    @Test
    void testOnUpdateMsg() {
        UpdateMsg updateMsg = mock(UpdateMsg.class);
        OmmConsumerEvent event = mock(OmmConsumerEvent.class);
        when(updateMsg.name()).thenReturn("RIC1");
        when(updateMsg.domainType()).thenReturn(EmaRdm.MMT_MARKET_PRICE);
        refinitivOmmConsumer.onUpdateMsg(updateMsg, event);
        verify(logger).info(contains("Received update message"));
    }

    @Test
    void testOnStatusMsg() {
        StatusMsg statusMsg = mock(StatusMsg.class);
        OmmConsumerEvent event = mock(OmmConsumerEvent.class);
        when(statusMsg.name()).thenReturn("RIC1");
        when(statusMsg.state()).thenReturn(mock(OmmState.class));
        refinitivOmmConsumer.onStatusMsg(statusMsg, event);
        verify(logger).info(contains("Received status message"));
    }

    @Test
    void testProcessMessageMarketPrice() throws Exception {
        Msg msg = mock(Msg.class);
        FieldList fieldList = mock(FieldList.class);
        FieldEntry entry = mock(FieldEntry.class);
        when(msg.domainType()).thenReturn(EmaRdm.MMT_MARKET_PRICE);
        when(msg.name()).thenReturn("RIC1");
        when(msg.payload()).thenReturn(mock(Payload.class));
        when(msg.payload().fieldList()).thenReturn(fieldList);
        when(fieldList.iterator()).thenReturn(List.of(entry).iterator());
        when(entry.fieldId()).thenReturn(22).thenReturn(25).thenReturn(393).thenReturn(396);
        when(entry.doubleValue()).thenReturn(100.0).thenReturn(101.0).thenReturn(1.5).thenReturn(1.6);

        Method processMessage = RefinitivOmmConsumer.class.getDeclaredMethod("processMessage", Msg.class, String.class);
        processMessage.setAccessible(true);
        processMessage.invoke(refinitivOmmConsumer, msg, "UPDATE");

        assertThat(refinitivOmmConsumer.pollMessage()).isNotNull();
        verify(timer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    void testProcessMessageChain() throws Exception {
        Msg msg = mock(Msg.class);
        Map map = mock(Map.class);
        MapEntry entry = mock(MapEntry.class);
        Data key = mock(Data.class);
        when(msg.domainType()).thenReturn(EmaRdm.MMT_MARKET_BY_PRICE);
        when(msg.name()).thenReturn("0#USTSY=");
        when(msg.payload()).thenReturn(mock(Payload.class));
        when(msg.payload().map()).thenReturn(map);
        when(map.iterator()).thenReturn(List.of(entry).iterator());
        when(entry.key()).thenReturn(key);
        when(key.dataType()).thenReturn(DataType.DataTypes.ASCII);
        when(key.ascii()).thenReturn("RIC2");
        ReflectionTestUtils.setField(refinitivOmmConsumer, "consumer", ommConsumer);
        when(ommConsumer.registerClient(any(), any())).thenReturn(123L);

        Method processMessage = RefinitivOmmConsumer.class.getDeclaredMethod("processMessage", Msg.class, String.class);
        processMessage.setAccessible(true);
        processMessage.invoke(refinitivOmmConsumer, msg, "UPDATE");

        assertThat(refinitivOmmConsumer.getActiveSubscriptions()).isEqualTo(1);
        verify(counter).increment();
    }

    @Test
    void testProcessMessageInvalidDomain() throws Exception {
        Msg msg = mock(Msg.class);
        when(msg.domainType()).thenReturn(999);
        when(msg.name()).thenReturn("RIC1");

        Method processMessage = RefinitivOmmConsumer.class.getDeclaredMethod("processMessage", Msg.class, String.class);
        processMessage.setAccessible(true);
        processMessage.invoke(refinitivOmmConsumer, msg, "UPDATE");

        verify(logger).debug(contains("Ignored message"));
    }

    @Test
    void testProcessMessageException() throws Exception {
        Msg msg = mock(Msg.class);
        when(msg.domainType()).thenThrow(new RuntimeException("Processing error"));

        Method processMessage = RefinitivOmmConsumer.class.getDeclaredMethod("processMessage", Msg.class, String.class);
        processMessage.setAccessible(true);
        processMessage.invoke(refinitivOmmConsumer, msg, "UPDATE");

        verify(logger).error(contains("Error processing message"));
    }

    @Test
    void testProcessMarketPriceInvalidRic() throws Exception {
        Msg msg = mock(Msg.class);
        when(msg.name()).thenReturn("");
        when(msg.payload()).thenReturn(mock(Payload.class));
        when(msg.payload().fieldList()).thenReturn(mock(FieldList.class));

        Method processMarketPrice = RefinitivOmmConsumer.class.getDeclaredMethod("processMarketPrice", Msg.class, String.class);
        processMarketPrice.setAccessible(true);
        processMarketPrice.invoke(refinitivOmmConsumer, msg, "UPDATE");

        verify(logger).warn(contains("Invalid RIC"));
        verify(timer, never()).record(anyLong(), any());
    }

    @Test
    void testProcessChainException() throws Exception {
        Msg msg = mock(Msg.class);
        Map map = mock(Map.class);
        when(msg.payload()).thenReturn(mock(Payload.class));
        when(msg.payload().map()).thenReturn(map);
        when(map.iterator()).thenThrow(new RuntimeException("Chain error"));

        Method processChain = RefinitivOmmConsumer.class.getDeclaredMethod("processChain", Msg.class);
        processChain.setAccessible(true);
        processChain.invoke(refinitivOmmConsumer, msg);

        verify(logger).error(contains("Error processing message"));
    }

    @Test
    void testPollMessage() {
        TreasuryPriceDto dto = createSampleDto();
        ReflectionTestUtils.invokeMethod(refinitivOmmConsumer, "messageQueue", new ArrayBlockingQueue<>(10000)).offer(dto);
        assertThat(refinitivOmmConsumer.pollMessage()).isEqualTo(dto);
        verify(timer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    void testPollMessageEmpty() {
        assertThat(refinitivOmmConsumer.pollMessage()).isNull();
        verify(timer, never()).record(anyLong(), any());
    }

    @Test
    void testMessageQueueSize() {
        ReflectionTestUtils.invokeMethod(refinitivOmmConsumer, "messageQueue", new ArrayBlockingQueue<>(10000)).offer(createSampleDto());
        assertThat(refinitivOmmConsumer.messageQueueSize()).isEqualTo(1);
    }

    @Test
    void testGetActiveSubscriptions() {
        ReflectionTestUtils.invokeMethod(refinitivOmmConsumer, "subscribedRics", ConcurrentHashMap.newKeySet()).add("RIC1");
        assertThat(refinitivOmmConsumer.getActiveSubscriptions()).isEqualTo(1);
    }

    // LeaderElectionService Tests
    @Test
    void testLeaderElectionServiceInitSuccess() {
        when(curatorFramework.getConnectionStateListenable()).thenReturn(mock(org.apache.curator.framework.listen.Listenable.class));
        leaderElectionService.init();
        verify(curatorFramework).start();
        verify(leaderLatch).start();
        verify(logger).info(contains("Leader election initialized"));
    }

    @Test
    void testLeaderElectionServiceInitException() {
        when(curatorFramework.getConnectionStateListenable()).thenThrow(new RuntimeException("ZK Error"));
        assertThrows(RuntimeException.class, () -> leaderElectionService.init());
        verify(logger).error(contains("Failed to initialize leader election"));
    }

    @Test
    void testLeaderElectionServiceDestroySuccess() {
        leaderElectionService.destroy();
        verify(leaderLatch).close();
        verify(curatorFramework).close();
        verify(logger).info(contains("Leader election shutdown"));
    }

    @Test
    void testLeaderElectionServiceDestroyException() {
        doThrow(new RuntimeException("Close Error")).when(leaderLatch).close();
        leaderElectionService.destroy();
        verify(logger).error(contains("Failed to shutdown leader election"));
    }

    @Test
    void testIsLeader() {
        when(leaderLatch.hasLeadership()).thenReturn(true);
        assertThat(leaderElectionService.isLeader()).isTrue();
        verify(logger).debug(contains("Checked leadership status"));
    }

    @Test
    void testLogLeadershipStatusSuccess() throws Exception {
        when(leaderLatch.hasLeadership()).thenReturn(true);
        when(leaderLatch.getParticipants()).thenReturn(List.of());
        leaderElectionService.logLeadershipStatus();
        verify(logger).info(contains("Periodic leadership check"));
    }

    @Test
    void testLogLeadershipStatusException() throws Exception {
        when(leaderLatch.getParticipants()).thenThrow(new RuntimeException("ZK Error"));
        leaderElectionService.logLeadershipStatus();
        verify(logger).error(contains("Failed to check leadership status"));
    }

    @Test
    void testGetLeadershipStatus() throws Exception {
        when(leaderLatch.hasLeadership()).thenReturn(true);
        when(leaderLatch.getParticipants()).thenReturn(List.of());
        String status = leaderElectionService.getLeadershipStatus();
        assertThat(status).contains("Is Leader: true");
    }

    // TreasuryDataService Tests
    @Test
    void testTreasuryDataServiceInitLeaderOperating() {
        ReflectionTestUtils.setField(treasuryDataService, "leaderElectionService", leaderElectionService);
        ReflectionTestUtils.setField(treasuryDataService, "ommConsumer", refinitivOmmConsumer);
        when(leaderElectionService.isLeader()).thenReturn(true);
        // Mock operating hours to true
        Method isWithinOperatingHours = TreasuryDataService.class.getDeclaredMethod("isWithinOperatingHours");
        isWithinOperatingHours.setAccessible(true);
        try (MockedStatic<LocalDateTime> mocked = mockStatic(LocalDateTime.class)) {
            mocked.when(() -> LocalDateTime.now(ZoneId.of("Asia/Kolkata")))
                  .thenReturn(LocalDateTime.of(2025, 7, 14, 10, 0)); // Monday 10 AM
            treasuryDataService.init();
            verify(refinitivOmmConsumer).subscribeToChain("0#USTSY=");
            verify(logger).info(contains("Subscribed to chain"));
        }
    }

    @Test
    void testTreasuryDataServiceInitNonLeader() {
        ReflectionTestUtils.setField(treasuryDataService, "leaderElectionService", leaderElectionService);
        ReflectionTestUtils.setField(treasuryDataService, "ommConsumer", refinitivOmmConsumer);
        when(leaderElectionService.isLeader()).thenReturn(false);
        treasuryDataService.init();
        verify(refinitivOmmConsumer, never()).subscribeToChain(anyString());
        verify(logger).warn(contains("Skipped startup subscription"));
    }

    @Test
    void testTreasuryDataServiceDestroy() {
        ReflectionTestUtils.setField(treasuryDataService, "ommConsumer", refinitivOmmConsumer);
        treasuryDataService.destroy();
        verify(refinitivOmmConsumer).shutdown();
        verify(logger).info(contains("TreasuryDataService shutdown"));
    }

    @Test
    void testIsWithinOperatingHoursMonday() throws Exception {
        Method isWithinOperatingHours = TreasuryDataService.class.getDeclaredMethod("isWithinOperatingHours");
        isWithinOperatingHours.setAccessible(true);
        try (MockedStatic<LocalDateTime> mocked = mockStatic(LocalDateTime.class)) {
            mocked.when(() -> LocalDateTime.now(ZoneId.of("Asia/Kolkata")))
                  .thenReturn(LocalDateTime.of(2025, 7, 14, 10, 0)); // Monday 10 AM
            boolean result = (boolean) isWithinOperatingHours.invoke(treasuryDataService);
            assertThat(result).isTrue();
        }
    }

    @Test
    void testIsWithinOperatingHoursSunday() throws Exception {
        Method isWithinOperatingHours = TreasuryDataService.class.getDeclaredMethod("isWithinOperatingHours");
        isWithinOperatingHours.setAccessible(true);
        try (MockedStatic<LocalDateTime> mocked = mockStatic(LocalDateTime.class)) {
            mocked.when(() -> LocalDateTime.now(ZoneId.of("Asia/Kolkata")))
                  .thenReturn(LocalDateTime.of(2025, 7, 13, 19, 0)); // Sunday 7 PM
            boolean result = (boolean) isWithinOperatingHours.invoke(treasuryDataService);
            assertThat(result).isFalse();
        }
    }

    @Test
    void testStartConsumerLeaderOperating() throws Exception {
        ReflectionTestUtils.setField(treasuryDataService, "leaderElectionService", leaderElectionService);
        ReflectionTestUtils.setField(treasuryDataService, "ommConsumer", refinitivOmmConsumer);
        when(leaderElectionService.isLeader()).thenReturn(true);
        Method isWithinOperatingHours = TreasuryDataService.class.getDeclaredMethod("isWithinOperatingHours");
        isWithinOperatingHours.setAccessible(true);
        try (MockedStatic<LocalDateTime> mocked = mockStatic(LocalDateTime.class)) {
            mocked.when(() -> LocalDateTime.now(ZoneId.of("Asia/Kolkata")))
                  .thenReturn(LocalDateTime.of(2025, 7, 14, 10, 0));
            treasuryDataService.startConsumer();
            verify(refinitivOmmConsumer).subscribeToChain("0#USTSY=");
            verify(counter).increment();
        }
    }

    @Test
    void testStartConsumerNonLeader() {
        ReflectionTestUtils.setField(treasuryDataService, "leaderElectionService", leaderElectionService);
        when(leaderElectionService.isLeader()).thenReturn(false);
        treasuryDataService.startConsumer();
        verify(refinitivOmmConsumer, never()).subscribeToChain(anyString());
        verify(counter).increment();
    }

    @Test
    void testStopConsumerLeader() {
        ReflectionTestUtils.setField(treasuryDataService, "leaderElectionService", leaderElectionService);
        ReflectionTestUtils.setField(treasuryDataService, "ommConsumer", refinitivOmmConsumer);
        when(leaderElectionService.isLeader()).thenReturn(true);
        treasuryDataService.stopConsumer();
        verify(refinitivOmmConsumer).unsubscribeAll();
        verify(counter).increment();
    }

    @Test
    void testStopConsumerNonLeader() {
        ReflectionTestUtils.setField(treasuryDataService, "leaderElectionService", leaderElectionService);
        when(leaderElectionService.isLeader()).thenReturn(false);
        treasuryDataService.stopConsumer();
        verify(refinitivOmmConsumer, never()).unsubscribeAll();
        verify(counter).increment();
    }

    @Test
    void testProcessMessagesOperatingLeader() {
        ReflectionTestUtils.setField(treasuryDataService, "leaderElectionService", leaderElectionService);
        ReflectionTestUtils.setField(treasuryDataService, "ommConsumer", refinitivOmmConsumer);
        ReflectionTestUtils.setField(treasuryDataService, "repository", treasuryDataRepository);
        when(leaderElectionService.isLeader()).thenReturn(true);
        when(refinitivOmmConsumer.pollMessage()).thenReturn(createSampleDto()).thenReturn(null);
        try (MockedStatic<LocalDateTime> mocked = mockStatic(LocalDateTime.class)) {
            mocked.when(() -> LocalDateTime.now(ZoneId.of("Asia/Kolkata")))
                  .thenReturn(LocalDateTime.of(2025, 7, 14, 10, 0));
            treasuryDataService.processMessages();
            verify(treasuryDataRepository, never()).upsert(anyList());
            verify(counter).increment();
        }
    }

    @Test
    void testProcessMessagesBatchFull() {
        ReflectionTestUtils.setField(treasuryDataService, "leaderElectionService", leaderElectionService);
        ReflectionTestUtils.setField(treasuryDataService, "ommConsumer", refinitivOmmConsumer);
        ReflectionTestUtils.setField(treasuryDataService, "repository", treasuryDataRepository);
        when(leaderElectionService.isLeader()).thenReturn(true);
        TreasuryPriceDto dto = createSampleDto();
        when(refinitivOmmConsumer.pollMessage()).thenAnswer(invocation -> {
            List<TreasuryPriceDto> batch = (List<TreasuryPriceDto>) ReflectionTestUtils.getField(treasuryDataService, "batch");
            if (batch.size() < 1000) {
                batch.add(dto);
                return dto;
            }
            return null;
        });
        try (MockedStatic<LocalDateTime> mocked = mockStatic(LocalDateTime.class)) {
            mocked.when(() -> LocalDateTime.now(ZoneId.of("Asia/Kolkata")))
                  .thenReturn(LocalDateTime.of(2025, 7, 14, 10, 0));
            treasuryDataService.processMessages();
            verify(treasuryDataRepository).upsert(anyList());
            verify(counter).increment();
        }
    }

    @Test
    void testProcessMessagesNonOperating() {
        ReflectionTestUtils.setField(treasuryDataService, "leaderElectionService", leaderElectionService);
        when(leaderElectionService.isLeader()).thenReturn(true);
        try (MockedStatic<LocalDateTime> mocked = mockStatic(LocalDateTime.class)) {
            mocked.when(() -> LocalDateTime.now(ZoneId.of("Asia/Kolkata")))
                  .thenReturn(LocalDateTime.of(2025, 7, 13, 19, 0));
            treasuryDataService.processMessages();
            verify(refinitivOmmConsumer, never()).pollMessage();
            verify(counter).increment();
        }
    }

    @Test
    void testFlushBatchOperatingLeader() {
        ReflectionTestUtils.setField(treasuryDataService, "leaderElectionService", leaderElectionService);
        ReflectionTestUtils.setField(treasuryDataService, "repository", treasuryDataRepository);
        when(leaderElectionService.isLeader()).thenReturn(true);
        ReflectionTestUtils.invokeMethod(treasuryDataService, "batch", new ArrayList<>()).add(createSampleDto());
        try (MockedStatic<LocalDateTime> mocked = mockStatic(LocalDateTime.class)) {
            mocked.when(() -> LocalDateTime.now(ZoneId.of("Asia/Kolkata")))
                  .thenReturn(LocalDateTime.of(2025, 7, 14, 10, 0));
            treasuryDataService.flushBatch();
            verify(treasuryDataRepository).upsert(anyList());
            verify(counter).increment();
        }
    }

    @Test
    void testFlushBatchEmpty() {
        ReflectionTestUtils.setField(treasuryDataService, "leaderElectionService", leaderElectionService);
        when(leaderElectionService.isLeader()).thenReturn(true);
        try (MockedStatic<LocalDateTime> mocked = mockStatic(LocalDateTime.class)) {
            mocked.when(() -> LocalDateTime.now(ZoneId.of("Asia/Kolkata")))
                  .thenReturn(LocalDateTime.of(2025, 7, 14, 10, 0));
            treasuryDataService.flushBatch();
            verify(treasuryDataRepository, never()).upsert(anyList());
            verify(counter).increment();
        }
    }

    @Test
    void testTestScheduler() {
        treasuryDataService.testScheduler();
        verify(counter).increment();
        verify(logger).info(contains("testScheduler invoked"));
    }

    @Test
    void testTriggerProcessMessages() {
        ReflectionTestUtils.setField(treasuryDataService, "leaderElectionService", leaderElectionService);
        ReflectionTestUtils.setField(treasuryDataService, "ommConsumer", refinitivOmmConsumer);
        when(leaderElectionService.isLeader()).thenReturn(true);
        String result = treasuryDataService.triggerProcessMessages();
        assertThat(result).isEqualTo("processMessages triggered");
        verify(counter).increment();
    }

    @Test
    void testTriggerFlushBatch() {
        ReflectionTestUtils.setField(treasuryDataService, "leaderElectionService", leaderElectionService);
        when(leaderElectionService.isLeader()).thenReturn(true);
        String result = treasuryDataService.triggerFlushBatch();
        assertThat(result).isEqualTo("flushBatch triggered");
        verify(counter).increment();
    }

    @Test
    void testTriggerStartConsumer() {
        ReflectionTestUtils.setField(treasuryDataService, "leaderElectionService", leaderElectionService);
        ReflectionTestUtils.setField(treasuryDataService, "ommConsumer", refinitivOmmConsumer);
        when(leaderElectionService.isLeader()).thenReturn(true);
        String result = treasuryDataService.triggerStartConsumer();
        assertThat(result).isEqualTo("startConsumer triggered");
        verify(counter).increment();
    }

    // ThreadPoolConfig Tests
    @Test
    void testConfigureTasks() {
        ScheduledTaskRegistrar taskRegistrar = mock(ScheduledTaskRegistrar.class);
        ScheduledMethodRunnable runnable = mock(ScheduledMethodRunnable.class);
        when(taskRegistrar.getScheduledTasks()).thenReturn(new HashSet<>(List.of(runnable)));
        when(runnable.getMethod()).thenReturn(RefinitivOmmConsumer.class.getDeclaredMethod("dispatchEvents"));
        when(runnable.getTarget()).thenReturn(refinitivOmmConsumer);
        threadPoolConfig.configureTasks(taskRegistrar);
        verify(taskRegistrar).setTaskScheduler(any(ThreadPoolTaskScheduler.class));
        verify(taskRegistrar).afterPropertiesSet();
        verify(logger).info(contains("Registered scheduled task"));
    }

    private TreasuryPriceDto createSampleDto() {
        TreasuryPriceDto dto = new TreasuryPriceDto();
        dto.setRic("RIC1");
        dto.setTimestamp(LocalDateTime.now());
        dto.setBidYield(1.5);
        dto.setAskYield(1.6);
        dto.setBidPrice(100.0);
        dto.setAskPrice(101.0);
        dto.setUpdateType("UPDATE");
        dto.setLastUpdate(LocalDateTime.now());
        return dto;
    }
}

```
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    
    <groupId>com.example</groupId>
    <artifactId>refinitiv-treasury-omm</artifactId>
    <version>0.0.1-SNAPSHOT</version>
    <name>refinitiv-treasury-omm</name>
    <description>Spring Boot OMM application for Refinitiv Treasury data</description>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>2.7.18</version>
        <relativePath/>
    </parent>

    <properties>
        <java.version>21</java.version>
        <junit-jupiter.version>5.9.3</junit-jupiter.version>
        <mockito.version>5.4.0</mockito.version>
        <jacoco.version>0.8.10</jacoco.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-jpa</artifactId>
        </dependency>
        <dependency>
            <groupId>com.oracle.database.jdbc</groupId>
            <artifactId>ojdbc8</artifactId>
            <version>21.7.0.0</version>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-registry-prometheus</artifactId>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>1.7.36</version>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <version>1.2.13</version>
        </dependency>
        <dependency>
            <groupId>org.apache.curator</groupId>
            <artifactId>curator-recipes</artifactId>
            <version>5.2.1</version>
        </dependency>
        <dependency>
            <groupId>org.springframework.retry</groupId>
            <artifactId>spring-retry</artifactId>
            <version>1.3.4</version>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-aop</artifactId>
        </dependency>
        <dependency>
            <groupId>com.refinitiv.ema</groupId>
            <artifactId>ema</artifactId>
            <version>3.6.7.0</version>
            <scope>system</scope>
            <systemPath>${project.basedir}/lib/ema.jar</systemPath>
        </dependency>
        <!-- Test Dependencies -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter-api</artifactId>
            <version>${junit-jupiter.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter-engine</artifactId>
            <version>${junit-jupiter.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.mockito</groupId>
            <artifactId>mockito-core</artifactId>
            <version>${mockito.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.mockito</groupId>
            <artifactId>mockito-junit-jupiter</artifactId>
            <version>${mockito.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
            <version>3.24.2</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>3.1.2</version>
                <configuration>
                    <includes>
                        <include>**/*Tests.java</include>
                    </includes>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.jacoco</groupId>
                <artifactId>jacoco-maven-plugin</artifactId>
                <version>${jacoco.version}</version>
                <executions>
                    <execution>
                        <id>prepare-agent</id>
                        <goals>
                            <goal>prepare-agent</goal>
                        </goals>
                    </execution>
                    <execution>
                        <id>report</id>
                        <phase>test</phase>
                        <goals>
                            <goal>report</goal>
                        </goals>
                        <configuration>
                            <outputDirectory>${project.build.directory}/jacoco-report</outputDirectory>
                        </configuration>
                    </execution>
                    <execution>
                        <id>check</id>
                        <goals>
                            <goal>check</goal>
                        </goals>
                        <configuration>
                            <rules>
                                <rule>
                                    <element>BUNDLE</element>
                                    <limits>
                                        <limit>
                                            <counter>LINE</counter>
                                            <value>COVEREDRATIO</value>
                                            <minimum>1.0</minimum>
                                        </limit>
                                        <limit>
                                            <counter>BRANCH</counter>
                                            <value>COVEREDRATIO</value>
                                            <minimum>1.0</minimum>
                                        </limit>
                                    </limits>
                                </rule>
                            </rules>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```
