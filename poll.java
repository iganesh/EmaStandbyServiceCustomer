package com.example.refinitivtreasury.service;

import com.example.refinitivtreasury.dto.TreasuryPriceDto;
import com.example.refinitivtreasury.entity.TreasuryData;
import com.example.refinitivtreasury.repository.TreasuryDataRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.DayOfWeek;
import java.util.ArrayList;
import java.util.List;

@Service
public class TreasuryDataService {

    @Autowired
    private RefinitivOmmConsumer ommConsumer;

    @Autowired
    private TreasuryDataRepository repository;

    private final List<TreasuryPriceDto> batch = new ArrayList<>();

    @PostConstruct
    public void init() {
        ommConsumer.initialize();
    }

    @PreDestroy
    public void destroy() {
        ommConsumer.shutdown();
    }

    private boolean isWithinOperatingHours() {
        LocalDateTime now = LocalDateTime.now(ZoneId.of("Asia/Kolkata"));
        LocalTime time = now.toLocalTime();
        DayOfWeek day = now.getDayOfWeek();
        return day != DayOfWeek.SATURDAY && day != DayOfWeek.SUNDAY &&
               time.isAfter(LocalTime.of(3, 0)) && time.isBefore(LocalTime.of(23, 0));
    }

    @Scheduled(cron = "0 0 3 * * MON-FRI", zone = "Asia/Kolkata")
    public void startConsumer() {
        if (isWithinOperatingHours()) {
            ommConsumer.subscribeToChain("0#USTSY=");
        }
    }

    @Scheduled(cron = "0 0 23 * * MON-FRI", zone = "Asia/Kolkata")
    public void stopConsumer() {
        ommConsumer.unsubscribeAll();
    }

    @Scheduled(fixedRate = 1000)
    @Transactional
    public void processMessages() {
        if (!isWithinOperatingHours()) {
            return;
        }

        TreasuryData data;
        while ((data = ommConsumer.pollMessage()) != null) {
            TreasuryPriceDto dto = new TreasuryPriceDto();
            dto.setRic(data.getRic());
            dto.setTimestamp(data.getTimestamp());
            dto.setBidYield(data.getBidYield());
            dto.setAskYield(data.getAskYield());
            dto.setBidPrice(data.getBidPrice());
            dto.setAskPrice(data.getAskPrice());
            dto.setUpdateType(data.getUpdateType());
            dto.setLastUpdate(data.getLastUpdate());
            batch.add(dto);

            // Process batch if it reaches a size threshold (e.g., 100)
            if (batch.size() >= 100) {
                repository.upsert(new ArrayList<>(batch));
                batch.clear();
            }
        }
    }

    @Scheduled(fixedRate = 10000) // Flush remaining batch every 10 seconds
    @Transactional
    public void flushBatch() {
        if (!isWithinOperatingHours() || batch.isEmpty()) {
            return;
        }
        repository.upsert(new ArrayList<>(batch));
        batch.clear();
    }
}


package com.example.refinitivtreasury.repository;

import com.example.refinitivtreasury.dto.TreasuryPriceDto;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import oracle.jdbc.OracleConnection;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

@Repository
public class TreasuryDataRepository {

    @PersistenceContext
    private EntityManager entityManager;

    @Transactional
    public void upsert(List<TreasuryPriceDto> prices) {
        try {
            OracleConnection oracleConnection = entityManager.unwrap(OracleConnection.class);

            // Create the Oracle record and array descriptors
            StructDescriptor structDescriptor = StructDescriptor.createDescriptor(
                "TREASURY_PRICE_T", oracleConnection);
            ArrayDescriptor arrayDescriptor = ArrayDescriptor.createDescriptor(
                "TREASURY_PRICE_TAB", oracleConnection);

            // Convert List<TreasuryPriceDto> to Oracle STRUCT array
            STRUCT[] structs = new STRUCT[prices.size()];
            for (int i = 0; i < prices.size(); i++) {
                TreasuryPriceDto price = prices.get(i);
                Object[] fields = new Object[]{
                    price.getRic(),
                    price.getTimestamp() != null ? Timestamp.valueOf(price.getTimestamp()) : null,
                    price.getBidYield(),
                    price.getAskYield(),
                    price.getBidPrice(),
                    price.getAskPrice(),
                    price.getUpdateType(),
                    price.getLastUpdate() != null ? Timestamp.valueOf(price.getLastUpdate()) : null
                };
                structs[i] = new STRUCT(structDescriptor, oracleConnection, fields);
            }
            ARRAY array = new ARRAY(arrayDescriptor, oracleConnection, structs);

            // Call the stored procedure
            String sql = "{call treasury_feeds.load_treasury_prices(?)}";
            jakarta.persistence.StoredProcedureQuery query = entityManager
                .createStoredProcedureQuery(sql)
                .registerStoredProcedureParameter(1, Object.class, jakarta.persistence.ParameterMode.IN)
                .setParameter(1, array);
            query.execute();

        } catch (SQLException e) {
            throw new RuntimeException("Failed to call stored procedure", e);
        }
    }
}


package com.example.refinitivtreasury.service;

import com.example.refinitivtreasury.dto.TreasuryPriceDto;
import com.refinitiv.ema.access.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

@Component
public class RefinitivOmmConsumer implements OmmConsumerClient {

    private static final Logger logger = LoggerFactory.getLogger(RefinitivOmmConsumer.class);

    private OmmConsumer consumer;
    private final ConcurrentLinkedQueue<TreasuryPriceDto> messageQueue = new ConcurrentLinkedQueue<>();
    private final List<Long> itemHandles = new ArrayList<>();

    public void initialize() {
        try {
            OmmConsumerConfig config = EmaFactory.createOmmConsumerConfig()
                    .host("your-refinitiv-host:14002") // Replace with your Refinitiv host
                    .username("your-username")         // Replace with your username
                    .password("your-password");        // Replace with your password
            consumer = EmaFactory.createOmmConsumer(config);
            logger.info("OmmConsumer initialized successfully");
        } catch (OmmException e) {
            logger.error("Failed to initialize OmmConsumer", e);
            throw new RuntimeException("Failed to initialize OmmConsumer", e);
        }
    }

    public void subscribeToChain(String chainRic) {
        try {
            long handle = consumer.registerClient(
                    EmaFactory.createReqMsg().serviceName("ELEKTRON_DD").name(chainRic), this);
            itemHandles.add(handle);
            logger.info("Subscribed to chain RIC: {}", chainRic);
        } catch (OmmException e) {
            logger.error("Failed to subscribe to chain RIC: {}", chainRic, e);
        }
    }

    public void unsubscribeAll() {
        for (Long handle : itemHandles) {
            try {
                consumer.unregister(handle);
                logger.info("Unsubscribed handle: {}", handle);
            } catch (OmmException e) {
                logger.error("Failed to unsubscribe handle: {}", handle, e);
            }
        }
        itemHandles.clear();
    }

    public void shutdown() {
        if (consumer != null) {
            try {
                consumer.uninitialize();
                logger.info("OmmConsumer shutdown successfully");
            } catch (OmmException e) {
                logger.error("Failed to shutdown OmmConsumer", e);
            }
        }
    }

    @Override
    public void onRefreshMsg(RefreshMsg refreshMsg, OmmConsumerEvent event) {
        logger.debug("Received refresh message for RIC: {}", refreshMsg.name());
        processMessage(refreshMsg, "REFRESH");
    }

    @Override
    public void onUpdateMsg(UpdateMsg updateMsg, OmmConsumerEvent event) {
        logger.debug("Received update message for RIC: {}", updateMsg.name());
        processMessage(updateMsg, "UPDATE");
    }

    @Override
    public void onStatusMsg(StatusMsg statusMsg, OmmConsumerEvent event) {
        logger.info("Received status message for RIC: {}, State: {}", 
                    statusMsg.name(), statusMsg.state());
    }

    private void processMessage(Msg msg, String updateType) {
        try {
            if (msg.domainType() == EmaRdm.MMT_MARKET_PRICE) {
                processMarketPrice(msg, updateType);
            } else if (msg.domainType() == EmaRdm.MMT_MARKET_BY_PRICE) {
                processChain(msg);
            }
        } catch (Exception e) {
            logger.error("Error processing message for RIC: {}", msg.name(), e);
        }
    }

    private void processMarketPrice(Msg msg, String updateType) {
        TreasuryPriceDto dto = new TreasuryPriceDto();
        dto.setRic(msg.name());
        dto.setTimestamp(LocalDateTime.now());
        dto.setUpdateType(updateType);
        dto.setLastUpdate(LocalDateTime.now());

        FieldList fieldList = msg.payload().fieldList();
        for (FieldEntry entry : fieldList) {
            // Verify field IDs with your Refinitiv data dictionary
            switch (entry.fieldId()) {
                case 22: // BID
                    dto.setBidPrice(entry.doubleValue());
                    break;
                case 25: // ASK
                    dto.setAskPrice(entry.doubleValue());
                    break;
                case 393: // YLD_1 (Bid Yield)
                    dto.setBidYield(entry.doubleValue());
                    break;
                case 396: // ASK_YLD
                    dto.setAskYield(entry.doubleValue());
                    break;
            }
        }

        // Validate data before queuing
        if (dto.getRic() != null && !dto.getRic().isEmpty()) {
            messageQueue.add(dto);
            logger.debug("Queued TreasuryPriceDto for RIC: {}", dto.getRic());
        } else {
            logger.warn("Invalid RIC in message: {}", msg.name());
        }
    }

    private void processChain(Msg msg) {
        if (msg.payload().map() != null) {
            Map map = msg.payload().map();
            for (MapEntry entry : map) {
                if (entry.key().dataType() == DataType.DataTypes.ASCII) {
                    String ric = entry.key().ascii();
                    try {
                        long handle = consumer.registerClient(
                                EmaFactory.createReqMsg().serviceName("ELEKTRON_DD").name(ric), this);
                        itemHandles.add(handle);
                        logger.info("Subscribed to chain constituent RIC: {}", ric);
                    } catch (OmmException e) {
                        logger.error("Failed to subscribe to RIC: {}", ric, e);
                    }
                }
            }
        }
    }

    public TreasuryPriceDto pollMessage() {
        return messageQueue.poll();
    }
}
