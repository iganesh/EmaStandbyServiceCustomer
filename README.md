# EmaStandbyServiceCustomerProcess Flow: Refinitiv Treasury OMM Application
The application processes U.S. Treasury market data (e.g., US2YT=RR) in the following steps:

Initialization:

Time Zone: RefinitivTreasuryOmmApplication sets Asia/Kolkata timezone.
EMA Setup: RefinitivOmmConsumer.initialize loads EmaConfig.xml, configures OmmConsumer with refinitiv.host and refinitiv.port, and retries up to 3 times on failure (5s delay).
Leadership Check: TreasuryDataService.init checks if the node is the leader via LeaderElectionService.isLeader and within operating hours (3 AM–11 PM IST, Mon–Fri).
Chain Subscription: If leader and operating, subscribes to 0#USTSY= chain, which includes US2YT=RR.


Leader Election:

LeaderElectionService uses ZooKeeper (localhost:2181) with Apache Curator to elect a leader via LeaderLatch at /refinitiv/leader.
Only the leader node processes data; others remain idle but monitor leadership status every 5s.


Market Data Subscription:

RefinitivOmmConsumer.subscribeToChain("0#USTSY=") registers the chain RIC with OmmConsumer.
processChain processes chain messages (MMT_MARKET_BY_PRICE), extracts constituent RICs (e.g., US2YT=RR), and subscribes to each using consumer.registerClient.
Subscriptions are tracked in subscribedRics and itemHandles.


Data Processing:

Message Handling: RefinitivOmmConsumer handles RefreshMsg and UpdateMsg (MMT_MARKET_PRICE) via onRefreshMsg and onUpdateMsg.
Field Extraction: processMarketPrice extracts fields (BID: 22, ASK: 25, YLD_1: 393, ASK_YLD: 396) for US2YT=RR, populates TreasuryPriceDto, and adds it to ArrayBlockingQueue (capacity: 10,000).
Queue Metrics: Tracks offer latency and dropped messages (queue_dropped_messages_total).


Batch Processing:

TreasuryDataService.processMessages (every 1s) polls the queue, adds TreasuryPriceDto to a batch (max 1000), and calls TreasuryDataRepository.upsert when full.
flushBatch (every 25s) upserts any remaining batch if the node is the leader and operating.


Database Storage:

TreasuryDataRepository.upsert converts TreasuryPriceDto to Oracle TREASURY_PRICE_T objects, creates a TREASURY_PRICE_TAB array, and calls treasury_feeds.load_treasury_prices.
Retries up to 3 times on SQLException (backoff: 500ms–2s).
Commits data to Treasury_data table.


Exception Handling:

Exceptions in initialize, dispatchEvents, subscribeToChain, processMessage, processChain, upsert, etc., are caught and logged.
ExceptionEmailService.sendExceptionEmail sends emails with exception details (message, stack trace, timestamp, method) to app.error.email.to (e.g., Gmail SMTP).


Scheduling:

startConsumer (cron: 0 * 3-22 * * MON-FRI): Subscribes to 0#USTSY= if leader and operating.
stopConsumer (cron: 0 0 23 * * MON-FRI): Unsubscribes all RICs if leader.
processMessages (every 1s): Processes queue data.
flushBatch (every 25s): Flushes remaining batch.
testScheduler (every 5s): Logs for testing.
logLeadershipStatus (every 5s): Logs leadership status.


Monitoring:

Metrics (/actuator/prometheus) track queue size, subscriptions, latency, and task invocations.
Logs provide detailed debugging (logging.level.com.example.refinitivtreasury=DEBUG).


Shutdown:

TreasuryDataService.destroy calls RefinitivOmmConsumer.shutdown to uninitialize OmmConsumer.
LeaderElectionService.destroy closes ZooKeeper connections.



Example for US2YT=RR:

At 9:47 PM IST, Thursday, the leader node subscribes to 0#USTSY=, registers US2YT=RR, processes its market price messages, queues TreasuryPriceDto, batches up to 1000 records, and upserts to Oracle. Exceptions (e.g., OmmException) trigger emails.
