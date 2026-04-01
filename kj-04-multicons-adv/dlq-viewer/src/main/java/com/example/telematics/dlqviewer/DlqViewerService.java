package com.example.telematics.dlqviewer;

import com.example.telematics.config.AppConfig;
import com.example.telematics.model.DlqEvent;
import com.example.telematics.util.JsonUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class DlqViewerService implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DlqViewerService.class);
    private static final int MAX_RECORDS = 100;

    private final List<DlqViewerRecord> records = new ArrayList<>();
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();
    private final AtomicLong sequence = new AtomicLong(0);

    @Override
    public void run(ApplicationArguments args) {
        Thread pollThread = new Thread(this::pollLoop, "dlq-viewer-kafka-poll");
        pollThread.setDaemon(true);
        pollThread.start();
        log.info("DLQ viewer Kafka poll thread started — group={} topic={}",
                AppConfig.GROUP_DLQ_VIEWER, AppConfig.TOPIC_DLQ);
    }

    private void pollLoop() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(buildConsumerProps())) {
            consumer.subscribe(Collections.singletonList(AppConfig.TOPIC_DLQ));
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : polled) {
                    DlqEvent event = JsonUtil.fromJson(record.value(), DlqEvent.class);
                    DlqViewerRecord viewerRecord = new DlqViewerRecord(
                            sequence.incrementAndGet(), record.partition(), record.offset(), event);
                    addRecord(viewerRecord);
                    pushToSse(viewerRecord);
                }
            }
        } catch (Exception e) {
            log.error("DLQ viewer poll loop terminated unexpectedly: {}", e.getMessage(), e);
        }
    }

    private synchronized void addRecord(DlqViewerRecord record) {
        records.add(0, record);
        while (records.size() > MAX_RECORDS) {
            records.remove(records.size() - 1);
        }
    }

    private void pushToSse(DlqViewerRecord record) {
        if (emitters.isEmpty()) return;
        String payload = JsonUtil.toJson(record);
        List<SseEmitter> dead = new ArrayList<>();
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().data(payload));
            } catch (IOException e) {
                dead.add(emitter);
            }
        }
        emitters.removeAll(dead);
    }

    public synchronized List<DlqViewerRecord> getRecentRecords() {
        return List.copyOf(records);
    }

    public synchronized Map<String, Object> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("dlqCount", records.size());
        metrics.put("latestSequence", sequence.get());
        return metrics;
    }

    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(0L);
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
        return emitter;
    }

    private Properties buildConsumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, AppConfig.GROUP_DLQ_VIEWER);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "dlq-viewer-1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return props;
    }
}
