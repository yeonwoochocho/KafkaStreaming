package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import org.example.JsonUtil.JsonSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;

@Component
public class KafkaStreamsExit {
    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsExit.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaServer;
    private long lastEventTime = 0L;

    public KafkaStreams createExitStreams() {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<JsonNode> jsonSerde = new JsonSerde();

        KStream<String, JsonNode> inputStream = builder.stream("web-log-topic", Consumed.with(Serdes.String(), jsonSerde));

        String outputTopic = "zzzexit-event-topic";

        KStream<String, JsonNode> filteredInputStream = inputStream
                .filter((key, value) -> value != null && value.has("Uid") && value.get("Uid").isTextual());


        // inputStream에 대한 처리
        filteredInputStream.foreach((key, value) -> {
            long currentTimestamp = System.currentTimeMillis();
            long deviation = currentTimestamp - lastEventTime;

            if (deviation >= 100000) {
                // "이탈"로 간주되는 이벤트에 대한 작업 수행
                log.info("이탈로 표시된 이벤트: {}", value);

                // 마지막 로그 데이터 또는 해당 로그 데이터의 고객 ID를 추출
                String customerId = value.get("Uid").asText();

                // 추출한 데이터를 원하는 대상으로 전송하거나 다른 작업 수행
                log.info("마지막  로그 데이터의 고객 ID: {}", customerId);



                // 현재 이벤트의 발생 시간을 마지막 이벤트 발생 시간으로 업데이트
                lastEventTime = currentTimestamp;
            }
        });

        // 선택적으로 "이탈" 이벤트를 "output" 토픽으로 전송할 수 있습니다.
        filteredInputStream.to(outputTopic, Produced.with(Serdes.String(), jsonSerde));

        // Kafka Streams 구성
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "exit-detection-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, jsonSerde.getClass().getName());

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // 애플리케이션 종료 시 스트림 종료
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        return streams;
    }
    private long parseEventTime(String evtTimeString) {
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.ENGLISH);
            Date parsedDate = format.parse(evtTimeString);
            return parsedDate.getTime();
        } catch (ParseException e) {
            log.error("Error parsing evt_time", e);
            return 0L;
        }
    }
   }

/*

import com.fasterxml.jackson.databind.JsonNode;
import org.example.JsonUtil.JsonSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;

@Component
public class KafkaStreamsExit {
    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsExit.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaServer;

    public KafkaStreams createExitStreams() {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<JsonNode> jsonSerde = new JsonSerde();

        // inputStream 키 형식: String, 값 형식: JsonNode
        KStream<String, JsonNode> inputStream = builder.stream("web-log-topic", Consumed.with(Serdes.String(), jsonSerde));

// lastEventTimeTable 키 형식: String, 값 형식: Long
        KTable<String, Long> lastEventTimeTable = inputStream
                .filter((key, value) -> value != null && value.has("Uid") && value.get("Uid").isTextual())
                .groupByKey(Grouped.with(Serdes.String(), jsonSerde))
                .aggregate(
                        () -> 0L,
                        (key, value, aggregate) -> {
                            long evtTime = parseEventTime(value.get("evt_time").asText());
                            return Math.max(aggregate, evtTime);
                        },
                        Materialized.with(Serdes.String(), Serdes.Long())
                );

        KStream<String, JsonNode> joinedStream = inputStream.leftJoin(
                lastEventTimeTable.toStream(),
                (leftValue, rightValue) -> {
                    long currentTimestamp = System.currentTimeMillis();
                    long deviation;

                    if (leftValue != null && rightValue != null) {
                        deviation = currentTimestamp - rightValue;
                        if (deviation >= 100000) {
                            // "이탈"로 간주되는 이벤트에 대한 작업 수행
                            log.info("이탈로 표시된 이벤트: {}", leftValue);
                        }
                    } else {
                        // leftValue 또는 rightValue가 null인 경우에 대한 처리
                        log.warn("leftValue 또는 rightValue가 null입니다. leftValue: {}, rightValue: {}", leftValue, rightValue);
                        deviation = Long.MAX_VALUE; // 무한대로 설정하거나 다른 값을 사용
                    }

                    return leftValue;
                },
                Joined.with(Serdes.String(), jsonSerde, null)
        );
// inputStream을 필터링하여 이탈 감지된 데이터만 추출
        KStream<String, JsonNode> modifiedStream = inputStream
                .filter(deviationCondition)
                .selectKey((key, value) -> value.get("Uid").asText());

// 이탈로 판단된 이벤트를 outputTopic으로 전송
        modifiedStream.to("output", Produced.with(Serdes.String(), jsonSerde));

        // Kafka Streams 구성
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "exit-detection-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, jsonSerde.getClass().getName());

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // 애플리케이션 종료 시 스트림 종료
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        return streams;
    }

    private long parseEventTime(String evtTimeString) {
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.ENGLISH);
            Date parsedDate = format.parse(evtTimeString);
            return parsedDate.getTime();
        } catch (ParseException e) {
            log.error("Error parsing evt_time", e);
            return 0L;
        }
    }
}


import com.fasterxml.jackson.databind.JsonNode;
import org.example.JsonUtil.JsonSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;

@Component
public class KafkaStreamsExit {
    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsExit.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaServer;

    public KafkaStreams createExitStreams() {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<JsonNode> jsonSerde = new JsonSerde();

        KStream<String, JsonNode> inputStream = builder.stream("web-log-topic", Consumed.with(Serdes.String(), jsonSerde));

        String outputTopic = "exit-event-topic";

        // 이탈 감지 조건 정의
        Predicate<String, JsonNode> deviationCondition = (key, value) -> {
            long evtTime = parseEventTime(value.get("evt_time").asText());
            long currentTimestamp = System.currentTimeMillis();
            long deviation = currentTimestamp - evtTime;
            return deviation >= 100000; // 100초 이상 이탈 시 이탈로 처리
        };

        KStream<String, JsonNode> modifiedStream = inputStream
                .filter(deviationCondition)
                .selectKey((key, value) -> value.get("Uid").asText());

        // 새로운 키로 변경된 스트림을 outputTopic으로 전송
        modifiedStream
                .groupByKey(Grouped.with(Serdes.String(), jsonSerde)) //Uid를 기준으로 그룹화
                .reduce((value1, value2) -> {
                    // 여러 이벤트 중에서 최근 이벤트 선택 (evt_time을 기준으로 최근 이벤트 선택)
                    long time1 = value1.get("evt_time").asLong();
                    long time2 = value2.get("evt_time").asLong();
                    return (time1 > time2) ? value1 : value2;
                });
        modifiedStream
                .filter(deviationCondition).mapValues(JsonUtil::transformToSchemaFormat)
                .to(outputTopic);

        // Kafka Streams 구성
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "exit-event-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, jsonSerde.getClass().getName());

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // 애플리케이션 종료 시 스트림 종료
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        return streams;
    }

    private long parseEventTime(String evtTimeString) {
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.ENGLISH);
            Date parsedDate = format.parse(evtTimeString);
            return parsedDate.getTime();
        } catch (ParseException e) {
            log.error("Error parsing evt_time", e);
            return 0L;
        }
    }
}
*/

