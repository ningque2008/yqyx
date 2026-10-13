package com.yqyx.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Kafka 消费失败处理配置。
 *
 * <p>消费端不再吞掉异常，失败后由 SeekToCurrentErrorHandler 接管：
 * 1. 重试 3 次，每次间隔 1 秒；
 * 2. 仍未成功则投递到 {topic}.DLT 死信 topic，并打日志便于人工排查；
 * 3. 成功才 ack，保证至少一次投递。</p>
 *
 * <p>注意：本项目 Spring Boot 为 2.3.12，配套 spring-kafka 为 2.5.x，
 * 该版本使用 SeekToCurrentErrorHandler；DefaultErrorHandler 需要
 * spring-kafka 2.8+（Spring Boot 2.6+），在当前版本并不存在。</p>
 */
@Slf4j
@Configuration
public class KafkaErrorHandlerConfig {

    /**
     * 消费失败处理器：重试 + 死信。
     */
    @Bean
    public SeekToCurrentErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> {
                    log.error("Kafka 消息进入死信队列，topic = {}, offset = {}, value = {}",
                            record.topic(), record.offset(), record.value(), ex);
                    return new TopicPartition(record.topic() + ".DLT", record.partition());
                });
        return new SeekToCurrentErrorHandler(recoverer, new FixedBackOff(1000L, 3L));
    }

    /**
     * 覆盖默认的监听容器工厂：复用 Spring Boot 的配置（如 manual ack 模式），
     * 额外把上面的错误处理器挂上，确保所有 @KafkaListener 都生效。
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory,
            SeekToCurrentErrorHandler kafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setErrorHandler(kafkaErrorHandler);
        return factory;
    }
}
