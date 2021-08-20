import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * kafka使用kerberos认证-数据消费
 *
 * @author hwang
 * @data 2021/8/19
 */
public class KafkaKerberosConsumer {
    /**
     * 日志对象
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaKerberosConsumer.class);

    public static void main(String[] args) {
        // Kerberos认证所需配置
        System.setProperty("java.security.krb5.conf", "E:\\kafka-kerberos-study\\src\\main\\resources\\krb5.conf");
        System.setProperty("java.security.auth.login.config", "E:\\kafka-kerberos-study\\src\\main\\resources\\kafka-jaas.conf");

        // kafka相关配置
        Properties props = new Properties();
        //集群地址，多个地址用"，"分隔
        props.put("bootstrap.servers", "1.15.154.243:9092");
        props.put("group.id", "1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // Kerberos认证必须添加以下三行
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.kerberos.service.name", "kafka");
        props.put("sasl.mechanism", "GSSAPI");

        // 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 订阅topic，可以为多个用,隔开，此处订阅了"test"这个topic
        consumer.subscribe(Collections.singletonList("test"));

        try {
            // 持续监听
            while (true) {
                // poll频率
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    LOGGER.info("消费数据：" + record.value());
                }
            }
        } catch (Exception e) {
            LOGGER.error("消费数据出错，原因：[]", e);
        }
    }
}
