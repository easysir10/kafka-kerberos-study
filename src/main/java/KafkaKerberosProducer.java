import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Objects;
import java.util.Properties;

/**
 * kafka使用kerberos认证-数据推送
 *
 * @author hwang
 * @data 2021/8/16
 */
public class KafkaKerberosProducer {
    /**
     * 日志对象
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaKerberosProducer.class);
    /**
     * kerberos配置文件
     */
    public static final String KRB5_CONF = "krb5.conf";

    public static final String KAFKA_JAAS_CONF = "kafka-jaas.conf";
    public static final String BOOTSTRAP_SERVERS = "server:9092";
    public static final String TOPIC = "test";
    private static final long COUNT = 5;

    public static void main(String[] args) {
        // Kerberos认证必须添加
        System.setProperty("java.security.krb5.conf", KRB5_CONF);
        System.setProperty("java.security.auth.login.config", KAFKA_JAAS_CONF);

        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // Kerberos认证必须添加以下三行
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.kerberos.service.name", "kafka");
        props.put("sasl.mechanism", "GSSAPI");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        int i = 1;

        do {
            String message = "{\"id\":" + i + ",\"date\":" + new Date() + "}";
            LOGGER.info("推送数据:{}", message);
            // 推送数据
            producer.send(new ProducerRecord<>(TOPIC, message));
        } while (i++ <= COUNT);
    }
}
