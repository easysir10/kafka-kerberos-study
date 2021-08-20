import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
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

    private static final long COUNT = 10;

    public static void main(String[] args) {
        // Kerberos认证所需配置
        System.setProperty("java.security.krb5.conf", "D:\\myWorkspace\\kafka-kerberos-study\\src\\main\\resources\\krb5.conf");
        System.setProperty("java.security.auth.login.config", "D:\\myWorkspace\\kafka-kerberos-study\\src\\main\\resources\\kafka-jaas.conf");

        // kafka相关配置
        Properties props = new Properties();
        //集群地址，多个地址用"，"分隔
        props.put("bootstrap.servers", "1.15.154.243:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // Kerberos认证必须添加以下三行
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.kerberos.service.name", "kafka");
        props.put("sasl.mechanism", "GSSAPI");

        // 新建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        int i = 1;
        do {
            String message = "hello word " + i;
            LOGGER.info("推送数据:{}", message);
            // 推送数据
            producer.send(new ProducerRecord<>("test", message));
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } while (i++ <= COUNT);
    }
}
