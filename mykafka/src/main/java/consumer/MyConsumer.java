package consumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import kafka.log.Log;
import kafka.log.Log$;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class MyConsumer {

    private Properties kafkaPros = new Properties();
    private Consumer<String, String> consumer = null;

    public void initProperty(){
        kafkaPros.put("bootstrap.servers", "localhost:9092");
        kafkaPros.put("group.id", "testGroup");
        kafkaPros.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaPros.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    public void setConsumer(){
        if (kafkaPros == null || kafkaPros.size() == 0){
            throw new IllegalArgumentException();
        }
        this.consumer =  new KafkaConsumer<String, String>(kafkaPros);
    }

    public void subscirbe(Collection<String> topics){
        consumer.subscribe(topics);
    }

    public void consumer(){
        try {
            System.out.println("开始获取数据了");
            for (; ; ) {
                ConsumerRecords<String, String> records = consumer.poll(100);
//                System.out.println(records.count());
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("topic:%s, partition:%s, offset:%s, key:%s, value:%s",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                }
            }
        }finally {
            consumer.close();
        }
    }

    public static void main(String[] args){
        MyConsumer myConsumer = new MyConsumer();
        myConsumer.initProperty();
        myConsumer.setConsumer();
        List<String> topics = new ArrayList<String>();
        topics.add("test");
        topics.add("country");
        myConsumer.subscirbe(topics);
        myConsumer.consumer();
    }
}
