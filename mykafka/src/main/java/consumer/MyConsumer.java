package consumer;

import java.util.*;
import java.util.Properties;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class MyConsumer {

    private Properties kafkaPros = new Properties();
    private Consumer<String, String> consumer = null;
    private Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
    private int cnt = 0;

    public void initProperty(){
        kafkaPros.put("bootstrap.servers", "localhost:9092");
        kafkaPros.put("group.id", "testGroup");
        kafkaPros.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaPros.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaPros.put("auto.offset.reset ", "earliest");
        kafkaPros.put("auto.commit.offset", false);
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
                if (records.isEmpty())
                    continue;

                for (ConsumerRecord<String, String> record : records) {
                    //模拟消息处理的过程
                    System.out.println(String.format("topic:%s, partition:%s, offset:%s, key:%s, value:%s",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value()));

                    offsets.put(new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset()+1,""));

                    //每处理完1000条记录就提交一次
                    if (cnt % 1000 == 0){
                        consumer.commitAsync(offsets, null);
                    }

                }
                //消费者运行过程中使用异步提交来提升吞吐量，因为就算这次提交失败，下次总会有成功的
                consumer.commitAsync(new OffsetCommitCallback(){
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e){
                        System.out.println(offsets);
                    }
                });

            }
        }finally {
            //在关闭消费者之前使用同步提交来确保可以提交成功
            consumer.commitSync();
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
