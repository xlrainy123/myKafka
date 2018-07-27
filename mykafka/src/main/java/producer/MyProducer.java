package producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.Future;

public class MyProducer {

    private static Properties kafkaProps = new Properties();

    public void initProperty(){
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("retries", 3);
        kafkaProps.put("acks", "all");
    }

    public Producer getProducer(Properties props){
        if (props == null || props.size() == 0)
            throw new IllegalArgumentException();
        return new KafkaProducer(props);
    }

    public void send(Producer producer) throws Exception{
        for (int i = 0; i < 2; i++){

            ProducerRecord<String, String> record = new ProducerRecord<String, String>
                    ("country","name","UK"+String.valueOf(i));

            //同步发送消息，消息发送成功后，服务端会返回给一个RecordMetadata对象
            Future<RecordMetadata> future = producer.send(record);

            RecordMetadata metadata = future.get();

            System.out.println(metadata.offset()+"\npartition:"+metadata.partition()
                    +"\ntopic:"+metadata.topic()+"\nserializedKeySize:"
                    +metadata.serializedKeySize()+"\nserializedValueSize:"+metadata.serializedValueSize());
        }
        producer.close();
    }

    public static void main(String[] args) throws Exception{
        MyProducer myProducer = new MyProducer();
        myProducer.initProperty();
        Producer producer = myProducer.getProducer(kafkaProps);
        myProducer.send(producer);
    }
}
