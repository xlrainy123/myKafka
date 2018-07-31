package producer;

import my.kafka.avro.User;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import serializer.AvroSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class MyProducer2<T extends SpecificRecordBase> {

    private static Properties kafkaProps = new Properties();

    private Producer<String, T> producer = null;

    private void initProperty(){
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", StringSerializer.class.getName());
        kafkaProps.put("value.serializer", AvroSerializer.class.getName());
        kafkaProps.put("retries", 3);
        kafkaProps.put("acks", "1");
        kafkaProps.put("client.id", "zhangsy");
    }

    private void config(){
        initProperty();
        producer = new KafkaProducer<String, T>(kafkaProps);
    }

    public void send(String topic, T data){
        ProducerRecord<String, T> record = new ProducerRecord<>(topic, ((User)data).get(0).toString(), data);
        System.out.println(record);
        /**
         * 异步发送
         */
//        producer.send(record, new Callback() {
//            @Override
//            public void onCompletion(RecordMetadata metadata, Exception exception) {
//                System.out.println("收到响应");
//                if (exception != null){
//                    System.out.println("offset:"+metadata.offset()+"\npartition:"+metadata.partition()
//                            +"\ntopic:"+metadata.topic()+"\nserializedKeySize:"
//                            +metadata.serializedKeySize()+"\nserializedValueSize:"+metadata.serializedValueSize()+"\n");
//                }else {
//                    exception.printStackTrace();
//                }
//            }
//        });
        /**
         * 同步发送
         */
        try{
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();
            System.out.println("offset:"+metadata.offset()+"\npartition:"+metadata.partition()
                            +"\ntopic:"+metadata.topic()+"\nserializedKeySize:"
                            +metadata.serializedKeySize()+"\nserializedValueSize:"+metadata.serializedValueSize()+"\n");
        }catch (Exception e){}
    }

    public static void main(String[] args){
        MyProducer2 myProducer2 = new MyProducer2();
        myProducer2.config();
        myProducer2.send("serialize", new User("xlcheng1","beijing"));
    }

}
