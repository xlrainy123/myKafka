package producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MyProducer {

    private static Properties kafkaProps = new Properties();

    /**
     * 初始化一些配置信息
     */
    public void initProperty(){
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        kafkaProps.put("value.serializer", "producer.Personed");
        kafkaProps.put("retries", 3);
        kafkaProps.put("acks", "all");
        kafkaProps.put("client.id", "zhangsy");
    }

    /**
     * 加载配置信息，生成一个生产者实例
     * @param props
     * @return
     */
    public Producer getProducer(Properties props){
        if (props == null || props.size() == 0)
            throw new IllegalArgumentException();
        return new KafkaProducer(kafkaProps);
    }

    /**
     * 同步发送消息
     * @param producer
     * @throws Exception
     */
    public void syncSend(Producer producer) throws Exception{
        for (int i = 0; i < 2; i++){

            ProducerRecord<String, String> record = new ProducerRecord<String, String>
                    ("country","name","UK"+String.valueOf(i));

            //同步发送消息，消息发送成功后，服务端会返回给一个RecordMetadata对象
            Future<RecordMetadata> future = producer.send(record);

            RecordMetadata metadata = future.get();

            System.out.println("offset:"+metadata.offset()+"\npartition:"+metadata.partition()
                    +"\ntopic:"+metadata.topic()+"\nserializedKeySize:"
                    +metadata.serializedKeySize()+"\nserializedValueSize:"+metadata.serializedValueSize()+"\n");
        }
        producer.close();
    }

    /**
     * 异步发送消息
     * @param producer
     */
    public void asyncSend(Producer producer){

        ProducerRecord<String, String> record = new ProducerRecord<String, String>("test","zhangsy","xlrainy");
        producer.send(record, new Callback(){
            public void onCompletion(RecordMetadata metadata, Exception e){
                System.out.println("offset:"+metadata.offset()+"\npartition:"+metadata.partition()
                        +"\ntopic:"+metadata.topic()+"\nserializedKeySize:"
                        +metadata.serializedKeySize()+"\nserializedValueSize:"+metadata.serializedValueSize()+"\n");
                if (e == null){
                    System.out.println("hello");
                }
            }
        });

        producer.close();
    }


    public void start() throws Exception{
        initProperty();
//        syncSend(getProducer(kafkaProps));
        asyncSend(getProducer(kafkaProps));
    }


    /**
     * 这个地方出现生产者初始化失败
     * @param producer
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void sendObject(Producer producer) throws ExecutionException, InterruptedException {

        ProducerRecord<String, Person> record = new ProducerRecord<String, Person>
                                            ("object","today",new Person("zhangsy",19));
        Future<RecordMetadata> future = producer.send(record);

        RecordMetadata metadata = future.get();

        System.out.println("offset:"+metadata.offset()+"\npartition:"+metadata.partition()
                +"\ntopic:"+metadata.topic()+"\nserializedKeySize:"
                +metadata.serializedKeySize()+"\nserializedValueSize:"+metadata.serializedValueSize()+"\n");

        producer.close();
    }

    public void startObject(){
        initProperty();
        try{
            Producer producer = new KafkaProducer(kafkaProps);
            sendObject(producer);
        }catch (ExecutionException e){

        }catch (InterruptedException e){

        }
    }
    public static void main(String[] args) throws Exception{
        MyProducer myProducer = new MyProducer();
        myProducer.start();
    }

}
