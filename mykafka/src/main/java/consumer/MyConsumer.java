package consumer;

import java.util.*;
import java.util.Properties;

import kafka.cluster.Partition;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class MyConsumer {

    private Properties kafkaPros = new Properties();
    private Consumer<String, String> consumer = null;
    private Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
    private int cnt = 0;
    private volatile boolean closing = false;
    /**
     *  ConsumerRebalanceListener的实现类
     *  在消费者订阅主题的时候传进去
     *  作用：
     *  1. 在消费者即将失去对分区的所有权的时候调用，也就是再均衡发生之前调用
     *  2. 再均衡发生之后调用
     */
    private class HandlerRebalance implements ConsumerRebalanceListener{
        /**
         * 再均衡之前调用
         * 主要用于消费者即将失去对分区所有权的时候，提交最新处理过的分区的偏移量
         * @param partitions
         */
        public void onPartitionsRevoked(Collection<TopicPartition> partitions){
            System.out.println("开始提交偏移量");
            consumer.commitSync(offsets);
        }

        /**
         * 再均衡之后调用
         * @param partitions
         */
        public void onPartitionsAssigned(Collection<TopicPartition> partitions){
            System.out.println("开始处理分区");
//            consumer.seekToBeginning(partitions);
            /**
             * 再均衡之后，也就是在发生分区分配之后，可以使用这个方法进行偏移量的重新设定
             */
            for (TopicPartition partition : partitions){
                consumer.seek(partition, 0);
            }
        }
    }
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
        /**
         * 在订阅主题的时候，传如一个ConsumerRebalanceListener实例，
         * 用来应对再均衡发生时的场景
         */
        consumer.subscribe(topics, new HandlerRebalance());
    }

    public void consumer(){
        /**
         * 通过在其他线程中调用consumer.wakeup()可以中断消费者的轮询
         * 这里通过一个volatile变量来保证内存可见性
         * consumer.wakeup()一旦被执行，那么消费者下次再调用poll方法就会跑出异常
         * 但是这个异常不需要捕获
         */
        Thread thread = new Thread(){
            @Override
            public void run(){
                System.out.println("start exiting...");
                while(!closing){}
                System.out.println(cnt);
                consumer.wakeup();
            }
        };

        thread.start();
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
                    cnt++;

                    if (cnt == 5){
                        closing = true;
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
            System.out.println("开始关闭");
            consumer.close();
        }
    }

    /**
     * 注册一个钩子，在JVM退出时执行
     */
    public void shutdown(){
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run(){
                System.out.println("start exiting...");
                while(!closing){}
                consumer.wakeup();
            }
        });
    }

    /**
     * 使用没有群组的消费者
     * 为消费者分配主题的分区
     */
    public void singleConsumer(){
        List<PartitionInfo> partitionInfos = consumer.partitionsFor("country");
        System.out.println("partitionInfos: "+partitionInfos);
        List<TopicPartition> partitions = new ArrayList<TopicPartition>();
        for (PartitionInfo partitionInfo : partitionInfos){
            partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
            System.out.println(String.format("topic: %s, patition: %s, leader: %s",
                    partitionInfo.topic(), partitionInfo.partition(), partitionInfo.leader()));
        }
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        try{
            for (;;){
                ConsumerRecords<String, String> records = consumer.poll(100);
                if (records.isEmpty())
                    continue;
                for (ConsumerRecord<String, String> record : records){
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
//        List<String> topics = new ArrayList<String>();
//        topics.add("test");
//        topics.add("country");
//        myConsumer.subscirbe(topics);
//        myConsumer.consumer();

        myConsumer.singleConsumer();
    }
}
