# mykafka
作为kafka练习之用

##  MyConsumer


##### 消费者消费数据的方式


*  pool()方法获取ConsumerRecords列表，一次遍历每个ConsumerRecord对象，包含了消息的主题，分区，键值对，偏移量

##### 消费者提交偏移量的方式

* 同步提交
* 异步提交
* 异步提交 + OffsetCommitCallback
* 同步/异步提交 + Map<TopicPartition, OffsetAndMetadata>

##### 再均衡监听器

* 接口类ConsumerRebalanceListener
* 订阅主题的时候，传入该接口的实例
* 再均衡之前调用的方法，提交当前处理的分区偏移量
* 再均衡之调用,seek()方法从指定源来加载分区的offset

## MyProducer

##### 生产者发送数据的方式

* 数据包装成ProducerRecord对象，包含topic，key，value
* 直接发送，不管不问： producer.send(record)
* 同步发送，获取发送结果： Future < RecordMetadata > data = producer.send(record); data.get();
* 异步发送，执行回调： send() + record + Callbcak实例
