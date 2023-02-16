package com.panwei.sparkStreaming.realTime.Util

/**
 * kafka工具类，用于生产和消费数据
 */

import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

object MyKafkaUtils {

  /**
   * 消费者配置
   */
  private val consumerConfigs: mutable.Map[String, Object] = mutable.Map[String, Object](
    // kafka集群位置
    // ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    // ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropsUtil("kafka.bootstrap-servers"),
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropsUtil(Myconfig.KAFKA_BOOTSTRAP_SERVERS),
    // kv反序列化器
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",

    // groupId
    // offset提交,自动，手动
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    // 自动提交的时间间隔
    // ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "5"
    // offset重置 "earliest","latest"
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest "

  )

  /**
   * 基于sparkstreaming消费,获取到kafkaDStream,使用的默认offset
   */
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String) = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc
      , LocationStrategies.PreferConsistent
      , ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs)
    )
    kafkaDStream
  }

  // 定义生产者对象
  val producer: KafkaProducer[String, String] = createProducer()

  // 创建生产者对象

  def createProducer(): KafkaProducer[String, String] = {
    val producerConfigs: java.util.HashMap[String, AnyRef] = new java.util.HashMap[String, AnyRef]
    // kafka集群配置
    //    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "loacalhost:9092")
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropsUtil(Myconfig.KAFKA_BOOTSTRAP_SERVERS))
    // kv序列化
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    // acks
    producerConfigs.put(ProducerConfig.ACKS_CONFIG, "all")
    // batch.size
    // linger.ms
    // retries
    producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")


    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](producerConfigs)

    producer

  }

  /**
   * 生产(按照默认黏性分区策略)
   */

  def send(topic: String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, msg))
  }

  /**
   * 按照key分区
   *
   * @param topic
   * @param msg
   * @param key
   */
  def send(topic: String, key: String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, key, msg))
  }


  // 关闭生产者对象
  def close(): Unit = {
    if (producer != null) producer.close()
  }


}
