package com.panwei.sparkStreaming.realTime.APP

import java.lang

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.panwei.sparkStreaming.realTime.Bean.{PageActionLog, PageDisplayLog, PageLog}
import com.panwei.sparkStreaming.realTime.Util.{MyKafkaUtils, PropsUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * 日志数据的消费分流
 * 1、准备实时处理环境 StreamingContext
 *
 * 2、从Kafka中消费数据
 *
 * 3、处理数据
 *    3.1 转换数据结构
 * 专用结构 Bean
 * 通用结构 Map JsonObject
 *    3.2 分流
 *
 * 4、写出到DWD层
 *
 */

object OdsBaseLog {
  def main(args: Array[String]): Unit = {
    // 1、准备实时处理环境 StreamingContext
    // TODO 注意并行度与kafka中topic的分区个数的对应关系
    val conf: SparkConf = new SparkConf().setAppName("ods_base_log_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    // 2、从kafka中消费数据
    val topicName: String = "ODS_BASE_LOG" // 对应生成器配置的主题名
    val groupid: String = "ODS_BASE_LOG_GROUP"
    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupid)


    // 3、处理数据
    // 3.1、转换数据结构
    val jsonObjDstream: DStream[JSONObject] = kafkaDstream.map(
      consumerRecord => {
        // 获取consumerRecord中的value，value就是日志数据
        val log: String = consumerRecord.value()
        // 转换成Json对象
        val jsonobj: JSONObject = JSON.parseObject(log)
        // 返回
        jsonobj
      }
    )
    // jsonObjDstream.print(1000)

    // 3.2 分流数据
    //  日志数据：
    //     页面访问数据
    //        公共字段
    //        页面数据
    //        曝光数据
    //        事件数据
    //        错误数据
    //     启动数据：
    //        公共字段
    //        启动数据
    //        错误数据
    val DWD_PAGE_LOG_TOPIC: String = "DWD_PAGE_LOG_TOPIC" // 页面访问
    val DWD_PAGE_DISPALY_TOPIC: String = "DWD_PAGE_DISPALY_TOPIC"
    val DWD_PAGE_ACTION_TOPIC: String = "DWD_PAGE_ACTION_TOPIC"
    val DWD_START_LOG_TOPIC: String = "DWD_START_LOG_TOPIC" // 启动数据
    val DWD_ERROR_LOG_TOPIC: String = "DWD_ERROR_LOG_TOPIC" // 错误数据

    // 分流规则：
    // 错误数据：不做任何拆分
    // 页面数据：拆分
    // 启动数据：不拆分
    jsonObjDstream.foreachRDD(
      rdd => {
        rdd.foreach(jsonObj => {
          // 分流过程
          // 分流错误数据
          val errObj: JSONObject = jsonObj.getJSONObject("err")
          if (errObj != null) {
            // 将错误数据发送到DWD_ERROR_LOG_TOPIC
            MyKafkaUtils.send(DWD_ERROR_LOG_TOPIC, jsonObj.toJSONString)
          } else {
            // 提取公共字段
            val commonObj: JSONObject = jsonObj.getJSONObject("common")
            val ar: String = commonObj.getString("ar")
            val uid: String = commonObj.getString("uid")
            val os: String = commonObj.getString("os")
            val ch: String = commonObj.getString("ch")
            val is_new: String = commonObj.getString("is_new")
            val md: String = commonObj.getString("md")
            val mid: String = commonObj.getString("mid")
            val vc: String = commonObj.getString("vc")
            val ba: String = commonObj.getString("ba")
            // 提取事件戳
            val ts: Long = jsonObj.getLong("ts")
            // 页面数据
            val pageObj: JSONObject = jsonObj.getJSONObject("page")
            if (pageObj != null) {
              // 提取页面字段
              val page_id: String = pageObj.getString("page_id")
              val item: String = pageObj.getString("item")
              val during_time: Long = pageObj.getLong("during_time")
              val item_type: String = pageObj.getString("item_type")
              val last_page_id: String = pageObj.getString("last_page_id")
              val source_type: String = pageObj.getString("source_type")
              // 封装成pageLog
              val pageLog = PageLog(mid,uid,ar,ch,is_new,md,os,vc,ba,page_id,last_page_id,item,item_type,during_time,source_type,ts)
              // 发送到DWD_PAGE_LOG_TOPIC
              MyKafkaUtils.send("DWD_PAGE_LOG_TOPIC",JSON.toJSONString(pageLog,new SerializeConfig(true)))
              // 提取曝光数据
              val arrayObj: JSONArray = jsonObj.getJSONArray("displays")
              if (arrayObj != null && arrayObj.size()> 0) {
                for (i <- 0 until arrayObj.size()) {
                  // 循环拿到每个曝光数据
                  val displayObj: JSONObject = arrayObj.getJSONObject(i)
                  val display_type: String = displayObj.getString("display_type")
                  val display_item: String = displayObj.getString("item")
                  val display_item_type: String = displayObj.getString("item_type")
                  val display_pos_id: String = displayObj.getString("pos_id")
                  val display_order: String = displayObj.getString("order")

                  // 封装成PageDisplay
                  val pageDisplayLog: PageDisplayLog = PageDisplayLog(mid,uid,ar,ch,is_new,md,os,vc,ba,page_id,last_page_id,item,item_type,during_time,source_type,display_type,display_item,display_item_type,display_order,display_pos_id,ts)
                  // 写到DWD_PAGE_DISPALY_TOPIC
                  MyKafkaUtils.send("DWD_PAGE_DISPALY_TOPIC",JSON.toJSONString(pageDisplayLog,new SerializeConfig(true)))
                }
              }


              // 提取事件数据
              val actionObj: JSONArray = jsonObj.getJSONArray("actions")
              if(actionObj != null && actionObj.size > 0 ){
                for(i <- 0 until actionObj.size() ){
                  val indexObj: JSONObject = actionObj.getJSONObject(i)
                  val action_id: String = indexObj.getString("action_id")
                  val action_item: String = indexObj.getString("item")
                  val action_item_type: String = indexObj.getString("item_type")
                  val action_ts: Long = indexObj.getLong("ts")

                  // 封装成actionLog
                  val pageLog: PageActionLog = PageActionLog(mid,uid,ar,ch,is_new,md,os,vc,ba,page_id,last_page_id,item,item_type,during_time,source_type,action_id,action_item,action_item_type,action_ts)
                  MyKafkaUtils.send("DWD_PAGE_ACTION_TOPIC",JSON.toJSONString(pageLog, new SerializeConfig(true)))


                }
              }


            }
            // 启动数据
          }

        })
      }
    )

    ssc.start()
    ssc.awaitTermination()

  }

}
