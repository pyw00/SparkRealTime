package com.panwei.sparkStreaming.realTime.Util

import java.util.ResourceBundle

/**
 * 配置文件解析类
 */

object PropsUtil {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  def apply(prosKey: String): String = {
    bundle.getString(prosKey)
  }

  def main(args: Array[String]): Unit = {
    println(PropsUtil.apply("kafka.bootstrap-servers"))
  }
}
