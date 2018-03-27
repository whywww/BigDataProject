
import java.io.Serializable

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, HTable, Put}
import org.apache.kafka.common.serialization.{BytesDeserializer, StringDeserializer}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql._

import scala.concurrent.duration._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.functions.{get_json_object, json_tuple}

import scala.util.parsing.json.{JSON, JSONObject}
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD


object KafkaTest {


  case class be(activetime1:Long,activetime2:Long,application1:String,application2:String,begintime:Long,day:String,endtime:Long,userId:String)
  implicit val formats = DefaultFormats


  def main(args: Array[String]): Unit = {




        val conf= new SparkConf()
          .setAppName("kafkaConnectionTest").setMaster("local")
        val streamingContext = new StreamingContext(conf, Seconds(2))
    streamingContext.sparkContext.setLogLevel("error")
        val sqlContext = new SQLContext(streamingContext.sparkContext)



        val kafkaParams = Map[String, Object](
          "bootstrap.servers" -> "192.168.48.101:9092,192.168.48.102:9092,192.168.103:9092,192.168.48.104:9092",
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "group.id" -> "flume-consumer",
          "auto.offset.reset" -> "latest",
          "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        val topics = Array("bigdata")
        val stream = KafkaUtils.createDirectStream[String, String](
          streamingContext,
          PreferConsistent,
          Subscribe[String, String](topics, kafkaParams)
        )



    val kafkaRDD = stream.map(record=>record.value).map(json=>{
      val a = parse(json).extract[be]
      var activetimeAll = new Array[Long](4)
      if (a.application1 == "BaiDu") activetimeAll(0) = a.activetime1
      else if (a.application1 == "MeiTuan") activetimeAll(1) = a.activetime1
      else if (a.application1 == "GaoDeDiTu") activetimeAll(2) = a.activetime1
      else if (a.application1 == "QQYouXiang") activetimeAll(3) = a.activetime1
      if (a.application2 == "BaiDu") activetimeAll(0) = a.activetime2
      else if (a.application2 == "MeiTuan") activetimeAll(1) = a.activetime2
      else if (a.application2 == "GaoDeDiTu") activetimeAll(2) = a.activetime2
      else if (a.application2 == "QQYouXiang") activetimeAll(3) = a.activetime2
      ((a.userId,a.day),activetimeAll)
    })

//
//  hBaseRDD.foreach { case (_,result) => {
//    //获取行键
//    val key = Bytes.toInt(result.getRow)
//    //通过列族和列名获取列
//    val baidu = Bytes.toLong(result.getValue("timeLen".getBytes, "BaiDu".getBytes))
//    val MeiTuan = Bytes.toLong(result.getValue("timeLen".getBytes, "MeiTuan".getBytes))
//    val GaoDeDiTu = Bytes.toLong(result.getValue("timeLen".getBytes, "GaoDeDiTu".getBytes))
//    val QQYouXiang = Bytes.toLong(result.getValue("timeLen".getBytes, "QQYouXiang".getBytes))
//    println("Row key:" + key + " baidu :" + baidu + "meituan:" + MeiTuan + "GAODE:" + GaoDeDiTu + "QQ" + QQYouXiang)
//
//  }
//  }




//    val tablename = "apps"//数据库名字
//    val hBaseConf = HBaseConfiguration.create()
//    hBaseConf.set("hbase.zookeeper.quorum","192.168.48.102,192.168.48.103,192.168.48.104")
//    hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")
//    hBaseConf.set(TableInputFormat.INPUT_TABLE, tablename)
//    val jobConf = new JobConf(hBaseConf)
//    jobConf.setOutputFormat(classOf[TableOutputFormat])
//    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)
//    val hBaseRDD = streamingContext.sparkContext.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat],
//      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
//      classOf[org.apache.hadoop.hbase.client.Result])
//
//    hBaseRDD.foreach { case (_,result) => {
//      //获取行键
//      val key = Bytes.toInt(result.getRow)
//      //通过列族和列名获取列
//      val baidu = Bytes.toLong(result.getValue("timeLen".getBytes, "BaiDu".getBytes))
//      val MeiTuan = Bytes.toLong(result.getValue("timeLen".getBytes, "MeiTuan".getBytes))
//      val GaoDeDiTu = Bytes.toLong(result.getValue("timeLen".getBytes, "GaoDeDiTu".getBytes))
//      val QQYouXiang = Bytes.toLong(result.getValue("timeLen".getBytes, "QQYouXiang".getBytes))
//      println("Row key:" + key + " baidu :" + baidu + "meituan:" + MeiTuan + "GAODE:" + GaoDeDiTu + "QQ" + QQYouXiang)
//
//
//
//    }
//    }
val tablename = "apps"//数据库名字
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.quorum","192.168.48.102,192.168.48.103,192.168.48.104")
    hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hBaseConf.set(TableInputFormat.INPUT_TABLE, tablename)


    kafkaRDD.foreachRDD(x=>{




        println("FOR EACH RDD")

        val hBaseRDD = streamingContext.sparkContext.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat],
          classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
          classOf[org.apache.hadoop.hbase.client.Result])

      println("hbaseRDD 数量：" +hBaseRDD.count())




        val resultRDD = hBaseRDD.map(tuple => tuple._2)
      println("hbaseRDD 数量：" +resultRDD.count())
        val keyvalueRDD = resultRDD.map(result => {
          var pre = Array(
            Bytes.toLong(result.getValue(Bytes.toBytes("timeLen"), Bytes.toBytes("BaiDu"))),
            Bytes.toLong(result.getValue(Bytes.toBytes("timeLen"), Bytes.toBytes("MeiTuan"))),
            Bytes.toLong(result.getValue(Bytes.toBytes("timeLen"), Bytes.toBytes("GaoDeDiTu"))),
            Bytes.toLong(result.getValue(Bytes.toBytes("timeLen"), Bytes.toBytes("QQYouXiang")))
          )

          ((Bytes.toInt(result.getRow).toString,
            Bytes.toString(result.getValue(Bytes.toBytes("timeLen"), Bytes.toBytes("day")))), pre)
        })
        println("keyvalueRDD数量： " + hBaseRDD.count())
        keyvalueRDD.foreach(x=>{
          println("userID: " + x._1._1 +  "day: "+ x._1._2 + "Baidu: " + x._2(0) + "MeiTuan: " + x._2(1) + "GaoDeDiTu: " + x._2(2)+ "QQYouXiang: " + x._2(3) )
        })

        val unionCombinedRDD = keyvalueRDD.union(x).reduceByKey(reduceFun)
        println("unionCombinedRDD： " + hBaseRDD.count())





        unionCombinedRDD.map(x => {
          val put = new Put(Bytes.toBytes(x._1._1.toInt))
          put.addColumn(Bytes.toBytes("timeLen"), Bytes.toBytes("day"), Bytes.toBytes(x._1._2))
          put.addColumn(Bytes.toBytes("timeLen"), Bytes.toBytes("BaiDu"), Bytes.toBytes(x._2(0).toLong))
          put.addColumn(Bytes.toBytes("timeLen"), Bytes.toBytes("MeiTuan"), Bytes.toBytes(x._2(1).toLong))
          put.addColumn(Bytes.toBytes("timeLen"), Bytes.toBytes("GaoDeDiTu"), Bytes.toBytes(x._2(2).toLong))
          put.addColumn(Bytes.toBytes("timeLen"), Bytes.toBytes("QQYouXiang"), Bytes.toBytes(x._2(3).toLong))
          put
        }).foreachPartition(x => {
//          val conn = ConnectionFactory.createConnection(hBaseConf)
//          val table = conn.getTable(TableName.valueOf(tablename))
          //两种获得table的方式
                  var jobConf = new JobConf(HBaseConfiguration.create)
                  val table = new HTable(jobConf, TableName.valueOf(tablename))
          import scala.collection.JavaConversions._
          table.put(seqAsJavaList(x.toSeq))
        })
        println("单次写入结束......")








    })

//  val shop = HBaseRDD.map(r => {
//    var pre = Array(
//      Bytes.toLong(r._2.getValue(Bytes.toBytes("timeLen"), Bytes.toBytes("BaiDu"))),
//      Bytes.toLong(r._2.getValue(Bytes.toBytes("timeLen"), Bytes.toBytes("MeiTuan"))),
//      Bytes.toLong(r._2.getValue(Bytes.toBytes("timeLen"), Bytes.toBytes("GaoDeDiTu"))),
//      Bytes.toLong(r._2.getValue(Bytes.toBytes("timeLen"), Bytes.toBytes("QQYouXiang")))
//    )
//
//    ((Bytes.toString(r._2.getValue(Bytes.toBytes("timeLen"), Bytes.toBytes("userID"))),
//      Bytes.toString(r._2.getValue(Bytes.toBytes("timeLen"), Bytes.toBytes("day")))), pre)
//  })
//  //      .union(kafkaRDD.foreachRDD{
//  //        x=>(
//  //          val batch = x.map(record=>(record._1._1,record._1._2,record._2(0),record._2(1),record._2(2),record._2(3)))
//  //          batch
//  //      })
//
//  //    shop.map(r=>(r._1._1,r._1._2),)
//  kafkaRDD.foreachRDD { x => {
//    shop.union(x).reduceByKey(reduceFun)
//  }
//  }
//
//  //val RDD = shop.map(record => (record._1._1, record._1._2, record._2(0), record._2(1), record._2(2), record._2(3)))
//
//
//  //println(RDD)
//
//
//  if(shop.isEmpty() != false) {
//
//    val rdd: RDD[(ImmutableBytesWritable, Put)] = shop.map(x => {
//
//
//      val put = new Put(Bytes.toBytes(x._1._1.toString))
//      put.add(Bytes.toBytes("timeLen"), Bytes.toBytes("day"), Bytes.toBytes(x._1._2.toString))
//      put.add(Bytes.toBytes("timeLen"), Bytes.toBytes("BaiDu"), Bytes.toBytes(x._2(0).toLong))
//      put.add(Bytes.toBytes("timeLen"), Bytes.toBytes("MeiTuan"), Bytes.toBytes(x._2(1).toLong))
//      put.add(Bytes.toBytes("timeLen"), Bytes.toBytes("GaoDeDiTu"), Bytes.toBytes(x._2(2).toLong))
//      put.add(Bytes.toBytes("timeLen"), Bytes.toBytes("QQYouXiang"), Bytes.toBytes(x._2(3).toLong))
//
//      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
//      (new ImmutableBytesWritable, put)
//
//    })
//
//
//    rdd.saveAsHadoopDataset(jobConf)








    streamingContext.start()

    streamingContext.awaitTermination()
}

  def reduceFun(x:Array[Long],y:Array[Long]):Array[Long] = {
    var batchReduceResult = Array(x(0)+y(0),x(1)+y(1),x(2)+y(2),x(3)+y(3))
    batchReduceResult
  }





}
