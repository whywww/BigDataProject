package Hbase


import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark._
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io._

object readHbase {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val tablename = "apps"
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum","hdp-node-02,hdp-node-03,hdp-node-04")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, tablename)

    // 如果表不存在则创建表
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tablename)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tablename))
      admin.createTable(tableDesc)
    }

    //读取数据并转化成rdd
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])





    val count = hBaseRDD.count()
    //println(count)
    hBaseRDD.foreach{case (_,result) =>{
      //获取行键
      val key = Bytes.toInt(result.getRow)
      //通过列族和列名获取列
      val baidu = Bytes.toLong(result.getValue("timeLen".getBytes,"BaiDu".getBytes))
      val MeiTuan = Bytes.toLong(result.getValue("timeLen".getBytes,"MeiTuan".getBytes))
      val GaoDeDiTu = Bytes.toLong(result.getValue("timeLen".getBytes,"GaoDeDiTu".getBytes))
      val QQYouXiang = Bytes.toLong(result.getValue("timeLen".getBytes,"QQYouXiang".getBytes))
      println("Row key:"+key+"      baidu :"+baidu+"    meituan:" + MeiTuan +"    GAODE:" +GaoDeDiTu +"  QQ："+ QQYouXiang)

    }}

    sc.stop()
    admin.close()
  }
}