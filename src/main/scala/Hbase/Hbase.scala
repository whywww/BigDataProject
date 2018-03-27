import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object Hbase {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum","hdp-node-01,hdp-node-02,hdp-node-03,hdp-node-04")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    val tablename = "apps"

    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    val indataRDD = sc.makeRDD(Array("1,2016-03-22,10000,10000,10000,10000","2,2018-03-21,10000,10000,10000,10000"))


    val rdd = indataRDD.map(_.split(',')).map{arr=>{
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val put = new Put(Bytes.toBytes(arr(0).toInt))
      put.add(Bytes.toBytes("timeLen"),Bytes.toBytes("day"),Bytes.toBytes(arr(1)))
      put.add(Bytes.toBytes("timeLen"),Bytes.toBytes("BaiDu"),Bytes.toBytes(arr(2).toLong))
      put.add(Bytes.toBytes("timeLen"),Bytes.toBytes("MeiTuan"),Bytes.toBytes(arr(3).toLong))
      put.add(Bytes.toBytes("timeLen"),Bytes.toBytes("GaoDeDiTu"),Bytes.toBytes(arr(4).toLong))
      put.add(Bytes.toBytes("timeLen"),Bytes.toBytes("QQYouXiang"),Bytes.toBytes(arr(5).toLong))
      //put.addColumn(Bytes.toBytes("basic"),Bytes.toBytes("name"))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, put)
    }}

    rdd.saveAsHadoopDataset(jobConf)

    sc.stop()
  }



}