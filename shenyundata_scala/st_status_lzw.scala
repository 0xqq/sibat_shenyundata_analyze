package shenyundataaseess

import java.io.{File, FileNotFoundException, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ListBuffer

object st_status_lzw {
  case class St(gb_code: String, dev_id: String, dev_type: String, dev_name: String, dev_mac: String, dev_ip: String,
                network_type: String, comp_name: String, org_name: String, project_desc: String, dev_addr: String, px: String,
                py: String, station_id: String, station_name: String, camera_type: String, camera_quality: String, dev_status: String,
                dev_grid: String, dev_gridlist_control: String, create_time: String, update_time: String, line_name: String)

  /** 计算延时 **/
  def subtract(datetime:String,receivetime:Long):Int={
    //将字符串转化为时间戳
    val ts = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    val date_long = ts.parse(datetime).getTime();
    //相差的秒数
    val diff = receivetime-date_long/1000
    diff.toInt.abs
  }

  /** 时间戳变date **/
  def tranTimeToString(tm:String) :String={
    //时间戳变date
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time = fm.format(new Date(tm.toLong*1000))
    time
  }

  /** date变时间戳 **/
  def tranTimeToLong(tm:String) :Long={
    //date变时间戳
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tim = fm.parse(tm).getTime();
    tim/1000
  }

  def main(args: Array[String]): Unit = {
    //val conf = new SparkConf().setAppName("st_status_lzw")//.setMaster("local")
    //val sc = new SparkContext(conf)
    //sc.setLogLevel("ERROR")
    // val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // new SparkContext(conf)

    val sqlContext = SparkSession
      .builder()
      .appName("st_status_lzw")
      .getOrCreate()

    /** 读文件basic_device_info.txt **/
    //val fileRDD = sc.textFile("/user/hadoop/GongAnV2/basic_device_info.txt")
    val fileRDD = sqlContext.sparkContext.textFile("/user/sibat/GongAn_analyze/basic_device_info.txt")

    //将RDD转为DataFrame
    import sqlContext.implicits._
    val deviceInfoDF = fileRDD.map { line => line.split("\\s{1}|\t")}
      .map { arr => St(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8),arr(9),arr(10),arr(11),arr(12),arr(13),
        arr(14),arr(15),arr(16),arr(17),arr(18),arr(19),arr(20),arr(21),arr(22))}.toDF()

    //注册一个临时表
    deviceInfoDF.createOrReplaceTempView("deviceInfo_table")

    //val parquetDFStorg =  sqlContext.read.parquet("/user/hadoop/GongAnV2/st_status/"+args(0))

    /** 遍历读取数据源文件夹下所有文件，异常文件则抛弃**/
    def isFile(hdfs : FileSystem, name : Path) : Boolean = {
      hdfs.isFile(name)
    }
    def createFile(hdfs : FileSystem, name : String) : Boolean = {
      hdfs.createNewFile(new Path(name))
    }
    class MyPathFilter extends PathFilter {
      override def accept(path: Path): Boolean = true
    }

    //递归获取集群文件夹下所有文件名及对应目录
    def listChildren(hdfs : FileSystem, fullName : String, holder : ListBuffer[String]) : ListBuffer[String] = {
      val filesStatus = hdfs.listStatus(new Path(fullName), new MyPathFilter)
      for(status <- filesStatus){
        val filePath : Path = status.getPath
        if(isFile(hdfs,filePath))
          holder += filePath.toString
        else
          listChildren(hdfs, filePath.toString, holder)
      }
      holder
    }

    val hdfs : FileSystem = FileSystem.get(new Configuration)
    val holder : ListBuffer[String] = new ListBuffer[String]
    val hdfsPath = "/user/hadoop/GongAnV2/st_status/"+args(0)
    val paths : List[String] = listChildren(hdfs, hdfsPath, holder).toList

    var parquetDFStorg:sql.DataFrame = null
    for (file_path<-paths){
      try{
        parquetDFStorg = sqlContext.read.parquet(file_path)
      } catch {
        case e:Exception=>println("Here is a file exception:"+file_path)
        case e:FileNotFoundException=>println("FileNotFoundException"+file_path)
      }
    }

    /*    |-- serverReceiveTimestamp: long (nullable = true)
    |-- cameraId: string (nullable = true)
    |-- cameraName: string (nullable = true)
    |-- dateTime: string (nullable = true)
    |-- intensity: double (nullable = true)
    |-- peopleCount: integer (nullable = true)
    |-- safetyIndex: integer (nullable = true)
    |-- gbNo: string (nullable = true)*/

    /*************************************************           需求1             *****************************************************************/
    val originnum = parquetDFStorg.count()
    val parquetDFSt = parquetDFStorg.distinct().dropDuplicates()
    val distinctnum =parquetDFSt.count()

    parquetDFSt.filter(parquetDFSt("gbNo")=!="").createOrReplaceTempView("St_table")
    val distinctnull = sqlContext.sql("select * from St_table")

    val distinctnullnum = distinctnull.count()
    val onlinenum = sqlContext.sql("select distinct gbNo from St_table")
    val finalSt = sqlContext.sql("select distinct gbNo from St_table,deviceInfo_table where St_table.gbNo=deviceInfo_table.gb_code")
    val noStationSt = sqlContext.sql("select distinct gbNo from St_table,deviceInfo_table where St_table.gbNo=deviceInfo_table.gb_code and deviceInfo_table.station_name=''")
    val fitdistinctStationSt = sqlContext.sql("select station_name,count(distinct gbNo) from St_table,deviceInfo_table where St_table.gbNo=deviceInfo_table.gb_code group by station_name")

    /** 需求1的一系列输出 **/
    val schema = StructType(List(
      StructField("item", StringType, nullable = false),
      StructField("number",StringType , nullable = true)
    ))
    val rdd = sqlContext.sparkContext.parallelize(Seq(
      Row("Total number of matches", finalSt.count().toString),//全网匹配数
      Row("Total number on line", onlinenum.count().toString),//全网匹配数
      Row("Deduplication ratio",((originnum-distinctnum)/originnum).toFloat.toString),//去重数据比 0
      Row("Number of devices without site", noStationSt.count().toString) //无站点设备数
      //distinctStationSt.show()                 //各站点在线数  TODO:由于不匹配就没站点信息，所以这里不处理
    ))
    val final_fileDF = sqlContext.createDataFrame(rdd, schema)
    final_fileDF//.coalesce(1)  //设置为一个partition, 这样可以把输出文件合并成一个文件
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .save("/user/sibat/GongAn_analyze/st_status/"+args(0)+"/fit_number")

    //fitdistinctStationSt.show()             ////各站点匹配数
    fitdistinctStationSt//.coalesce(1)  //设置为一个partition, 这样可以把输出文件合并成一个文件
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .save("/user/sibat/GongAn_analyze/st_status/"+args(0)+"/fit_Station")

    /***********************************                                需求2                                    ********************************/
    /** 单个设备15分钟粒度的数据采集数 **/
    //     st表：serverReceiveTimestamp ，cameraId ,  cameraName  , dateTime  , intensity , peopleCount , safetyIndex  , gbNo
    val StRDD = distinctnull.rdd

    val single_StRDD = StRDD.map{x=>
      val s = x.mkString("`").replace("[","").replace("]","").split("`")
      val date = tranTimeToString(s(0)).substring(0,10)
      val zeroTimestamp = date +" 00:00:00"
      val slot = (s(0).toLong - tranTimeToLong(zeroTimestamp))/900
     val slotstr = tranTimeToString((tranTimeToLong(zeroTimestamp)+slot*900).toString).substring(11,16) + "~" +tranTimeToString((tranTimeToLong(zeroTimestamp)+(slot+1)*900).toString).substring(11,16)
      ((date,s(7),slotstr),1)
    }.reduceByKey(_ + _).map(x=>(x._1._1,x._1._2,x._1._3,x._2))
//下面这句输出如下形式   date|             gb_code|       slot,  number|           ；；；；    |2018-06-24|44039602011420240004|(04:00~04:15,11) ...|
    //      .map(x=>((x._1._1,x._1._2),(x._1._3,x._2))).groupByKey().map(x => (x._1._1,x._1._2,x._2.toArray.sortBy(x=>x).mkString(" ")))

////////
//      TODO:  在这里本来想把时段表示成列的形式，但RDD字段受限问题有点麻烦，暂没解决
//      .map{x=>
//              val z:Array[Long] = new Array[Long](96)
//             for(m<-0 until 96){
//               z(m) = 0
//             }
//              val value2split = x._2.toArray.sortBy(x=>x).mkString(" ").split(" ")
//              val splitnum = value2split.length
//              value2split.foreach(t=>
//                (t.replace("(","").replace(")","").split(",")(0),t.replace("(","").replace(")","").split(",")(1)))
//              for(i<-0 until splitnum){
//                z(value2split(i)(0))=value2split(i)(1)
//              }
//              (x._1._1,x._1._2,z(0),z(1),z(2),z(3),z(4),z(5),z(6),z(7),z(8),z(9),z(10),z(11),z(12),z(13),z(14),z(15),z(16),z(17),z(18),z(19),z(20), z(21),z(22),z(23),z(24),z(25),z(26),,z(27),z(28),z(29),z(30),z(31),z(32),z(33),z(34),z(35),z(36),z(37),z(38),z(39), z(40),z(41),z(42),z(43),z(44),z(45),z(46),z(47),z(48),z(49),z(50),z(51),z(52),z(53),z(54),z(55),z(56),z(57),z(58), z(59),z(60),z(61),z(62),z(63),z(64),z(65),z(66),z(67),z(68),z(69),z(70),z(71),z(72),z(73),z(74),z(75),z(76),z(77), z(78),z(79),z(80),z(81),z(82),z(83),z(84),z(85),z(86),z(87),z(88),z(89),z(90),z(91),z(92),z(93),z(94),z(95)
//      )}
//
  //  .foreach(x=>println(x))

    val single_StRDD_DF = single_StRDD.toDF()
//    val single_StRDD_newNames = Seq("gb_code", "date",
//      "00:00~00:15","00:15~00:30","00:30~00:45","00:45~01:00","01:00~01:15","01:15~01:30",
//      "01:30~01:45","01:45~02:00","02:00~02:15","02:15~02:30","02:30~02:45","02:45~03:00",
//      "03:00~03:15","03:15~03:30","03:30~03:45","03:45~04:00","04:00~04:15","04:15~04:30",
//      "04:30~04:45","04:45~05:00","05:00~05:15","05:15~05:30","05:30~05:45","05:45~06:00",
//      "06:00~06:15","06:15~06:30","06:30~06:45","06:45~07:00","07:00~07:15","07:15~07:30",
//      "07:30~07:45","07:45~08:00","08:00~08:15","08:15~08:30","08:30~08:45","08:45~09:00",
//      "09:00~09:15","09:15~09:30","09:30~09:45","09:45~10:00","10:00~10:15","10:15~10:30",
//      "10:30~10:45","10:45~11:00","11:00~11:15","11:15~11:30","11:30~11:45","11:45~12:00",
//      "12:00~12:15","12:15~12:30","12:30~12:45","12:45~13:00","13:00~13:15","13:15~13:30",
//      "13:30~13:45","13:45~14:00","14:00~14:15","14:15~14:30","14:30~14:45","14:45~15:00",
//      "15:00~15:15","15:15~15:30","15:30~15:45","15:45~16:00","16:00~16:15","16:15~16:30",
//      "16:30~16:45","16:45~17:00","17:00~17:15","17:15~17:30","17:30~17:45","17:45~18:00",
//      "18:00~18:15","18:15~18:30","18:30~18:45","18:45~19:00","19:00~19:15","19:15~19:30",
//      "19:30~19:45","19:45~20:00","20:00~20:15","20:15~20:30","20:30~20:45","20:45~21:00",
//      "21:00~21:15","21:15~21:30","21:30~21:45","21:45~22:00","22:00~22:15","22:15~22:30",
//      "22:30~22:45","22:45~23:00","23:00~23:15","23:15~23:30","23:30~23:45","23:45~00:00")
val single_StRDD_newNames = Seq( "date","gb_code","slot", "number")
    val single_StRDD_renamed =single_StRDD_DF.toDF(single_StRDD_newNames: _*)
      //single_StRDD_renamed.show()
    single_StRDD_renamed//.coalesce(1)  //设置为一个partition, 这样可以把输出文件合并成一个文件
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .save("/user/sibat/GongAn_analyze/st_status/"+args(0)+"/single_15min_num")


    /** 所有设备15分钟粒度的数据采集数 **/
    val all_15_StRDD = StRDD.map{x=>
      val s = x.mkString("`").replace("[","").replace("]","").split("`")
      val date = tranTimeToString(s(0)).substring(0,10)
      val zeroTimestamp = date +" 00:00:00"
      val slot = (s(0).toLong - tranTimeToLong(zeroTimestamp))/900
      val slotstr = tranTimeToString((tranTimeToLong(zeroTimestamp)+slot*900).toString).substring(11,16) + "~" +tranTimeToString((tranTimeToLong(zeroTimestamp)+(slot+1)*900).toString).substring(11,16)
      ((date,slotstr),1)
    }.reduceByKey(_ + _).map(x=>(x._1._1,x._1._2,x._2))
      //.map(x=>(x._1._1,(x._1._2,x._2))).groupByKey().map(x => (x._1,x._2.toArray.sortBy(x=>x).mkString(" ")))

    val all_15_StRDD_DF = all_15_StRDD.toDF()
    val all_15_StRDD_newNames = Seq( "date","slot",  "number")
    val all_15_StRDD_renamed =all_15_StRDD_DF.toDF(all_15_StRDD_newNames: _*)
    //all_15_StRDD_renamed.show()
    all_15_StRDD_renamed//.coalesce(1)  //设置为一个partition, 这样可以把输出文件合并成一个文件
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .save("/user/sibat/GongAn_analyze/st_status/"+args(0)+"/all_15min_num")

    /** 所有设备每小时粒度的数据采集数 **/
    val all_60_StRDD = StRDD.map{x=>
      val s = x.mkString("`").replace("[","").replace("]","").split("`")
      val date = tranTimeToString(s(0)).substring(0,10)
      val zeroTimestamp = date +" 00:00:00"
      val slot = (s(0).toLong - tranTimeToLong(zeroTimestamp))/3600
      val slotstr = tranTimeToString((tranTimeToLong(zeroTimestamp)+slot*3600).toString).substring(11,16) + "~" +tranTimeToString((tranTimeToLong(zeroTimestamp)+(slot+1)*3600).toString).substring(11,16)
      ((date,slotstr),1)
    }.reduceByKey(_ + _).map(x=>(x._1._1,x._1._2,x._2))
      //.map(x=>(x._1._1,(x._1._2,x._2))).groupByKey().map(x => (x._1,x._2.toArray.sortBy(x=>x).mkString(" ")))

    val all_60_StRDD_DF = all_60_StRDD.toDF()
    val all_60_StRDD_newNames = Seq( "date","slot",  "number")
    val all_60_StRDD_renamed =all_60_StRDD_DF.toDF(all_60_StRDD_newNames: _*)
    //all_60_StRDD_renamed.show()
    all_60_StRDD_renamed//.coalesce(1)  //设置为一个partition, 这样可以把输出文件合并成一个文件
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .save("/user/sibat/GongAn_analyze/st_status/"+args(0)+"/all_60min_num")

    /********************************************需求3、4****************************************************/
    /** 单个设备分天计算延时分布 **/
    val single_parquetRDD = StRDD.map{x=>
      val s = x.mkString("`").replace("[","").replace("]","").split("`")
      val sub = subtract(s(3),s(0).toLong)
      val date = s(3).substring(0,10)
      ((s(7),date),sub)
    }.groupByKey().map(x=>(x._1._1,x._1._2,x._2.toArray.mkString(",")))

    val single_delayDF = single_parquetRDD.toDF()
    val singDelay_newNames = Seq("gb_code", "date", "diff")
    val single_delay = single_delayDF.toDF(singDelay_newNames: _*)
    //single_delay.show()
    single_delay//.coalesce(1)  //设置为一个partition, 这样可以把输出文件合并成一个文件
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .save("/user/sibat/GongAn_analyze/st_status/"+args(0)+"/single_delay")

    /*** 单个设备最小/最大/平均延时 ***/
    val delay_newNames_avg = Seq("gb_code","avg")
    val avg_delayRDD = single_parquetRDD.map(x=>(x._1,x._3.split(",").map(x=>(x.toInt,1)).reduce((x,y)=>(x._1+y._1,x._2+y._2))))
      .map(x=>(x._1,(x._2._1.toDouble/x._2._2.toDouble).formatted("%.2f")))
    val avg_delayDF =avg_delayRDD.toDF().toDF(delay_newNames_avg:_*)

    val delay_newNames_max = Seq("gb_code","max")
    val max_delayRDD = single_parquetRDD.map(x=>(x._1,x._3.split(",").map(x=>x.toInt).max))
    val max_delayDF =max_delayRDD.toDF().toDF(delay_newNames_max:_*)

    val delay_newNames_min = Seq("gb_code","min")
    val min_delayRDD = single_parquetRDD.map(x=>(x._1,x._3.split(",").map(x=>x.toInt).min))
    val min_delayDF =min_delayRDD.toDF().toDF(delay_newNames_min:_*)

/*    avg_delayDF.show()
    max_delayDF.show()
    min_delayDF.show()*/

    avg_delayDF//.coalesce(1)  //设置为一个partition, 这样可以把输出文件合并成一个文件
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .save("/user/sibat/GongAn_analyze/st_status/"+args(0)+"/single_avg_delay")

    max_delayDF//.coalesce(1)  //设置为一个partition, 这样可以把输出文件合并成一个文件
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .save("/user/sibat/GongAn_analyze/st_status/"+args(0)+"/single_max_delay")

    min_delayDF//.coalesce(1)  //设置为一个partition, 这样可以把输出文件合并成一个文件
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .save("/user/sibat/GongAn_analyze/st_status/"+args(0)+"/single_min_delay")

    /*** 所有设备分天计算分布;或用case实现.(2018-07-16，<0秒):10 ***/
    val all_delaygroupRDD = StRDD.map{x=>
      val s = x.mkString("`").replace("[","").replace("]","").split("`")
      val sub = subtract(s(3),s(0).toLong)
      val date = s(3).substring(0,10)
      (date,sub)
    }.groupByKey().map(x=>(x._1,x._2.toArray.mkString(",")))

    val all_delayRDD = all_delaygroupRDD .map { x =>
      val value2split = x._2.split(",")
      var s0 = 0
      var s0_1 = 0
      var s1_2 = 0
      var s2_3 = 0
      var s3_4 = 0
      var s4_5 = 0
      var s5_10 = 0
      var s10_30 = 0
      var s30_60 = 0
      var s60_120 = 0
      var s120_180 = 0
      var s180_240 = 0
      var s240_300 = 0
      var s300 = 0

      for (elem<-value2split){
        val elem2int = elem.toInt

        if (elem2int<0){
          s0+=1
        }else if (elem2int>=0 & elem2int<1){
          s0_1+=1
        }else if (elem2int>=1 & elem2int<2){
          s1_2+=1
        }else if (elem2int>=2 & elem2int<3){
          s2_3+=1
        }else if (elem2int>=3 & elem2int<4){
          s3_4+=1
        }else if (elem2int>=4 & elem2int<5){
          s4_5+=1
        }else if (elem2int>=5 & elem2int<10){
          s5_10+=1
        }else if (elem2int>=10 & elem2int<30){
          s10_30+=1
        }else if (elem2int>=30 & elem2int<60){
          s30_60+=1
        }else if (elem2int>=60 & elem2int<120){
          s60_120+=1
        }else if (elem2int>=120 & elem2int<180){
          s120_180+=1
        }else if (elem2int>=180 & elem2int<240){
          s180_240+=1
        }else if (elem2int>=240 & elem2int<300){
          s240_300+=1
        }else{
          s300+=1
        }
      }
      (x._1,s0,s0_1,s1_2,s2_3,s3_4,s4_5,s5_10,s10_30,s30_60,s60_120,s120_180,s180_240,s240_300,s300)
    }
    val all_delayDF = all_delayRDD.toDF()
    val delay_newNames = Seq("date", "<0秒","0-1秒","1-2秒","2-3秒","3-4秒","4-5秒","5-10秒","10-30秒","30-60秒","60-120秒",
      "120-180秒","180-240秒","240-300秒",">300秒")
    val delay_Renamed = all_delayDF.toDF(delay_newNames: _*)
   // delay_Renamed.show()
    delay_Renamed//.coalesce(1)  //设置为一个partition, 这样可以把输出文件合并成一个文件
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .save("/user/sibat/GongAn_analyze/st_status/"+args(0)+"/all_delay")

    /*** 所有设备延时日均值 ***/
    val alldelay_avg = Seq("date","avg")
    val alldelay_avgRDD = all_delaygroupRDD.map(x=>(x._1,x._2.split(",").map(x=>(x.toInt,1)).reduce((x,y)=>(x._1+y._1,x._2+y._2))))
      .map(x=>(x._1,(x._2._1.toDouble/x._2._2.toDouble).formatted("%.2f")))
    //val alldelay_avgRDD = all_delayRDD.map(x=>(x._1,Seq(x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,x._10,x._11,x._12,x._13,x._14,x._15).max))
    val avg_dealyDF =alldelay_avgRDD.toDF().toDF(alldelay_avg:_*)
    //avg_dealyDF.show()
    avg_dealyDF//.coalesce(1)  //设置为一个partition, 这样可以把输出文件合并成一个文件
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .save("/user/sibat/GongAn_analyze/st_status/"+args(0)+"/all_avg_delay")

    /*** 单个设备分天计算发送频率 ***/
    val single_freqRDD = StRDD.map{x=>
      val s = x.mkString("`").replace("[","").replace("]","").split("`")
      val gbNo = s(7)
      val date = s(3).substring(0,10)
      val receiveTime = s(0)
      ((gbNo,date),receiveTime.toInt)
    }.groupByKey().map(x=>(x._1._1,x._1._2,x._2.toArray.sortBy(x=>x))).map{x=>
      if (x._3.length==1){
        (x._1,x._2,"999")
      }else {
        val len = x._3.length-2
        var fre_list = new ListBuffer[String]

        for (i<-0 to len){
          val fre = (x._3(i+1)-x._3(i)).toString
          fre_list.append(fre)
        }

        (x._1,x._2,fre_list.toArray.mkString(","))
      }
    }//当只有一条数据时，发送频率显示为“999”

    val single_freqDF = single_freqRDD.toDF()
    val newNames = Seq("gb_code", "date", "frequent")
    val dfRenamed = single_freqDF.toDF(newNames: _*)
    /*single_freqRDD.withColumnRenamed("_1","gbNo").printSchema() //单列改列名*/
    dfRenamed//.coalesce(1)  //设置为一个partition, 这样可以把输出文件合并成一个文件
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .save("/user/sibat/GongAn_analyze/st_status/"+args(0)+"/single_frequent")

    /*** 单个设备最小/最大/平均发送频率 ***/
    val newNames_avg = Seq("gb_code","avg")
    val avg_freRDD = single_freqRDD.map(x=>(x._1,x._3.split(",").map(x=>(x.toInt,1)).reduce((x,y)=>(x._1+y._1,x._2+y._2))))
      .map(x=>(x._1,(x._2._1.toDouble/x._2._2.toDouble).formatted("%.2f")))
    val avg_freDF =avg_freRDD.toDF().toDF(newNames_avg:_*)

    val newNames_max = Seq("gb_code","max")
    val max_freRDD = single_freqRDD.map(x=>(x._1,x._3.split(",").map(x=>x.toInt).max))
    val max_freDF =max_freRDD.toDF().toDF(newNames_max:_*)

    val newNames_min = Seq("gb_code","min")
    val min_freRDD = single_freqRDD.map(x=>(x._1,x._3.split(",").map(x=>x.toInt).min))
    val min_freDF =min_freRDD.toDF().toDF(newNames_min:_*)

/*    avg_freDF.show()
    max_freDF.show()
    min_freDF.show()*/
    avg_freDF//.coalesce(1)  //设置为一个partition, 这样可以把输出文件合并成一个文件
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .save("/user/sibat/GongAn_analyze/st_status/"+args(0)+"/single_avg_frequent")

    max_freDF//.coalesce(1)  //设置为一个partition, 这样可以把输出文件合并成一个文件
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .save("/user/sibat/GongAn_analyze/st_status/"+args(0)+"/single_max_frequent")

    min_freDF//.coalesce(1)  //设置为一个partition, 这样可以把输出文件合并成一个文件
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .save("/user/sibat/GongAn_analyze/st_status/"+args(0)+"/single_min_frequent")

    /*** 所有设备分天所有频率 ***/
    val all_fregroupRDD = StRDD.map{x=>
      val s = x.mkString("`").replace("[","").replace("]","").split("`")
      val gbNo = s(7)
      val date = s(3).substring(0,10)
      val receiveTime = s(0)
      ((gbNo,date),receiveTime.toInt)
    }.groupByKey().map(x=>(x._1._1,x._1._2,x._2.toArray.sortBy(x=>x))).map{x =>
      if (x._3.length==1){
        (x._2,List("0"))
      }else {
        val len = x._3.size - 2
        var fre_list = new ListBuffer[String]

        for (i <- 0 to len) {
          val fre = (x._3(i + 1) - x._3(i)).toString
          fre_list.append(fre)
        }
        (x._2, fre_list)
      }
    }.groupByKey().map(x=>(x._1,x._2.flatMap(x=>x).toArray.mkString(",")))

    all_fregroupRDD.persist(StorageLevel.MEMORY_AND_DISK)
    all_fregroupRDD.isEmpty()

    /*** 所有设备分天所有频率分布 ***/
    val all_freRDD = all_fregroupRDD .map { x =>
      val value2split = x._2.split(",")
      var s0_5 = 0
      var s5_10 = 0
      var s10_30 = 0
      var s30_60 = 0
      var s60_120 = 0
      var s120_180 = 0
      var s180_240 = 0
      var s240_300 = 0
      var s300 = 0
      var s999 = 0

      for (elem<-value2split){
        if (elem!=""){
          val elem2int = elem.toInt

          if (elem2int>=0 & elem2int<5){
            s0_5+=1
          }else if (elem2int>=5 & elem2int<10){
            s5_10+=1
          }else if (elem2int>=10 & elem2int<30){
            s10_30+=1
          }else if (elem2int>=30 & elem2int<60){
            s30_60+=1
          }else if (elem2int>=60 & elem2int<120){
            s60_120+=1
          }else if (elem2int>=120 & elem2int<180){
            s120_180+=1
          }else if (elem2int>=180 & elem2int<240){
            s180_240+=1
          }else if (elem2int>=240 & elem2int<300){
            s240_300+=1
          }else{
            s300+=1
          }
        }else{
          s999+=1
        }
      }
      (x._1,s0_5,s5_10,s10_30,s30_60,s60_120,s120_180,s180_240,s240_300,s300,s999)
    }

    val all_freDF = all_freRDD.toDF()
    val fre_newNames = Seq("date", "0-5秒","5-10秒","10-30秒","30-60秒","60-120秒","120-180秒","180-240秒","240-300秒",">300秒","999")
    val fre_Renamed = all_freDF.toDF(fre_newNames: _*)
    //fre_Renamed.show()
    fre_Renamed//.coalesce(1)  //设置为一个partition, 这样可以把输出文件合并成一个文件
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .save("/user/sibat/GongAn_analyze/st_status/"+args(0)+"/all_frequent")
    /* 所有part20180624文件的值
        +----------+------+-----+------+------+-------+--------+--------+--------+-----+
        |      date|  0-5秒|5-10秒|10-30秒|30-60秒|60-120秒|120-180秒|180-240秒|240-300秒|>300秒|
        +----------+------+-----+------+------+-------+--------+--------+--------+-----+
        |2018-07-15|   684|   36|     0|     0|      0|       0|       0|       0|    0|
        |2018-07-16|167978| 1348|     0|     0|      0|     280|       0|     112|   24|
        +----------+------+-----+------+------+-------+--------+--------+--------+-----+*/

    /*** 所有设备频率日均值 ***/
    val allfre_avg = Seq("date","avg")
    val allfre_avgRDD = all_fregroupRDD.map(x=>(x._1,x._2.split(",").map(x=>
      if (x==""){//若该天只有一条数据则返回NaN值
        (0,0)
      }else{
        (x.toInt,1)}
    ).reduce((x,y)=>(x._1+y._1,x._2+y._2))))
      .map(x=>(x._1,(x._2._1.toDouble/x._2._2.toDouble).formatted("%.2f")))
    val allavg_freDF =allfre_avgRDD.toDF().toDF(allfre_avg:_*)
    //allavg_freDF.show()
    allavg_freDF//.coalesce(1)  //设置为一个partition, 这样可以把输出文件合并成一个文件
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .save("/user/sibat/GongAn_analyze/st_status/"+args(0)+"/all_avg_frequent")

    /*   所有part20180624文件的值
     +----------+----+
     |      date| avg|
     +----------+----+
     |2018-07-15|3.00|
     |2018-07-16|3.41|
     +----------+----+*/
  }
}
