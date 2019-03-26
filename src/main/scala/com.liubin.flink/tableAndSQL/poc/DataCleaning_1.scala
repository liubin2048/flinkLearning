/*

//import org.apache.calcite.interpreter.Row
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.sinks.CsvTableSink
//import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.sources.CsvTableSource
//import org.apache.flink.api.common.typeinfo.Types
//import org.apache.flink.api.common.typeinfo.Types
//import org.apache.flink.api.scala._

object DataCleaning_1 {

  def main(args: Array[String]): Unit = {
  print("start table api")
     //case class struct (   unique_transaction_id:String ,description_unmasked :String  )
    //val fieldNames: Array[String] = Array("unique_transaction_id", "description_unmasked")
    //val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.STRING)

    //val csvSource: TableSource = new CsvTableSource()

   val env = ExecutionEnvironment.getExecutionEnvironment
 //  val env = ExecutionEnvironment.createRemoteEnvironment("localhost",8085)
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    import org.apache.flink.api.scala._
    import org.apache.flink.types.Row


//    val keywordMapping= env.readCsvFile[struct]("D:/scala_spark/TDE/PUFQA2/files/bk/keyword.txt").
////    header="merchant_name|KEYWORDS"
//    tableEnv.registerTable("keywordMapping",tableEnv.toDataSet[Row ](keywordMapping))
//         tableEnv.toDataSet[Row ](res).first(3).print(
 val keywordMapping =  CsvTableSource
  .builder
    .path("D:/scala_spark/inputdata/merchat_mapping.txt")
    .field("merchant_name", Types.STRING)
  .field("keywords", Types.STRING)
  .field("rejection_list", Types.STRING)
    .fieldDelimiter("|")  // by default field delimiter is comma ..
  .ignoreFirstLine()
  .build

    tableEnv.registerTableSource("merchant_mapping",keywordMapping)
    val res1 = tableEnv.sqlQuery("select *  from merchant_mapping")

    tableEnv.toDataSet[Row ](res1).first(3).print()


    val ds2 = env.readCsvFile[struct]("D:/scala_spark/inputdata/merchat_mapping.txt")
    val ds1 = env.readTextFile("D:/scala_spark/inputdata/merchat_mapping.txt")
//    val textLines: DataSet[String] = keywordMapping

    import org.apache.flink.api.scala._
//   ds1.flatMap { _.split("|") }.print()
    val ds4= ds1.map(x => x.split('|'))
   val ds5= ds1.flatMap(x => x.split('|')).first(5).print()

    val ds6= ds1.map(x => (x.split('|')(0),x)).first(5).print()

     //class tdef temp(line:String) {print("hello")}
//  ds1.flatMap(flatMapper =)
//    ds1.map(x=> (x.split('|')(0) , ( x.split('|')(1) .split(',')(0,x.split('|')(1) .split(',').length)  )  )).first(5).print()

    ds1.map(x=> (x.split('|')(0) , ( x.split('|')(1) .split(',')(0)  )  )).first(5).print()
//    ds1.map(x=> (x.split('|')(0) , x.split(',')(1).map( y=> y.  )).first(5).print()




    ds1.map(x=> (x.split('|')(0) , ( x.split('|')(1) .split(',')(0)  )  )).first(5).print()

      ds1.flatMap{
        a=>
        val list=a.split('|')
        val firstTerm=list(0)
          print("a-- a",a)
          print("firstterm-- a",firstTerm)
        val secondTermAsList=list(1).split('|')
        secondTermAsList.map{
          x =>   (firstTerm,x)
        }
      }.print()


    println(ds1.flatMap { a =>
      val list = a.split('|')
      val firstTerm = list(0)
      val secondTermAsList = list(1).split(',')
      secondTermAsList.map { b =>
        // val key=if(b>firstTerm) (firstTerm,b) else (b,firstTerm)
        //        val value=secondTermAsList diff List(b)
        //        (key,value)
        (firstTerm, b)
      }
    }.count())

    ds1.flatMap { a =>
      val list = a.split('|')
      val firstTerm = list(0)
      val secondTermAsList = list(1).split(',')
      secondTermAsList.map { b =>
        // val key=if(b>firstTerm) (firstTerm,b) else (b,firstTerm)
        //        val value=secondTermAsList diff List(b)
        //        (key,value)
        (firstTerm, b)
      }
    }.print()

    println(ds1.flatMap { a =>
      val list = a.split('|')
      val firstTerm = list(0)
      val secondTermAsList = list.last.split(',')
      secondTermAsList.map { b =>
        // val key=if(b>firstTerm) (firstTerm,b) else (b,firstTerm)
        //        val value=secondTermAsList diff List(b)
        //        (key,value)
        (firstTerm, b)
      }
    }.count())

    def temp(string: String) {}



    //ds1.flatMap{w=>  w.split("|")}.map {(  _.split(","))} .first(4).print()

//    ds1.map(x=> (x.split('|')(0) ,new MyMapFunction_1().toString )).first(5).print()
//    ds1.flatMap(x=>  x.split("|")).first(5).print()
//    ds1.map(new MyMapFunction()).print()
//    ds1.map(x=> (x.split('|')(0) , x.split('|')(1).foreach()  )).first(5).print()

//    ds1.map ( x => x.split('|').map(_.split(',')).foreach(split => (split(0), split(1)))).first(8).print()
//    ds1.map ( x => x.split('|').map(_.split(',')).foreach(split => (split(0), split(0)))).first(8).print()

    //    ds1.map(x=> (x.split('|')(0) , x.split('|')(1).map(x => x.sp)  )).first(5).print()

//  ds1.map(new MyMapFunction()).print()
//    def parseLine(line:String) {
//    var fields = line.split(",")  // split line into list at comma positions
//    var age = (fields[2])  // extract and typecast relevant fields
//    var  numFriends = (fields[3])
//    return(age, numFriends)
//    }
//    val ds6= ds1.map(parseLine).first(5).print()

//    ds4.print()
//    ds5.print()
System.exit(1)
//
//    var tab = tableEnv.fromDataSet(ds2)
//    tableEnv.registerTable("dstable",tab)
//    val res2 = tableEnv.sqlQuery("select *  from dstable")
//    tableEnv.toDataSet[Row ](res2).first(3).print()
//
//    print("converted dataset into table")
//    print("converted dataset into table ")

//    val words = textLines.flatMap { _.split(" ") }
 //   ds2.map(_.KEYWORDS.split("|"))
//    ds1.map(_.split("|"))
//    ds1.flatMap { _.split("|") }.print()

//    ds1.flatMap { _.split("|") }.groupBy(1).pr
//  ds1.groupBy("merchant_name").re




//    val res1 = tableEnv.sqlQuery("select *  from merchant_mapping")
//val res1 = tableEnv.sqlQuery("select rtrim ( ltrim (a.merchant_name) ) as merchant_name ," +
//  "rtrim (ltrim (keywords)) as  keywords ,rtrim ( ltrim (rejection_list) ) as rejection_list   from " +
//  " (SELECT explode(split(keywords, ',')) as keywords , merchant_name " +
//  "from merchant_mapping )a  left join  (SELECT explode(split(rejection_list, ',')) as rejection_list , " +
//  "merchant_name from merchant_mapping ) b  on a.merchant_name = b.merchant_name")

    //tableEnv.toDataSet[Row ](res1).first(3).print()

//  System.exit(1)

    val csvSource = CsvTableSource
      .builder
      //.path("D:\\flink\\input_data\\test_3col.txt")
//      .path("D:\\flink\\input_data\\test.txt")
      .path("D:\\flink\\input_data\\test2.txt")
      //.path("D:\\flink\\input_data\\test2.txt")
      .field("unique_transaction_id", Types.STRING)
    .field("description_unmasked", Types.STRING)
        .field("tde2_merchant_name",Types.STRING)
      .field("city",Types.STRING)
      .field("state",Types.STRING)
      .field("yodlee_merchant_name",Types.STRING)
      .field("tdev1_merchant_name",Types.STRING)
      .field("dcv4_merchant_name",Types.STRING)
      .field("tde2_last_run",Types.STRING)
      .fieldDelimiter("|")  // by default field delimiter is comma ..
      .ignoreFirstLine()
      .build


    // register the TableSource as table "CsvTable"
    tableEnv.registerTableSource("CsvTable", csvSource)

    // scan registered Orders table
    val orders = tableEnv.scan("CsvTable")

      orders.printSchema()
    println("schema prinnted")


    tableEnv.registerFunction("mask", new maskDescription("40"))
//    tableEnv.registerFunction("hashCode", new HashCode(10))

    val res   = tableEnv.sqlQuery("""select dcv4_merchant_name,tde2_merchant_name,
                                     description_unmasked, mask (description_unmasked)

                                 ,count (1)
      from  CsvTable group by dcv4_merchant_name ,tde2_merchant_name , description_unmasked """)
//      """select  dcv4_merchant_name, tde2_merchant_name,
//        |,count(1) from CsvTable
//        | """.stripMargin)

    res.printSchema()

    import org.apache.flink.api.scala._
    import org.apache.flink.types.Row

//    tableEnv.toDataSet[Row](res).print   // print 1 column
   //tableEnv.toDataSet[Row](orders).print  // print all column
    tableEnv.toDataSet[Row ](orders).first(3).print()

    print("printing the table ")
    tableEnv.toDataSet[Row ](res).first(3).print()



    print("done............")

//      System.exit(1)


    //tableEnv.sqlQuery("");

//    print( "explain ", tableEnv.explain(res))

    // convert the Table into a DataSet of Row
//     tableEnv.toDataSet[Row](res)
//    tableEnv.toDataSet(res)
//    val dsRow: DataSet[Row] = tableEnv.toDataSet[Row](res)
//    print("done", dsRow)
    //val dsRow: DataSet[Row] =

    //val sink= new CsvTableSink("D:\\flink\\output_data\\test_3col_op8.txt")


    res.writeToSink(
      new CsvTableSink(
        "D:\\flink\\output_data\\test_3col_op8.txt",                             // output path
        fieldDelim = "|",                 // optional: delimit files by '|'
        numFiles = 1,                     // optional: write to a single file
        writeMode = WriteMode.OVERWRITE))


    //val result = orders.select("unique_transaction_id").print()






    env.execute()
    //print("result ", result)
  println("over")
    System.exit(1)
//   val orders = tableEnv.scan("CsvTable") // schema (a, b, c, rowtime)
//    orders.select("unique_transaction_id").printSchema()






    print("compiled")


    print("execution done")
    System.exit(1)


//    val data = "D:\\flink\\input_data\\test_3col.txt"
//    val ds = env.readCsvFile[struct](data);
//    //,lineDelimiter = "\n", fieldDelimiter = ":",ignoreFirstLine = false    )
//
//    System.exit(1)
//    //val rds= tableEnv.registerTable("tab",ds.toTable(tableEnv))
//    val rds= tableEnv.registerTable("tab",ds.toTable(tableEnv))
//    val sqlresult =tableEnv.sqlQuery("select * from tab").toDataSet[struct1]
//    sqlresult.print()
//    sqlresult.writeAsCsv("D:\\flink\\output_data\\test_3col_op.txt")
//    env.execute()
    print ("over")
  }

}

import org.apache.flink.table.functions.ScalarFunction
class maskDescription(factor: String) extends ScalarFunction {
  def eval(s: String): String = {

     var  maskedDesc= s.replaceAll("[0-9.,/#!$%^&*;:=_`~()-]"," ")
       .replaceAll("[x]{4,}", "").trim.replaceAll(" +", " ")
    return maskedDesc  ;
  }
}

case class struct (merchant_name:String,KEYWORDS:String,rejection_list:String )

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.functions.RichMapFunction

class MyMapFunction extends RichMapFunction[String, String] { var a = Map[String, String]() // create an empty map
  def map(in: String):String = {
     var str = in.split('|')
    var merchant = str(0)
    var keyword = str(1).split(',')

//      in.split('|').map(_.split(',')).foreach(split => (split(0), split(1)))

     for (x <- keyword)
       {
//        a+=(merchant -> x)
//         print ("map is ----", a+(merchant -> x))
         println("keyword is ", x)
         println("merchant is ", merchant)
           return  merchant+"----"+x;
       }

//     return  str(0)+"---"+str(1)+"---------"+str.last

    //    for (v <- str {
//       println("printed v ",v)
//      println("printed str ", str)
//      return  v
//    }
    //val a = Map[String, String]() // create an empty map

    //a+(""->"")
     //return  a
return  ""
  }
};

class MyMapFunction_1 extends  RichMapFunction[String, String] { var a = Map[String, String]() // create an empty map
def map(in: String):String = {
  var str = in.split('|')
  var merchant = str(0)
  var keyword = str(1).split(',')

  //      in.split('|').map(_.split(',')).foreach(split => (split(0), split(1)))

  for (x <- keyword)
  {
    //        a+=(merchant -> x)
    //         print ("map is ----", a+(merchant -> x))
    println("keyword is ", x)
    println("merchant is ", merchant)
    return  (merchant+"----"+x);
  }

  //     return  str(0)+"---"+str(1)+"---------"+str.last

  //    for (v <- str {
  //       println("printed v ",v)
  //      println("printed str ", str)
  //      return  v
  //    }
  //val a = Map[String, String]() // create an empty map

  //a+(""->"")
  //return  a
  return  ""
}
};

import org.apache.flink.api.common.functions.FlatMapFunction
//class FlatMapper1 extends FlatMapFunction [T value, Collector<O> out] {
//  private static final long serialVersionUID = 1L;
//
//  @Override
//  public void flatMap(String value, Collector<String> out) throws Exception {
//    if ( value.contains("bananas") ) {
//      out.collect(value);
//    }
//  }
//}*/
