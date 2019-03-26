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

object DataCleaning {

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



//
//    var tab = tableEnv.fromDataSet(ds2)
//    tableEnv.registerTable("dstable",tab)
//    val res2 = tableEnv.sqlQuery("select *  from dstable")
//    tableEnv.toDataSet[Row ](res2).first(3).print()
//
//    print("converted dataset into table")
//    print("converted dataset into table ")
//System.exit(1)
//    val words = textLines.flatMap { _.split(" ") }
    ds2.map(_.KEYWORDS.split("|"))
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

case class struct (merchant_name:String,KEYWORDS:String,rejection_list:String )*/
