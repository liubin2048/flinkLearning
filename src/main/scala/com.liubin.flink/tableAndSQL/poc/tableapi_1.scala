/*

//import org.apache.calcite.interpreter.Row
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.{Table, TableEnvironment, Types}
import org.apache.flink.table.sinks.{CsvTableSink, TableSink}

import org.apache.flink.table.api.scala._
//import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.table.sources.TableSource
//import org.apache.flink.api.common.typeinfo.Types
//import org.apache.flink.api.common.typeinfo.Types

object tableapi_1 {

  def main(args: Array[String]): Unit = {
  print("start table api")
     //case class struct (   unique_transaction_id:String ,description_unmasked :String  )
    //val fieldNames: Array[String] = Array("unique_transaction_id", "description_unmasked")
    //val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.STRING)

    //val csvSource: TableSource = new CsvTableSource()

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val csvSource = CsvTableSource
      .builder
      .path("D:\\flink\\input_data\\test_3col.txt")
      //.path("D:\\flink\\input_data\\test2.txt")
      .field("unique_transaction_id", Types.STRING)
      .field("description_unmasked", Types.STRING)
      .fieldDelimiter("|")  // by default field delimiter is comma ..

      .build


    // register the TableSource as table "CsvTable"
    tableEnv.registerTableSource("CsvTable", csvSource)

    // scan registered Orders table
    val orders = tableEnv.scan("CsvTable")

    val res   = tableEnv.sqlQuery("select  unique_transaction_id from CsvTable ")

    res.printSchema()

    import org.apache.flink.types.Row

    import org.apache.flink.table.api.scala._
   import org.apache.flink.api.scala._

//    tableEnv.toDataSet[Row](res).print   // print 1 column
//    tableEnv.toDataSet[Row](orders).print  // print all column
    tableEnv.toDataSet[Row ](orders).first(2).print()
    print("done............")
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
    import org.apache.flink.api.scala._
    import org.apache.flink.table.api.scala._






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
*/
