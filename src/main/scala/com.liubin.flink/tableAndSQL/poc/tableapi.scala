package com.liubin.flink.tableAndSQL.poc

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object tableapi {

  case class struct (   unique_transaction_id:String ,description_unmasked :String  )

  def main(args: Array[String]): Unit = {
  print("start table api")


    import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
    val fieldNames: Array[String] = Array("unique_transaction_id", "description_unmasked")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.STRING)

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val data = "D:\\input.txt"
    val ds = env.readCsvFile[struct](data);
    //,lineDelimiter = "\n", fieldDelimiter = ":",ignoreFirstLine = false    )
//    System.exit(1)
    //val rds= tableEnv.registerTable("tab",ds.toTable(tableEnv))
    val rds= tableEnv.registerTable("tab",ds.toTable(tableEnv))
    val sqlresult =tableEnv.sqlQuery("select * from tab").toDataSet[struct]
//    sqlresult.print()
    sqlresult.writeAsCsv("D:\\output.txt").setParallelism(1)
    env.execute()

    print ("over")
  }

}
