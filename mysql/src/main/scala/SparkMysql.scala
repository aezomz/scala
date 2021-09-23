package main.mysql.spark

import main.mysql.config.{Credentials}

import org.apache.spark.sql.{DataFrameWriter, DataFrameReader, SparkSession}
// import org.apache.spark.{SparkConf, SparkContext}
import java.util.Properties

class SparkMysql (credentials: Credentials) {
    val url = f"jdbc:mysql://${credentials.host}:${credentials.port}/aezo"
    val spark = SparkSession
        .builder()
        .appName("SparkMysql")
        .config("spark.master", "local")
        .getOrCreate()
    val connectionProperties = new Properties()
    connectionProperties.setProperty("driver", "com.mysql.jdbc.Driver") 
    connectionProperties.setProperty("user", credentials.username)
    connectionProperties.setProperty("password", credentials.password) 
    
    def insertSparkCsv(filePath: String, table: String):Unit ={         
        // spark.read.format("csv").option("header", "false").load(filePath).show()
        spark.read.format("csv").option("header", "false").load(filePath).toDF("id","name","type")
        .write.mode("append")
        .jdbc(url, table, connectionProperties)
              
    }
    def stop: Unit = {
        spark.stop()
    }


}