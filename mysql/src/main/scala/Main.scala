package main.mysql

import main.mysql.config.{Credentials}
import main.mysql.connector._
import main.mysql.spark._

object Main extends App {
    println("Hello, World!")
    val credentials = Credentials("127.0.0.1",3306,"root","admin")

    // val mysql = new Mysql(credentials)
    // mysql.insert("/Users/aezo.teo/Documents/stash/test/data_usage.csv", "aezo.testing")
    // mysql.closeConnection

    val sparksql = new SparkMysql(credentials)
    sparksql.insertSparkCsv("/Users/aezo.teo/Documents/stash/test/data_usage.csv", "aezo.testing")
    sparksql.stop

}