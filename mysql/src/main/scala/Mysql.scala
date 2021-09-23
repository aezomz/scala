package main.mysql.connector

import main.mysql.config.{Credentials}

import java.sql.{Connection,DriverManager,PreparedStatement}
import scala.collection.mutable.ArrayBuffer


class Mysql (credentials: Credentials) {
    // connect to the database named "mysql" on port 8889 of localhost
    val url = f"jdbc:mysql://${credentials.host}:${credentials.port}/aezo"
    val driver = "com.mysql.jdbc.Driver"
    val username: String = credentials.username
    val password: String = credentials.password
    var connection:Connection = _
    try {
        println(Class.forName(driver))
        Class.forName(driver)
        connection = DriverManager.getConnection(url, username, password)
        val statement = connection.createStatement
        val rs = statement.executeQuery("SHOW SCHEMAS")
        while (rs.next) {
            val database = rs.getString("Database")
            println("database = %s".format(database))
        }
    } catch {
        case e: Exception => e.printStackTrace
    }
    
    def insert(filePath: String, table: String):Unit ={
        // each row is an array of strings (the columns in the csv file)
        val rows = ArrayBuffer[Array[String]]()
        val insertSql =s"""
                          |replace into $table (id,name,type)
                          |values (?,?,?)
                        """.stripMargin
        println(insertSql)                
        val preparedStmt: PreparedStatement = connection.prepareStatement(insertSql)
        using(scala.io.Source.fromFile(filePath)) { source =>
            for (line <- source.getLines) {                
                rows += line.split(",").map(_.trim)
            }
        }

        for (row <- rows) {
            preparedStmt.setInt(1, row(0).toInt)
            preparedStmt.setString(2, row(1))
            preparedStmt.setString(3, row(2))
            preparedStmt.executeUpdate()
            println(s"${row(0)}|${row(1)}|${row(2)}")
        }
        def using[A <: { def close(): Unit }, B](resource: A)(f: A => B): B =
            try {
                f(resource)
            } finally {
                resource.close()
            }
    }
    def closeConnection:Unit ={
        connection.close
    }


}