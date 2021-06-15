package sparkmaven

import java.sql.{DriverManager}

object Lab01 {
   
  def main(args:Array[String]):Unit=
  {
    Class.forName("com.mysql.cj.jdbc.Driver")  
    val con=DriverManager.getConnection("jdbc:mysql://localhost/custdb","root","Root123$") 
    val stmt=con.createStatement()
    val rs=stmt.executeQuery("select * from customer") 
    while(rs.next())  
      println(rs.getInt(1) + "  " + rs.getString(2) + "  "+ rs.getString(3))
      
    //stmt.execute("select * from customer")
    con.close();     
  }
}