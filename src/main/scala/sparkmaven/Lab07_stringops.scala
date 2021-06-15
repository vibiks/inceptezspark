package sparkmaven

object Lab07_stringops {
  def main(args:Array[String])=
  {
     val nm = "ab5c7d2"
     println(nm)
     var tmp = ""
     var result = ""
     for(l <- nm)
     {
       if(l.isDigit)
       {
          println(l.toString().toInt)
          for( c <- 1 until l.toString().toInt)
          {
            result = result + tmp
          }
       }
       else
         result = result + l
       
       tmp = l.toString()
       
     }
     println(result)
  }
}