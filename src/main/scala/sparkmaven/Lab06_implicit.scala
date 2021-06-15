package sparkmaven

object Lab06_implicit {
  
  def main(args:Array[String])=
  {
    
    def addnum(implicit a:Int,b:Float) = a + b
    
    implicit val x = 10
    implicit val f = 20.0f
    
    println(addnum)
    
    implicit def fromstr2int(a:String)=
    {
      10
    } 
    
    val c:Int = "5"
    
    println(c)
    
    val str = "Hello"
    
    println(str.upcase)
     
  }
  
  
  
 
  
  implicit class sops(s:String)
   {
     def upcase = s.toUpperCase()
     def lowcase = s.toLowerCase()
   }
 
  
}