import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object ReadCSV extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("TechTask")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  //Read customer.csv
  val customerDFSchem = new StructType()
    .add("id", IntegerType, true)
    .add("name", StringType, true)
    .add("email", StringType, true)
    .add("joinDate", DateType, true)
    .add("status", StringType, true)

  val customerdf = spark.read.option("header", false)
    .option("delimiter", "\t")
    .format("csv")
    .schema(customerDFSchem)
    .load("src/main/resources/customer.csv").cache()


  customerdf.show()
  customerdf.printSchema()

  //Read product.csv
  val productDFSchem = new StructType()
    .add("id", IntegerType, true)
    .add("name", StringType, true)
    .add("price", DoubleType, true)
    .add("numberOfProduct", IntegerType, true)

  val productdf = spark.read.option("header", false)
    .option("delimiter", "\t")
    .format("csv")
    .schema(productDFSchem)
    .load("src/main/resources/product.csv").cache()


  productdf.show()
  productdf.printSchema()


  //Read order.csv
  val orderDFSchem = new StructType()
    .add("customerID", IntegerType, true)
    .add("orderID", IntegerType, true)
    .add("productID", IntegerType, true)
    .add("numberOfProduct", IntegerType, true)
    .add("orderDate", DateType, true)
    .add("status", StringType, true)


  val orderdf = spark.read.option("header", false)
    .option("delimiter", "\t")
    .format("csv")
    .schema(orderDFSchem)
    .load("src/main/resources/order.csv").cache()


  orderdf.show()
  orderdf.printSchema()


  //поиск количества заказов товара у каждого клиента
  val prodcountDF = orderdf.where("status == 'delivered'")
    .groupBy("customerID", "productID").count().orderBy("customerID")
  prodcountDF.show()


  val windowspec = Window.partitionBy("custID")


  val resultDM = prodcountDF
    .join(orderdf, prodcountDF("customerID") === orderdf("customerID") &&
      prodcountDF("productID") === orderdf("productID"), "inner")
    .withColumn("custID", prodcountDF("customerID"))
    .where("status == 'delivered'") //остаются только успешные заказы
    .drop("orderDate", "status")
    .withColumn("fullorder", col("count") * col("numberOfProduct")) //Кол-во проданного товара в заказе клиента
    .join(customerdf, customerdf("ID") === prodcountDF("customerID"), "inner")
    .join(productdf, productdf("ID") === prodcountDF("productID"), "inner")
    .where("status == 'active'") //оставются только активные клиенты
    .withColumn("max", max("fullorder").over(windowspec)) //Поиск товара с максимальными продажами по клиенту
    .where("fullorder == max")
    .select(customerdf("name").as("customer.name"), productdf("name").as("product.name")).distinct()


  resultDM.show()
  resultDM.coalesce(1)
    .write
    .option("header", true)
    .option("delimiter", "\t")
    .csv("src/main/resources/result")


}
