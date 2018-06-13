import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

object ReturnTrips {
def compute(trips : Dataset[Row], dist : Double, spark : SparkSession) : Dataset[Row] = {

    import spark.implicits._

    val R = 6371; //radius in km
 
	def haversine(lat1:Double, lon1:Double, lat2:Double, lon2:Double)={
      val dLat=(lat2 - lat1).toRadians
      val dLon=(lon2 - lon1).toRadians
 
      val a = Math.pow(Math.sin(dLat/2),2) + Math.pow(Math.sin(dLon/2),2) * Math.cos(lat1.toRadians) * Math.cos(lat2.toRadians)
      val c = 2 * Math.asin(Math.sqrt(a))
      R * c * 1000
	}

	val interval = (dist*0.001)/111.2;

	lazy val trips_data = trips.select('tpep_pickup_datetime,'tpep_dropoff_datetime,'pickup_longitude,'pickup_latitude,'dropoff_longitude,'dropoff_latitude);

    lazy val distance_func = udf(haversine _)

	lazy val trips_time = trips_data.withColumn("pickup_time",unix_timestamp($"tpep_pickup_datetime")).withColumn("drop_time",unix_timestamp($"tpep_dropoff_datetime"));

    lazy val trips_bucket = trips_time.withColumn("Bucket_pickup_time",floor($"pickup_time"/(28800))).withColumn("Bucket_drop_time",floor($"drop_time"/(28800))).withColumn("Bucket_pickup_lat",floor($"pickup_latitude"/interval)).withColumn("Bucket_drop_lat",floor($"dropoff_latitude"/interval));

	lazy val trips_bucketNeighbors = trips_bucket.withColumn("Bucket_pickup_time", explode(array($"Bucket_pickup_time" - 1, $"Bucket_pickup_time"))).withColumn("Bucket_pickup_lat", explode(array($"Bucket_pickup_lat" - 1, $"Bucket_pickup_lat", $"Bucket_pickup_lat" + 1))).withColumn("Bucket_drop_lat",explode(array($"Bucket_drop_lat" - 1, $"Bucket_drop_lat", $"Bucket_drop_lat" + 1)));

	lazy val return_trips = trips_bucket.as("a").join(trips_bucketNeighbors.as("b"),
	($"b.Bucket_pickup_lat" === $"a.Bucket_drop_lat")
	&& ($"a.Bucket_pickup_lat" === $"b.Bucket_drop_lat")
	&& ($"a.Bucket_drop_time" === $"b.Bucket_pickup_time") 
	&& (distance_func($"a.dropoff_latitude",$"a.dropoff_longitude",$"b.pickup_latitude",$"b.pickup_longitude") < dist)
	&& (distance_func($"b.dropoff_latitude",$"b.dropoff_longitude",$"a.pickup_latitude",$"a.pickup_longitude") < dist)
	&& ($"b.pickup_time" > $"a.drop_time")
	&& (($"a.drop_time" + 28800) > $"b.pickup_time"));

    return_trips
  }
}
