package com
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.functions._


object boston_crimes extends App {
  val spark: SparkSession = {
    SparkSession.builder().master("local").appName("JsonReader_K").getOrCreate()
  }
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val crimes_csv = args(0)
  val offence_csv = args(1)
  val output_folder = args(2)
  print(crimes_csv, offence_csv)

  val crimeFacts = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(crimes_csv)
  crimeFacts.createOrReplaceTempView("crimeFacts")
  val offenseCodes = spark.read.option("header", "true").option("inferSchema", "true").csv(offence_csv).withColumn("Type", split(col("NAME"),"-")(0))
  val offenseCodesBroadcast = broadcast(offenseCodes)
  offenseCodesBroadcast.createOrReplaceTempView("offenseCodes")

  val df_final = spark.sql("""
with stg1 as (
    select district, year, month, count(*) as cnt_per_month
    from crimeFacts
    group by district, year, month
),
     stg2 as (
         select district, count(*) as cnt_all, avg(Lat) as avgLat, avg(Long) as avgLong
         from crimeFacts
         group by district
     ),
     stg3 as
         (select district, Type, count(*) as cnt
          from crimeFacts cr
                   left join offenseCodes codes on codes.code = cr.offense_code
          group by district, Type
          order by cnt desc),
     stg4 as (
         select district, Type, row_number() over (PARTITION BY district order by cnt desc ) as rn
         from stg3
     ),
     stg5 as (
         select collect_list(stg4.Type) as frequent_crime_types, district
         from stg4
         where rn < 4
         group by district
     )
select cr.district, cnt_all as crimes_total, percentile_approx(cnt_per_month,0.5) as crimes_monthly, frequent_crime_types,
avg(avgLat) as lat, avg(avgLong) as lng
from stg1 cr
         left join stg2 on stg2.district = cr.district
         left join stg5 on stg5.district = cr.district
group by cr.district, cnt_all, frequent_crime_types
""")
  //print(df_final.count())
  df_final.repartition(1).write.mode("overwrite").parquet(output_folder)

}


/*spark-submit --master local[*] --class com.boston_crimes  C:\Users\Yestay\IdeaProjects\boston_crimes\target\scala-2.11\Boston_Crimes-assembly-0.0.1.jar  C:\Users\Yestay\\Desktop\Otus\HW\crime.csv C:\Users\Yestay\Desktop\Otus\HW\offense_codes.csv C:\Users\Yestay\Desktop\Otus\HW\1\
 {path/to/crime.csv} {path/to/offense_codes.csv} {path/to/output_folder}

 * */