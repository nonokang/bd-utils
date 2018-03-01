package org.bd.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bd.spark.enums.FormatType;

import scala.Tuple2;

public class WordeCountTest {

	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "F:\\hadoop-common-2.2.0-bin-master");
		SparkConf conf = new SparkConf();
		conf.setAppName("readTest");
		conf.setMaster("local[2]");
		conf.set("spark.some.config.option", "some-value");
		SparkSession spark = SessionDrive.getInstance().getSparkSession(conf);
		//统计文件字符长度
//		calculateFileLength1(spark);
		
		calculateFileLength2(spark);
	}
	
	/**
	 * <b>描述：统计文件字符长度</b>
	 * @author wpk | 2017年7月26日 下午10:56:20 |创建
	 * @param spark
	 * @throws Exception
	 * @return void
	 */
	public static void calculateFileLength1(SparkSession spark) throws Exception{
		Dataset<Row> txt = ReadComm.getInstance().
				readSource(spark, FormatType.TEXT, "D:/wpk/devToll/workspace/nkzjProject1/idata-spark/test-file/people.txt");
		JavaRDD<Row> jr = txt.javaRDD();
		JavaRDD<String> sd = jr.map((Function<Row,String>) row -> row.getAs(0).toString()+":"+row.getAs(0).toString().length());
		int lines = jr.map((Function<Row,Integer>) row -> 1).reduce((Function2<Integer,Integer,Integer>) (i,j) -> i+j).intValue();
		int count = jr.map((Function<Row,Integer>) row -> row.getAs(0).toString().length()).reduce((Function2<Integer,Integer,Integer>) (i,j) -> i+j).intValue();
		System.out.println("总行数："+lines);
		sd.foreach(n -> System.out.println(n));
		System.out.println("总字符长度："+count);
	}
	
	/**
	 * <b>描述：统计文件字符长度并转换为Dataset</b>
	 * @author wpk | 2017年7月26日 下午11:21:14 |创建
	 * @param spark
	 * @throws Exception
	 * @return void
	 */
	public static void calculateFileLength2(SparkSession spark) throws Exception{
		Dataset<Row> txt = ReadComm.getInstance().
				readSource(spark, FormatType.TEXT, "C:/Users/Administrator/Desktop/target.txt");
		JavaRDD<Row> jr = txt.javaRDD();
		//分词
		JavaRDD<String> words = jr.flatMap((FlatMapFunction<Row, String>) row -> {
			String[] str = row.getAs(0).toString().split("\\|");
			return Arrays.asList(str).iterator();
			});
		//分词设值
		JavaPairRDD<String, Integer> pairs = words.mapToPair((PairFunction<String, String, Integer>) w -> new Tuple2<String,Integer>(w,1));
		//统计词数
		JavaPairRDD<String, Integer> wordsCount = pairs.reduceByKey((Function2<Integer, Integer, Integer>) (i,j)-> i+j);
		//迭代
		/*wordsCount.foreach((VoidFunction<Tuple2<String,Integer>>) pair -> System.out.println(pair._1+":"+pair._2));*/
		
		//JavaPairRDD转JavaRDD
		JavaRDD<String> wordsRDD = wordsCount.map((Function<Tuple2<String,Integer>,String>) row -> row._1+","+row._2);
		
		String schemaString = "name age";
		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<>();
		for (String fieldName : schemaString.split(" ")) {
		  StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
		  fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);
		
		//JavaRDD<String>转JavaRDD<Row>
		JavaRDD<Row> rowRDD = wordsRDD.map((Function<String, Row>) record -> {
			  String[] attributes = record.split(",");
			  return RowFactory.create(attributes[0], attributes[1].trim());
			});
		
		//JavaRDD<Row>转Dataset<Row>
		Dataset<Row> dsFrame = spark.createDataFrame(rowRDD, schema);
		dsFrame.filter("age > 20").show();
		dsFrame.show();
		
	}

}
