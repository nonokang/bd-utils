package org.bd.spark.mllib;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.ChiSqSelector;
import org.apache.spark.ml.feature.ChiSqSelectorModel;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class SVM {

	String libsvmFile = "C:/Users/Administrator/Desktop/MachineLearning-master/data/sample/sample_libsvm_data.txt";
	String scalerModelPath = "C:/Users/Administrator/Desktop/MachineLearning-master/data/sample/scalerModel";
	String featureSelectedModelPath = "C:/Users/Administrator/Desktop/MachineLearning-master/data/sample/featuresSelectionModel";
	String mlModelPath = "C:/Users/Administrator/Desktop/MachineLearning-master/data/sample/mlModelPath";
	String predictDataPath = "C:/Users/Administrator/Desktop/MachineLearning-master/data/sample/sample_test.txt";
	
	

	public static void main(String[] agrs) throws Exception {
		System.setProperty("hadoop.home.dir", "F:\\hadoop-common-2.2.0-bin-master");
		SVM svm = new SVM();
//		svm.trainModel();
//		System.out.println("model is ok");
		svm.predictData();
		System.out.println("predict is ok");
	}
	
	public void  predictData() throws Exception {
		//初始化spark
		SparkConf conf = new SparkConf().setAppName("SVM").setMaster("local");
		conf.set("spark.testing.memory", "2147480000");
		SparkContext sc = new SparkContext(conf);
		
		//加载测试数据
		JavaRDD<LabeledPoint> testData = MLUtils.loadLibSVMFile(sc, this.predictDataPath).toJavaRDD();
		
		//转化DataFrame数据类型
		JavaRDD<Row> jrow =testData.map(new LabeledPointToRow());
		StructType schema = new StructType(new StructField[]{
					new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
					new StructField("features", new VectorUDT(), false, Metadata.empty()),
		});
		SQLContext jsql = new SQLContext(sc);
//		DataFrame df = jsql.createDataFrame(jrow, schema);
		Dataset<Row> df = jsql.createDataFrame(jrow, schema);
		
		//数据规范化
		StandardScaler scaler = StandardScaler.load(this.scalerModelPath);
//		DataFrame scalerDF = scaler.fit(df).transform(df);
		Dataset<Row> scalerDF = scaler.fit(df).transform(df);
		
		//特征选取
		ChiSqSelectorModel chiModel = ChiSqSelectorModel.load( this.featureSelectedModelPath);
//		DataFrame selectedDF = chiModel.transform(scalerDF).select("label", "selectedFeatures");
		Dataset<Row> selectedDF = chiModel.transform(scalerDF).select("label", "selectedFeatures");
		
		//转化为LabeledPoint数据类型， 训练模型
		JavaRDD<Row> selectedrows = selectedDF.javaRDD();
		JavaRDD<LabeledPoint> testset = selectedrows.map(new RowToLabel());
		
		//预测模型
		SVMModel svmmodel = SVMModel.load(sc, this.mlModelPath);
		JavaRDD<Tuple2<Double, Double>> predictResult = testset.map(new Prediction(svmmodel)) ;
		predictResult.collect();
		
		//计算准确率
		double accuracy = predictResult.filter(new PredictAndScore()).count() * 1.0 / predictResult.count();
		System.out.println(accuracy);
		sc.stop();
	}
	
	public void trainModel() throws Exception{
		//初始化spark
		SparkConf conf = new SparkConf().setAppName("SVM").setMaster("local");
		conf.set("spark.testing.memory","2147480000");
		SparkContext sc = new SparkContext(conf);
		
		//加载libsvm文件, 使用MLUtils包
		JavaRDD<LabeledPoint> lpdata = MLUtils.loadLibSVMFile(sc, this.libsvmFile).toJavaRDD();
		
		//转化DataFrame数据类型
		JavaRDD<Row> jrow = lpdata.map(new LabeledPointToRow());
		StructType schema = new StructType(new StructField[]{
					new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
					new StructField("features", new VectorUDT(), false, Metadata.empty()),
		});
		SQLContext jsql = new SQLContext(sc);
//		DataFrame df = jsql.createDataFrame(jrow, schema);
		Dataset<Row> df = jsql.createDataFrame(jrow, schema);
		
		//数据规范化
		StandardScaler scaler = new StandardScaler().setInputCol("features").setOutputCol("normFeatures").setWithStd(true);
//		DataFrame scalerDF = scaler.fit(df).transform(df);
		Dataset<Row> scalerDF = scaler.fit(df).transform(df);
		scaler.save(this.scalerModelPath);
		
		//利用卡方统计做特征提取, 保存特征选择模型结果
		ChiSqSelector selector = new ChiSqSelector().setNumTopFeatures(500).setFeaturesCol("normFeatures").setLabelCol("label").setOutputCol("selectedFeatures");
		ChiSqSelectorModel chiModel = selector.fit(scalerDF);
//		DataFrame selectedDF = chiModel.transform(scalerDF).select("label", "selectedFeatures");
		Dataset<Row> selectedDF = chiModel.transform(scalerDF).select("label", "selectedFeatures");
		chiModel.save(this.featureSelectedModelPath);
		
		//转化为LabeledPoint数据类型， 训练模型
		JavaRDD<Row> selectedrows = selectedDF.javaRDD();
		JavaRDD<LabeledPoint> trainset = selectedrows.map(new RowToLabel());
		
		//训练SVM模型, 并保存
		int numIteration = 200;
		SVMModel model = SVMWithSGD.train(trainset.rdd(), numIteration);
		model.clearThreshold();
		model.save(sc, this.mlModelPath);
		sc.stop();
	}
	
	// LabeledPoint数据类型转化为Row
	static class LabeledPointToRow implements Function<LabeledPoint, Row> {
		
		public Row call(LabeledPoint p) throws Exception {
			double label = p.label();
			Vector vector = p.features();
			return RowFactory.create(label, vector);
		}
	}
	
	//Rows数据类型转化为LabeledPoint
	static class RowToLabel implements Function<Row, LabeledPoint> {
		
		public LabeledPoint call(Row r) throws Exception {
			Vector features = r.getAs(1);
			double label = r.getDouble(0);
			return new LabeledPoint(label, features);
		}
	}

	static class Prediction implements Function<LabeledPoint, Tuple2<Double , Double>> {
		SVMModel model;
		public Prediction(SVMModel model){
			this.model = model;
		}
		public Tuple2<Double, Double> call(LabeledPoint p) throws Exception {
			Double score = model.predict(p.features());
			return new Tuple2<Double , Double>(score, p.label());
		}
	}
	
	static class PredictAndScore implements Function<Tuple2<Double, Double>, Boolean> {
		public Boolean call(Tuple2<Double, Double> t) throws Exception {
			double score = t._1();
			double label = t._2();
			System.out.print("score:" + score + ", label:"+ label);
			if(score >= 0.0 && label >= 0.0) return true;
			else if(score < 0.0 && label < 0.0) return true;
			else return false;
		}
	}
	
}
