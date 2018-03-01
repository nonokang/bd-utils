package org.bd.spark.mllib;

// $example on$
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.mllib.util.MLUtils;
import org.netlib.util.doubleW;

import scala.Tuple2;
import shapeless.newtype;
import sun.security.pkcs11.P11TlsKeyMaterialGenerator;


// $example off$

public class GBDT {
	
	String trainsetFile = "C:/Users/Administrator/Desktop/MachineLearning-master/data/sample/sample_libsvm_data.txt";
	String modelpath = "C:/Users/Administrator/Desktop/MachineLearning-master/data/sample/gbdtModel";
	String predictFile = "C:/Users/Administrator/Desktop/MachineLearning-master/data/sample/sample_libsvm_data.txtt";
	
	public void trainModel(){
	
		//初始化spark
		SparkConf conf = new SparkConf().setAppName("GBDT").setMaster("local");
		conf.set("spark.testing.memory","2147480000");
		SparkContext sc = new SparkContext(conf);
		
		//加载训练文件, 使用MLUtils包
		JavaRDD<LabeledPoint> lpdata = MLUtils.loadLibSVMFile(sc, this.trainsetFile).toJavaRDD();
		
		//训练模型, 默认情况下使用均值方差作为阈值标准
		int numIteration = 10; 	//boosting提升迭代的次数
		int maxDepth = 3;		//回归树的最大深度
		BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams("Regression");
		boostingStrategy.setNumIterations(numIteration);
		boostingStrategy.getTreeStrategy().setMaxDepth(maxDepth);
		//记录所有特征的连续结果
		Map<Integer, Integer> categoricalFeaturesInfoMap = new HashMap<Integer, Integer>();
		boostingStrategy.treeStrategy().setCategoricalFeaturesInfo(categoricalFeaturesInfoMap);
		//gdbt模型
		final GradientBoostedTreesModel model = GradientBoostedTrees.train(lpdata, boostingStrategy);
		model.save(sc, modelpath);
		sc.stop();
	}
	
	public void predict() {
		//初始化spark
		SparkConf conf = new SparkConf().setAppName("GBDT").setMaster("local");
		conf.set("spark.testing.memory","2147480000");
		SparkContext sc = new SparkContext(conf);
		
		//加载gbdt模型
		final GradientBoostedTreesModel model = GradientBoostedTreesModel.load(sc, this.modelpath);
		
		//加载测试文件
		JavaRDD<LabeledPoint> testData = MLUtils.loadLibSVMFile(sc, this.predictFile).toJavaRDD();
		testData.cache();
	
		
		//预测数据
		JavaRDD<Tuple2<Double, Double>>  predictionAndLabel = testData.map(new Prediction(model)) ;
		
		//计算所有数据的平均值方差
		 Double testMSE = predictionAndLabel.map(new CountSquareError()).reduce(new ReduceSquareError()) / testData.count();
		 System.out.println("testData's MSE is : " + testMSE);
		 sc.stop();
	}
	
	
	public static void main(String[] args) {
		GBDT gbdt = new GBDT();
//		gbdt.trainModel();
		gbdt.predict();
		System.out.println("ok");
	}
	
	static class Prediction implements Function<LabeledPoint, Tuple2<Double , Double>> {
		GradientBoostedTreesModel model;
		public Prediction(GradientBoostedTreesModel model){
			this.model = model;
		}
		public Tuple2<Double, Double> call(LabeledPoint p) throws Exception {
			Double score = model.predict(p.features());
			return new Tuple2<Double , Double>(score, p.label());
		}
	}
	
	static class CountSquareError implements Function<Tuple2<Double, Double>, Double> {
		public Double call (Tuple2<Double, Double> pl) {
			double diff = pl._1() - pl._2();
			return diff * diff;
		}
	}
	
	static  class ReduceSquareError implements Function2<Double, Double, Double> {
		public Double call(Double a , Double b){
			return a + b ;
		}
	}
	
  public static void test(String[] args) {
		System.setProperty("hadoop.home.dir", "F:\\hadoop-common-2.2.0-bin-master");
    // $example on$
    SparkConf sparkConf = new SparkConf()
      .setAppName("JavaGradientBoostedTreesRegressionExample").setMaster("local");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    // Load and parse the data file.
    String datapath = "C:/Users/Administrator/Desktop/MachineLearning-master/data/sample/sample_libsvm_data.txt";
    JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(jsc.sc(), datapath).toJavaRDD();
    // Split the data into training and test sets (30% held out for testing)
    JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
    JavaRDD<LabeledPoint> trainingData = splits[0];
    JavaRDD<LabeledPoint> testData = splits[1];

    // Train a GradientBoostedTrees model.
    // The defaultParams for Regression use SquaredError by default.
    BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams("Regression");
    boostingStrategy.setNumIterations(3); // Note: Use more iterations in practice.
    boostingStrategy.getTreeStrategy().setMaxDepth(5);
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
    boostingStrategy.treeStrategy().setCategoricalFeaturesInfo(categoricalFeaturesInfo);

    final GradientBoostedTreesModel model =
      GradientBoostedTrees.train(trainingData, boostingStrategy);

    // Evaluate model on test instances and compute test error
    JavaPairRDD<Double, Double> predictionAndLabel =
      testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
        @Override
        public Tuple2<Double, Double> call(LabeledPoint p) {
          return new Tuple2<>(model.predict(p.features()), p.label());
        }
      });
    Double testMSE =
      predictionAndLabel.map(new Function<Tuple2<Double, Double>, Double>() {
        @Override
        public Double call(Tuple2<Double, Double> pl) {
          Double diff = pl._1() - pl._2();
          return diff * diff;
        }
      }).reduce(new Function2<Double, Double, Double>() {
        @Override
        public Double call(Double a, Double b) {
          return a + b;
        }
      }) / data.count();
    System.out.println("Test Mean Squared Error: " + testMSE);
    System.out.println("Learned regression GBT model:\n" + model.toDebugString());

    // Save and load model
    model.save(jsc.sc(), "target/tmp/myGradientBoostingRegressionModel1");
    GradientBoostedTreesModel sameModel = GradientBoostedTreesModel.load(jsc.sc(),
      "target/tmp/myGradientBoostingRegressionModel1");
    // $example off$

    jsc.stop();
  }
}
