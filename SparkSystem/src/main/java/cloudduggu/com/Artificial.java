package cloudduggu.com;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Spark MLlib project code.
 *
 * @author  Sarvesh Kumar (CloudDuggu.com)
 * @version 1.0
 * @since   2020-08-12
 */

public class Artificial {

    static final Logger logger = LoggerFactory.getLogger(Artificial.class);

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("CloudDugguSparkML")
//                .config("spark.master", "local")
                .getOrCreate();

//      Create an RDD objects from training dataset
        JavaRDD<TrainingDocument> trainingDocumentJavaRDD = spark.read()
                .csv(args[0])
                .javaRDD()
                .map(line -> {
                    TrainingDocument doc = new TrainingDocument();
                    doc.setType(line.getString(1));
                    doc.setText(line.getString(2));
                    return doc;
                });

//		Apply a schema to an RDD of JavaBeans to get a Dataset
        Dataset<Row> trainingRowDataset = spark.createDataFrame(trainingDocumentJavaRDD, TrainingDocument.class);

//      string column(type) into numeric column (label)
        trainingRowDataset = new StringIndexer()
                .setInputCol("type")
                .setOutputCol("label")
                .setHandleInvalid("keep")
                .fit(trainingRowDataset)
                .transform(trainingRowDataset);

//      create temp view & dataset for emotion data
        trainingRowDataset.createOrReplaceTempView("emotionData");
        Dataset<Row> emotionList = spark.sql("SELECT type, label FROM emotionData group by type, label order by label");

//      Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
        Tokenizer tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words");
        HashingTF hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol()).setOutputCol("features");
        LogisticRegression lr = new LogisticRegression().setMaxIter(10).setRegParam(0.001);
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{tokenizer, hashingTF, lr});

//      Fit the pipeline to training documents.
        PipelineModel model = pipeline.fit(trainingRowDataset);

//      start loop for fetching client data and sentiment analysis
        int count=0;
        while (true) {

            try {
                // get client request text data
                String dataURL= "http://"+args[1]+":"+args[2]+"/data";
                HttpGet dataHttpGet = new HttpGet(dataURL);
                HttpResponse dataResponse = new DefaultHttpClient().execute(dataHttpGet);
                int dataResponseCode = dataResponse.getStatusLine().getStatusCode();

                logger.info("==============================================================");
                logger.info("["+count+"] Fetch Text Response Code : "+dataResponseCode);
                logger.info("==============================================================");

//              if response is success then start analysis
                if(dataResponseCode == 200) {

                    StringBuilder requestText = new StringBuilder();
                    try(BufferedReader br = new BufferedReader(new InputStreamReader((dataResponse.getEntity().getContent())))) {
                        // Read in all of the post results into a String.
                        for(String line = br.readLine(); line != null; line = br.readLine())
                            requestText.append(line+"\n");
                    }

                    logger.info("==============================================================");
                    logger.info("["+count+"] Fetch Text : "+requestText);
                    logger.info("==============================================================");


//                  convert client text data into object list
                    AtomicInteger lineCount=new AtomicInteger(0);
                    List<TestDocument> data = Arrays.asList(requestText.toString().split("\n")).stream()
                        .map(line -> new TestDocument(lineCount.incrementAndGet(), line))
                        .collect(Collectors.toList());

//                  make dataset for client text data object
                    Dataset<Row> testDataset = spark.createDataFrame(data, TestDocument.class);

//                    testDataset.show(false);

//                  Make predictions on client text.
                    Dataset<Row> predictions = model.transform(testDataset);

//                  create temp view for emotionList & predictions
                    emotionList.createOrReplaceTempView("emotionTable");
                    predictions.createOrReplaceTempView("resultTable");

//                  get selected result into dataset
                    Dataset<Row> sqlDF = spark.sql("SELECT id, text, type, probability, prediction FROM emotionTable, resultTable where label=prediction");

                    logger.info("1-FinalResult====================================================================================");
                    sqlDF.show();
                    logger.info("2-FinalResult====================================================================================");


//                  create map object form result dataset
                    Map<Integer, String> resultMap = new HashMap();
                    sqlDF.collectAsList().stream()
                            .forEach(line -> resultMap.put(Integer.parseInt(line.get(0).toString()), line.get(1)+"\t"+line.get(2)+"\t"+line.get(3)+"\t"+line.get(4)+"\n"));

//                  create ordered string result form map object
                    StringBuilder resultString = new StringBuilder();
                    resultMap.entrySet().stream()
                            .sorted(Map.Entry.comparingByKey())
                            .forEachOrdered(line -> resultString.append(line.getValue()));


                    logger.info("==============================================================");
                    logger.info("["+count+"] ResultString: "+resultString);
                    logger.info("==============================================================");


//                  Upload Result Output
                    String uploadURL= "http://"+args[1]+":"+args[2]+"/upload?type=result&text=" + URLEncoder.encode( resultString.toString(), "UTF-8" );

                    HttpPost resultHttpPost = new HttpPost(uploadURL);
                    HttpResponse resultResponse = new DefaultHttpClient().execute(resultHttpPost);
                    int resultResponseCode = resultResponse.getStatusLine().getStatusCode();

                    logger.info("==============================================================");
                    logger.info("["+count+"] Upload Success... : "+resultResponseCode);
                    logger.info("==============================================================");

                }

//              for next process wait 5 second
                Thread.sleep(5 * 1000);
            } catch (Exception ex) {
                ex.printStackTrace();
            }

//          count loop value for avoiding infinite process
            count++;
            if(count >=100)
                break;

        }

        spark.stop();
    }
}
