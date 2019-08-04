
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class AWSservice {

    private static final String KEY_PAIR = "amen";
    private static final String JAR_BUCKET_NAME = "jarfilesstepsdist2bucket";
    private static final String OUTPUT_BUCKET_NAME = "outputdist2bucket";
    private static final String LOGS_BUCKET_NAME = "logsbucketdist2";
    public static final String N_FILE_BUCKET ="nfilebucket";

    private static final String JAR1_FILE_NAME = "WordCount1.jar";
    private static final String JAR2_FILE_NAME = "WordGroupCount2.jar";
    private static final String JAR3_FILE_NAME = "UniteWordGroupWithR3.jar";
    private static final String JAR4_FILE_NAME = "Tcalc4.jar";
    private static final String JAR5_FILE_NAME = "Ncalc5.jar";
    private static final String JAR6_FILE_NAME = "Prob6.jar";
    private static final String JAR7_FILE_NAME = "ResultForEveryId7.jar";
    private static final String JAR8_FILE_NAME = "SortWords8.jar";

    private static final String Corpus_PATH = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";
    private static final String OUTPUTFROM1_PATH = "s3n://"+OUTPUT_BUCKET_NAME+"/output1";
    private static final String OUTPUTFROM2_PATH = "s3n://"+OUTPUT_BUCKET_NAME+"/output2";
    private static final String OUTPUTFROM3_PATH = "s3n://"+OUTPUT_BUCKET_NAME+"/output3";
    private static final String OUTPUTFROM4_PATH = "s3n://"+OUTPUT_BUCKET_NAME+"/output4";
    private static final String OUTPUTFROM5_PATH = "s3n://"+OUTPUT_BUCKET_NAME+"/output5";
    private static final String OUTPUTFROM6_PATH = "s3n://"+OUTPUT_BUCKET_NAME+"/output6";
    private static final String OUTPUTFROM7_PATH = "s3n://"+OUTPUT_BUCKET_NAME+"/output7";
    private static final String FINAL_RESULT_PATH = "s3n://" + OUTPUT_BUCKET_NAME + "/final_result";

    private static AmazonS3 s3;
    private static AmazonElasticMapReduce mapReduce;


    public static void main(String[] args) {

        init();

        ///////WordCount1////////////////////////////
        HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
                .withJar("s3n://" + JAR_BUCKET_NAME + "/" + JAR1_FILE_NAME) // This should be a full map reduce application.
                .withArgs(Corpus_PATH, OUTPUTFROM1_PATH);

        StepConfig stepConfig1 = new StepConfig()
                .withName("WordCountstep1")
                .withHadoopJarStep(hadoopJarStep1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        ///////WordGroupCount2////////////////////////////
        HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
                .withJar("s3n://" + JAR_BUCKET_NAME + "/" + JAR2_FILE_NAME) // This should be a full map reduce application.
                .withArgs(Corpus_PATH, OUTPUTFROM2_PATH);

        StepConfig stepConfig2 = new StepConfig()
                .withName("WordGroupCountstep2")
                .withHadoopJarStep(hadoopJarStep2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        ///////UniteWordGroupWithR3////////////////////////////
        HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
                .withJar("s3n://" + JAR_BUCKET_NAME + "/" + JAR3_FILE_NAME) // This should be a full map reduce application.
                .withArgs(OUTPUTFROM1_PATH, OUTPUTFROM2_PATH, OUTPUTFROM3_PATH);

        StepConfig stepConfig3 = new StepConfig()
                .withName("UniteWordGroupWithRstep3")
                .withHadoopJarStep(hadoopJarStep3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        ///////////Tcalc4////////////////////////////
        HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig()
                .withJar("s3n://" + JAR_BUCKET_NAME + "/" + JAR4_FILE_NAME) // This should be a full map reduce application.
                .withArgs(OUTPUTFROM3_PATH, OUTPUTFROM4_PATH);

        StepConfig stepConfig4 = new StepConfig()
                .withName("Tcalcstep4")
                .withHadoopJarStep(hadoopJarStep4)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        ///////Ncalc5////////////////////////////
        HadoopJarStepConfig hadoopJarStep5 = new HadoopJarStepConfig()
                .withJar("s3n://" + JAR_BUCKET_NAME + "/" + JAR5_FILE_NAME) // This should be a full map reduce application.
                .withArgs(OUTPUTFROM2_PATH, OUTPUTFROM5_PATH);

        StepConfig stepConfig5 = new StepConfig()
                .withName("Ncalcstep5")
                .withHadoopJarStep(hadoopJarStep5)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        ///////Prob6////////////////////////////
        HadoopJarStepConfig hadoopJarStep6 = new HadoopJarStepConfig()
                .withJar("s3n://" + JAR_BUCKET_NAME + "/" + JAR6_FILE_NAME) // This should be a full map reduce application.
                .withArgs(OUTPUTFROM4_PATH, OUTPUTFROM5_PATH, OUTPUTFROM6_PATH);

        StepConfig stepConfig6 = new StepConfig()
                .withName("Probstep6")
                .withHadoopJarStep(hadoopJarStep6)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        ///////ResultForEveryId7////////////////////////////
        HadoopJarStepConfig hadoopJarStep7 = new HadoopJarStepConfig()
                .withJar("s3n://" + JAR_BUCKET_NAME + "/" + JAR7_FILE_NAME) // This should be a full map reduce application.
                .withArgs(OUTPUTFROM1_PATH, OUTPUTFROM6_PATH, OUTPUTFROM7_PATH);

        StepConfig stepConfig7 = new StepConfig()
                .withName("ResultForEveryId7")
                .withHadoopJarStep(hadoopJarStep7)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        ///////SortWords8////////////////////////////
        HadoopJarStepConfig hadoopJarStep8 = new HadoopJarStepConfig()
                .withJar("s3n://" + JAR_BUCKET_NAME + "/" + JAR8_FILE_NAME) // This should be a full map reduce application.
                .withArgs(OUTPUTFROM7_PATH, FINAL_RESULT_PATH);


        StepConfig stepConfig8 = new StepConfig()
                .withName("SortWords8")
                .withHadoopJarStep(hadoopJarStep8)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        ///////jobflow/////////////////////////////////
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(5)
                .withMasterInstanceType(InstanceType.M1Large.toString())
                .withSlaveInstanceType(InstanceType.M1Large.toString())
                .withHadoopVersion("2.6.2")
                .withEc2KeyName(KEY_PAIR)
                .withKeepJobFlowAliveWhenNoSteps(false) //need to be false
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("job2Test")
                .withInstances(instances)
                .withSteps(stepConfig1, stepConfig2, stepConfig3, stepConfig4, stepConfig5, stepConfig6, stepConfig7, stepConfig8)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0")
                .withVisibleToAllUsers(true)
                .withLogUri("s3n://" + LOGS_BUCKET_NAME + "/logs/");

        System.out.println("RunJobFlowRequest: " + runFlowRequest.toString());

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }

    private static void init() {
        s3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();
        mapReduce = AmazonElasticMapReduceClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();
    }
}