import java.util.List;
import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

public class Example {
    public static void main(String[] args) {
        System.out.println("Initializing a dataflow pipeline");

        // defining configuration for data flow pipeline
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

        // setting up the options for dataflow options
        options.setJobName("soa-xiong-test");
        options.setProject("york-cdf-start");
        options.setRegion("us-central1");
        options.setRunner(DataflowRunner.class);
        options.setGcpTempLocation("gs://york-project-bucket/soa-xiong/java/tmp");
        options.setStagingLocation("gs://york-project-bucket/soa-xiong/java/staging");

        // create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        // creating an array list collection
        final List<String> input = Arrays.asList("First", "second", "Third", "Fourth");

        // turns collection as an input into a PCollection and writing it out as a txt file
        pipeline.apply(Create.of(input)).apply(TextIO.write().to("gs://york-project-bucket/soa-xiong/java/results/example").withSuffix(".txt"));

        // runs pipeline
        pipeline.run().waitUntilFinish();
    }
}

// COMMANDS TO RUN JAVA CODE
/*
--clean compile code
mvn clean compile

--sets google application credentials to json key--
export GOOGLE_APPLICATION_CREDENTIALS="/Users/yorkmac048/IdeaProjects/java_dataflow/src/main/york-cdf-start-8a26c05b158d.json"

--sets environment variable for JAVA--
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-11.0.12.jdk/Contents/Home

--runs main class
mvn compile exec:java -Dexec.mainClass=Example
*/