package exercise_4;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Exercise_4 {
	
	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) throws IOException {

		String path_to_read_files = "C:\\Users\\Alex\\Desktop\\Jim Profile\\Studies\\BDMA\\Courses\\Semantic Data Management\\lab2\\SparkGraphXassignment\\src\\main\\resources\\";
		String path_to_write_files = "C:\\Users\\Alex\\Desktop\\Jim Profile\\Studies\\BDMA\\Courses\\Semantic Data Management\\lab2\\SparkGraphXassignment\\src\\main\\resources\\output\\";
		String row;
		Double DF = 0.15;
		Integer MAX_ITER = 20;

		// Open csv files
		BufferedReader wiki_vertices = new BufferedReader(new FileReader(path_to_read_files+"wiki-vertices.txt"));
		BufferedReader wiki_edges = new BufferedReader(new FileReader(path_to_read_files+"wiki-edges.txt"));

		java.util.List<Row> vertices_list = new ArrayList<Row>();
		Integer counter = 0;

		// Read wiki_vertices line by line
		while ((row = wiki_vertices.readLine()) != null) {
			String[] data = row.split("\t");
			vertices_list.add(RowFactory.create(data[0], data[1]));
		}

		JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices_list);

		StructType vertices_schema = new StructType(new StructField[]{
				new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("title", DataTypes.StringType, true, new MetadataBuilder().build())
		});

		Dataset<Row> vertices =  sqlCtx.createDataFrame(vertices_rdd, vertices_schema);

		// edges creation
		java.util.List<Row> edges_list = new ArrayList<Row>();

		// Read wiki_edges line by line
		while ((row = wiki_edges.readLine()) != null) {
			String[] data = row.split("\t");
			edges_list.add(RowFactory.create(data[0], data[1]));
		}

		// Close files
		wiki_vertices.close();
		wiki_edges.close();

		JavaRDD<Row> edges_rdd = ctx.parallelize(edges_list);

		StructType edges_schema = new StructType(new StructField[]{
				new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build())
		});

		Dataset<Row> edges = sqlCtx.createDataFrame(edges_rdd, edges_schema);

		GraphFrame gf = GraphFrame.apply(vertices,edges);

		System.out.println(gf);

		gf.edges().show();
		gf.vertices().show();

		// Calculate the Page Rank
		GraphFrame results = gf.pageRank().resetProbability(DF).maxIter(MAX_ITER).run();


		// Display resulting top 10 pageranks and final edge weights
		results.vertices().select("id", "title", "pagerank").filter("pagerank != '0.0'").orderBy(org.apache.spark.sql.functions.col("pagerank").desc()).show(10);
		
		// Display resulting pageranks and final edge weights
		//results.vertices().select("id", "title", "pagerank").filter("pagerank != '0.0'").show();


	}
	
}
