package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import exercise_2.Exercise_2;
import javafx.util.Pair;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Exercise_3 {

    private static class VProg extends AbstractFunction3<Long,Pair<Integer,List<Long>>,Pair<Integer,List<Long>>,Pair<Integer,List<Long>>> implements Serializable {
        @Override
        public Pair<Integer,List<Long>> apply(Long vertexID, Pair<Integer,List<Long>> vertexValue, Pair<Integer,List<Long>> message) {
            if (message.getKey() == Integer.MAX_VALUE) {             // superstep 0
                return new Pair<Integer,List<Long>>(vertexValue.getKey(),vertexValue.getValue());
            } else {   			// superstep > 0
                if(vertexValue.getKey() < message.getKey())
                    return new Pair<Integer,List<Long>>(vertexValue.getKey(),vertexValue.getValue());
                else
                    return new Pair<Integer,List<Long>>(message.getKey(),message.getValue());
            }

        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Pair<Integer,List<Long>>,Integer>, Iterator<Tuple2<Object,Pair<Integer,List<Long>>>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Pair<Integer,List<Long>>>> apply(EdgeTriplet<Pair<Integer,List<Long>>, Integer> triplet) {

            Tuple2<Object,Pair<Integer,List<Long>>> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object,Pair<Integer,List<Long>>> dstVertex = triplet.toTuple()._2();
            Integer edge_weight = triplet.toTuple()._3();
            Integer mysum = sourceVertex._2.getKey() + edge_weight;

            Integer edge = triplet.toTuple()._3();

			// the cost to get to the source vertex + the edge weight to go to the
			// neighbor vertex is greater or equal to the cost, already calculated
			// to go the neighbor vertex
            if (mysum >= dstVertex._2.getKey() || sourceVertex._2.getKey() == Integer.MAX_VALUE) {
                // do nothing
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Pair<Integer,List<Long>>>>().iterator()).asScala();
            } else {
                // propagate source vertex value
                List<Long> new_list = sourceVertex._2.getValue();
                new_list.add((Long) dstVertex._1);

                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,Pair<Integer,List<Long>>>(triplet.dstId(),new Pair<Integer,List<Long>>(mysum,new_list))).iterator()).asScala();
            }
        }
    }

    private static class merge extends AbstractFunction2<Integer,Integer,Integer> implements Serializable {
        @Override
        public Integer apply(Integer o, Integer o2) {
            return Math.max(o,o2);
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {

        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();

        Pair<Integer,List<Long>> l = new Pair<Integer,List<Long>>(0,Lists.newArrayList() );
        
        // in each node we store an id (Object), the cost (Integer) and the path (List<Long>) to get to this node
        List<Tuple2<Object,Pair<Integer,List<Long>>>> vertices = Lists.newArrayList(
                new Tuple2<Object,Pair<Integer,List<Long>>>(1l,new Pair<Integer,List<Long>>(0,Lists.newArrayList(1l))),
                new Tuple2<Object,Pair<Integer,List<Long>>>(2l,new Pair<Integer,List<Long>>(Integer.MAX_VALUE,Lists.newArrayList())),
                new Tuple2<Object,Pair<Integer,List<Long>>>(3l,new Pair<Integer,List<Long>>(Integer.MAX_VALUE,Lists.newArrayList())),
                new Tuple2<Object,Pair<Integer,List<Long>>>(4l,new Pair<Integer,List<Long>>(Integer.MAX_VALUE,Lists.newArrayList())),
                new Tuple2<Object,Pair<Integer,List<Long>>>(5l,new Pair<Integer,List<Long>>(Integer.MAX_VALUE,Lists.newArrayList())),
                new Tuple2<Object,Pair<Integer,List<Long>>>(6l,new Pair<Integer,List<Long>>(Integer.MAX_VALUE,Lists.newArrayList()))
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l,2l, 4), // A --> B (4)
                new Edge<Integer>(1l,3l, 2), // A --> C (2)
                new Edge<Integer>(2l,3l, 5), // B --> C (5)
                new Edge<Integer>(2l,4l, 10), // B --> D (10)
                new Edge<Integer>(3l,5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );

        JavaRDD<Tuple2<Object,Pair<Integer,List<Long>>>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);
        Pair<Integer,List<Long>> x = new Pair<Integer,List<Long>>(1,Lists.newArrayList());

        Graph<Pair<Integer,List<Long>>,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),x, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Object.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Object.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));


        ops.pregel(new Pair<Integer,List<Long>>(Integer.MAX_VALUE,Lists.newArrayList()),
                Integer.MAX_VALUE,
                EdgeDirection.Out(),
                new Exercise_3.VProg(),
                new Exercise_3.sendMsg(),
                new Exercise_3.merge(),
                ClassTag$.MODULE$.apply(Object.class))
                .vertices()
                .toJavaRDD()
                .foreach(v -> {
                    Tuple2<Object,Pair<Integer,List<Long>>> vertex = (Tuple2<Object,Pair<Integer,List<Long>>>)v;
                    System.out.print("Minimum cost to get from "+labels.get(1l)+" to "+labels.get(vertex._1)+" is [ ");

                    for (Long temp : vertex._2.getValue())  //transform the vertex id to label e.g [1,2] -> [A,B]
                        System.out.print(labels.get(temp)+" ");
                    System.out.println("] with cost "+vertex._2.getKey());
                });
    }
	
}
