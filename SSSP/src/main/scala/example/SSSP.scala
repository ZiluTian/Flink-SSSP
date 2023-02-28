package Example

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.scala._
import org.apache.flink.graph.{Graph, Vertex, Edge}
import org.apache.flink.graph.pregel.{ComputeFunction, MessageCombiner, MessageIterator}
import org.apache.flink.graph.generator.GridGraph

import org.apache.flink.types.LongValue
import org.apache.flink.types.NullValue
import org.apache.flink.api.common.functions.MapFunction



/**
 * Implements the Single-Source Shortest Path algorithm. 
 *
 * This example shows how to:
 *
 *   - write a simple Flink program for iterative algorithms; and 
 *   - write user-defined functions.
 */
object SSSP {
  def main(args: Array[String]): Unit = {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    
    // generate a random graph, in this case, a grid, and update the vertex and edge attributes
    val graph: Graph[LongValue, Double, Double] = new GridGraph(env.getJavaEnv).addDimension(2, true).addDimension(4, true).generate()
        .mapEdges(new MapFunction[Edge[LongValue, NullValue], Double]() { def map(value: Edge[LongValue, NullValue]): Double = 1 })
        .mapVertices(new MapFunction[Vertex[LongValue, NullValue], Double]() { def map(value: Vertex[LongValue, NullValue]): Double = 1 })


    // define the maximum number of iterations
    val maxIterations = 10

    final class SSSPComputeFunction extends org.apache.flink.graph.pregel.ComputeFunction[LongValue, Double, Double, Double] {

        override def compute(vertex: Vertex[LongValue, Double], messages: MessageIterator[Double]): Unit = {

          var minDistance = if (vertex.getId.equals(0)) 0 else Double.MaxValue

          while (messages.hasNext) {
              val msg = messages.next
              if (msg < minDistance) {
                  minDistance = msg
              }
          }

          if (vertex.getValue > minDistance) {
              setNewVertexValue(minDistance)
              val it = getEdges.iterator()
              while (it.hasNext) {
                val edge = it.next
                sendMessageTo(edge.getTarget, vertex.getValue + edge.getValue)
              }
          }
      }
    }

    // message combiner
    final class SSSPCombiner extends org.apache.flink.graph.pregel.MessageCombiner[LongValue, Double] {

        override def combineMessages(messages: MessageIterator[Double]): Unit = {

            var minDistance = Double.MaxValue

            while (messages.hasNext) {
              val msg = messages.next
              if (msg < minDistance) {
                minDistance = msg
              }
            }
            sendCombinedMessage(minDistance)
        }
    }

    // // Execute the vertex-centric iteration
    val result = graph.runVertexCentricIteration[Double](new SSSPComputeFunction(), new SSSPCombiner(), maxIterations)

    // // Extract the vertices as the result
    val singleSourceShortestPaths = result.getVertices

    // // execute and print result
    singleSourceShortestPaths.print()
  }
}
