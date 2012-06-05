package motifs

import org.jgrapht._
import org.jgrapht.graph._

import scala.collection._
import scala.collection.JavaConversions.{asScalaSet, setAsJavaSet}


class ConnectedSubgraphEnumerator[V, E] {
	type Graph = DirectedGraph[V, E]
	type Subgraph = DirectedSubgraph[V, E]
	type Extensions = mutable.HashMap[E, Option[V]]

	class Seed( g: Subgraph, c: immutable.Set[E], e: Extensions ) {
		private val _graph = g
		private val _considered = c
		private val _extensions = e

		def graph: Subgraph = _graph
		def considered: immutable.Set[E] = _considered
		def extensions: Extensions = _extensions
	}

	def streamFor( graph: Graph ): Stream[Subgraph] = {
		var considered = immutable.Set[E]()

		var stream = Stream[Seed]()
		
		graph.edgeSet.foreach( e => {
			// println( graph.containsEdge(e) )
			val source = graph.getEdgeSource( e )
			val target = graph.getEdgeTarget( e )

			val subgraph = new Subgraph( graph, Set[V]( source, target ), Set( e ) )
			
			// println( subgraph.containsEdge(e)+"\n" )
			// println( "considered: "+considered.toString )
			considered = considered + e
			
			val extensions = possibleExtensions( subgraph, immutable.Set[V]( source, target ), considered )

			stream = stream :+ new Seed( subgraph, considered, extensions )
		})

		generate( stream, mutable.Queue[Seed]() )
	}

	private def generate( newSeeds: Stream[Seed], queue: mutable.Queue[Seed] ): Stream[Subgraph] = {
		if( newSeeds.isEmpty ) {
			if( queue.isEmpty ) {
				// Stream.cons( graph, Stream.empty )
				Stream.empty
			}
			else {
				val first = queue.dequeue
				val extensionSubsets = first.extensions.keys.toSet.subsets
				val _ = extensionSubsets.next // skip the empty extension

				// println( "expanding "+first.graph.getEdges.toString )

				val newStream = expansionStream( first, extensionSubsets )

				generate( newStream, queue )
			}
		}
		else {
			val seed = newSeeds.head

			if( !seed.extensions.isEmpty )
				queue.enqueue( seed )

			// println( seed.graph.toString )

			Stream.cons( seed.graph, generate( newSeeds.tail, queue ) )
		}
	}

	private def possibleExtensions( subGraph: Subgraph, newVertices: immutable.Set[V], considered: immutable.Set[E] )
			: Extensions = {

		val graph = subGraph.getBase

		val extensions = new Extensions()
		val internalExtensions = mutable.Set[EdgeNode]()

		val edgeFunction = (e: E, outEdge: Boolean) => {
			if( !considered.contains( e ) ) {
				val dest = 
					if( outEdge ) graph.getEdgeTarget( e )
					else graph.getEdgeSource( e )
			
				if( subGraph.containsVertex( dest ) ) {
					extensions.put( e, None )
				}
				else {
					extensions.put( e, Some(dest) )
				}
			}
		}

		newVertices.foreach( v => {
			graph.incomingEdgesOf( v ).foreach( e => {
				edgeFunction( e, false )
			})

			graph.outgoingEdgesOf( v ).foreach( e => {
				edgeFunction( e, true )
			})
		})

		extensions
	}

	private def expansionStream( seed: Seed, extensionSubsetsIterator: Iterator[Set[E]] ): Stream[Seed] = {
		if( !extensionSubsetsIterator.hasNext ) {
			Stream.empty
		}
		else {
			val graph = seed.graph.getBase
			val subset = extensionSubsetsIterator.next
			
			var newVertices = immutable.Set[V]()
			var edges = seed.graph.edgeSet
			var newConsidered = seed.considered ++ seed.extensions.keys

			// println( "\t\tseed: "+seed.graph.getEdges.toString )
			// println( "\t\tconsidered: "+newConsidered.toString )
			// println( "\t\tsubset: "+subset.toString+"\n" )

			subset.foreach( edge => {
				// println( "\t\t\tedge: "+edge.toString )
				
				edges = edges + edge

				val maybeNewVertex = seed.extensions( edge )

				maybeNewVertex match {
					case Some( newVertex ) => newVertices = newVertices + newVertex
					case None =>
				}					
			})

			// println( "\t\t\tnew considered: "+newConsidered.toString+"\n" )
			val newSubgraph = new Subgraph( graph, newVertices ++ seed.graph.vertexSet, edges )

			val newExtensions = possibleExtensions( newSubgraph, newVertices, newConsidered )

			Stream.cons( new Seed( newSubgraph, newConsidered, newExtensions ), expansionStream( seed, extensionSubsetsIterator ) )
		}
	}
}