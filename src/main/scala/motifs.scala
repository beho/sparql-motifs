package motifs

// import java.awt.Dimension;
import java.io._
import java.lang.ProcessBuilder
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.{Date, Scanner}

import scala.collection._
// import scala.collection.mutable.Queue
// import scala.collection.immutable.Set
import scala.collection.JavaConversions.{asScalaSet, setAsJavaSet}

import scala.annotation.tailrec

import com.hp.hpl._
import com.hp.hpl.jena._
import com.hp.hpl.jena.graph._
import com.hp.hpl.jena.query._
import com.hp.hpl.jena.sparql.algebra._
import com.hp.hpl.jena.sparql.core.Var
import com.hp.hpl.jena.sparql.expr._
import com.hp.hpl.jena.tdb._

// import com.clarkparsia.stardog.StardogDBMS
// import com.clarkparsia.stardog.jena._

import motifs.fix.NodecSSEFixed

import org._
import org.jgrapht._
import org.jgrapht.graph._

// jung visualisation
// import edu.uci.ics.screencap.PNGDump

object NodecHolder {
	val nodecSSE = new NodecSSEFixed

	// def nodecSSE = nodec
}


class EdgeNode( var p: jena.graph.Node, var s: jena.graph.Node, var o: jena.graph.Node ) extends java.io.Serializable {

	override def toString: String = {
		p.toString
	}

	// override def equals( o: Any ): Boolean = {
	// 	return this.hashCode == o.hashCode
	// }

	// // do not take into account anything but p
	// override def hashCode: Int = {
	// 	p.hashCode
	// }

	// serialization code

	private def writeObject( out: ObjectOutputStream ) = {
		val nodecSSE = NodecHolder.nodecSSE

		val sBuffer = ByteBuffer.allocate( nodecSSE.maxSize( s ) )
		nodecSSE.encode( s, sBuffer, null )

		val pBuffer = ByteBuffer.allocate( nodecSSE.maxSize( p ) )
		nodecSSE.encode( p, pBuffer, null )

		val oBuffer = ByteBuffer.allocate( nodecSSE.maxSize( o ) )
		nodecSSE.encode( o, oBuffer, null )

		out.writeObject( sBuffer.array )
		out.writeObject( pBuffer.array )
		out.writeObject( oBuffer.array )

		// println( "writing: "+s.toString+" "+p.toString+" "+o.toString )
	}

	private def readObject( in: ObjectInputStream ) = {
		val nodecSSE = NodecHolder.nodecSSE

		s = nodecSSE.decode( ByteBuffer.wrap( in.readObject.asInstanceOf[Array[Byte]] ), null )
		p = nodecSSE.decode( ByteBuffer.wrap( in.readObject.asInstanceOf[Array[Byte]] ), null )
		o = nodecSSE.decode( ByteBuffer.wrap( in.readObject.asInstanceOf[Array[Byte]] ), null )

		// println( "reading: "+s.toString+" "+p.toString+" "+o.toString)
	}
}

class Motif extends DirectedPseudograph[jena.graph.Node, EdgeNode]( classOf[EdgeNode] ) {
	def containsPredicateVar: Boolean = {
		this.edgeSet.foreach( e => {
			if( e.p.isVariable ) {
				return true
			}
		})

		return false
	}

	override def equals( o: Any ): Boolean = {
		if( o.isInstanceOf[Motif] ) {
			// println("x")
			val m = o.asInstanceOf[Motif]
			return( m.edgeSet == this.edgeSet && m.vertexSet == this.vertexSet )
		}

		return false
	}

	// override def hashCode: Int {
		
	// }
}

// object GraphVisualiser {
// 	def visualise[V, E]( graph: DirectedSparseMultigraph[V, E], filename: String ) = {
// 		val width = 1200
// 		val height = 750

// 		val layout = new ISOMLayout[V, E]( graph )
// 		layout.setSize( new Dimension( width, height ) )
 
// 		val vv = new BasicVisualizationServer[V, E]( layout )
// 		vv.getRenderContext().setVertexLabelTransformer( new ToStringLabeller() )
// 		vv.getRenderContext().setEdgeLabelTransformer( new ToStringLabeller() )
// 		vv.setSize( width, height )
 
// 		val dumper = new PNGDump()
// 		try {
// 			dumper.dumpComponent( new File( filename ), vv )
// 		} 
// 		catch {
// 			case e => { e.printStackTrace() }
// 		}
// 	}
// }


class GraphBuilder extends OpVisitorBase {
	// var patterns = immutable.Set[jena.graph.Triple]()
	var patternsContainPredicateVar = false
	var predicateVars = Set[Var]()

	val termToBNode = new mutable.HashMap[jena.graph.Node, jena.graph.Node]

	var graph = new DirectedPseudograph[jena.graph.Node, EdgeNode]( classOf[EdgeNode] )

	override def visit( opBGP: op.OpBGP ) {
		val newPatterns = scala.collection.JavaConversions.asScalaBuffer( opBGP.getPattern.getList )
		// patterns = patterns ++ newPatterns

		newPatterns.foreach( pattern => {
			val s = pattern.getSubject
			val p = pattern.getPredicate
			val o = pattern.getObject

			if( !termToBNode.contains( s ) ) {
				termToBNode.put( s, jena.graph.Node.createAnon )
			}
			
			if( !termToBNode.contains( o ) ){
				termToBNode.put( o, jena.graph.Node.createAnon )
			}

			val source = termToBNode( s )
			val target = termToBNode( o )

			graph.addVertex( source )
			graph.addVertex( target )
			graph.addEdge( source, target, new EdgeNode( p, source, target ) )

			val predicate = pattern.getPredicate
			if( predicate.isVariable ) {
				patternsContainPredicateVar = true
				predicateVars = predicateVars + Var.alloc( predicate )
			}
		})
	}
}


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

object QueryHelper {
	val motifVarName = "motif"
	val countVarName = "count"

	def queryWithCountFor( subgraph: DirectedGraph[jena.graph.Node, EdgeNode] ): jena.query.Query = {
		val query = queryFor( subgraph )

		val countVar = Var.alloc( countVarName )
		val countProperty = jena.graph.Node.createURI( "http://fit.vutbr.cz/query-analysis#timesEncountered" )

		query.addResultVar( countVar )

		val topGroup = query.getQueryPattern
		val countBGP = new jena.sparql.syntax.ElementTriplesBlock

		countBGP.addTriple( new Triple( Var.alloc( motifVarName ), countProperty, countVar ) )

		// add ?g <count> ?c and result var ?c

		return query
	}

	def queryFor( subgraph: DirectedGraph[jena.graph.Node, EdgeNode] ): jena.query.Query = {
		val sVar = Var.alloc( "s" )
		val pVar = Var.alloc( "p" )
		val oVar = Var.alloc( "o" )
		val countProperty = jena.graph.Node.createURI( "http://fit.vutbr.cz/query-analysis#timesEncountered" )

		val topGroup = new jena.sparql.syntax.ElementGroup
		val namedGraphGroup = new jena.sparql.syntax.ElementGroup
		
		val requiredBGP = new jena.sparql.syntax.ElementTriplesBlock
		// val requiredBGP = new jena.sparql.syntax.ElementPathBlock
		
		val iffFilters = mutable.Set[jena.sparql.syntax.ElementFilter]()

		val termToVar = new mutable.HashMap[jena.graph.Node, Var]
		val pairsByPredicate = new mutable.HashMap[jena.graph.Node, mutable.Set[(Var, Var)]]

		var counter = 1
		subgraph.edgeSet.foreach( e => {
			if( !termToVar.contains( e.s ) ) {
				val v = Var.alloc( "v"+counter.toString )
				termToVar.put( e.s, v )
				counter = counter + 1
			}

			if( !termToVar.contains( e.o ) ){
				val v = Var.alloc( "v"+counter.toString )
				termToVar.put( e.o, v )
				counter = counter + 1
			}

			val s = termToVar( e.s )
			val o = termToVar( e.o )

			if( !pairsByPredicate.contains( e.p ) ) {
				pairsByPredicate.put( e.p, mutable.Set[(Var, Var)]() )
			}
			pairsByPredicate( e.p ).add( (s, o) )

			requiredBGP.addTriple( new Triple( s, e.p, o ) )

			val filter = new jena.sparql.syntax.ElementFilter( 
				new E_LogicalOr( 
					new E_NotEquals( new ExprVar( sVar ), new ExprVar( s ) ), 
					new E_LogicalOr( 
						new E_NotEquals( new ExprVar( pVar ), new nodevalue.NodeValueNode( e.p ) ), 
						new E_NotEquals( new ExprVar( oVar ), new ExprVar( o ) ) ) ) )

			iffFilters.add( filter )
		})

		val varPairsToCompare = mutable.Set[(Var, Var)]()

		pairsByPredicate.foreach( predicatePairs => {
			val pairs = predicatePairs._2
			for( p1 <- pairs; p2 <- pairs ) {
				if( p1._1 != p2._1 ) {
					val toCompare = (p1._1, p2._1)
					if( !varPairsToCompare.contains( toCompare ) && !varPairsToCompare.contains( (p2._1, p1._1) ) )
						varPairsToCompare.add( toCompare )
				}
				if( p1._2 != p2._2 ) {
					val toCompare = (p1._2, p2._2)
					if( !varPairsToCompare.contains( toCompare ) && !varPairsToCompare.contains( (p2._2, p1._2) ) )
						varPairsToCompare.add( toCompare )
				}
			}
		})

		val notExistsGroup = new jena.sparql.syntax.ElementGroup

		val notExistsBGP = new jena.sparql.syntax.ElementTriplesBlock
		// val notExistsBGP = new jena.sparql.syntax.ElementPathBlock
		
		notExistsBGP.addTriple( new Triple( sVar, pVar, oVar ) )

		notExistsGroup.addElement( notExistsBGP )

		iffFilters.foreach( f => { 
			notExistsGroup.addElementFilter( f ) 
		})

		val notExists = new jena.sparql.expr.E_NotExists( notExistsGroup )

		namedGraphGroup.addElement( requiredBGP )
		namedGraphGroup.addElementFilter( new jena.sparql.syntax.ElementFilter( notExists ) )

		varPairsToCompare.foreach( p => {
			namedGraphGroup.addElementFilter( new jena.sparql.syntax.ElementFilter( 
							new E_NotEquals( 
								new ExprVar( p._1 ),
								new ExprVar( p._2 ) ) ) )
		})

		val namedGraph = new jena.sparql.syntax.ElementNamedGraph( Var.alloc( motifVarName ), namedGraphGroup )

		topGroup.addElement( namedGraph )

		// counting
		val countBGP = new jena.sparql.syntax.ElementTriplesBlock
		countBGP.addTriple( new Triple( Var.alloc( motifVarName ), countProperty, Var.alloc( countVarName ) ) )
		topGroup.addElement( countBGP )

		val query = jena.query.QueryFactory.create

		query.setQuerySelectType
		query.setQueryPattern( topGroup )
		query.addResultVar( Var.alloc( motifVarName ) )

		//counting
		query.addResultVar( Var.alloc( countVarName ) )

		return query
	}

	def subgraphToRDFGraph( subgraph: DirectedGraph[jena.graph.Node, EdgeNode] ): jena.graph.Graph = {
		val graph = jena.graph.Factory.createDefaultGraph

		subgraph.edgeSet.foreach( e => {
			graph.add( new Triple( e.s, e.p, e.o ) )
		})

		return graph
	}

	def createUnsignedIntLiteral( value: Int ): Node = {
		return Node.createLiteral( value.toString, null, jena.datatypes.xsd.XSDDatatype.XSDunsignedInt )
	}
}

trait MotifHandler[N, E] {
	def handle( subgraph: DirectedGraph[N, E], queryURI: String )
	def close: Unit
}

trait MotifCounter[N, E] extends MotifHandler[N, E] {
	protected var handled = 0
	protected var unique = 0

	def motifsHandled: Int = handled
	def uniqueMotifsEncountered: Int = unique
}

class TDBMotifCounter( filename: String ) extends MotifCounter[jena.graph.Node, EdgeNode] {
	// type Subgraph = AbstractGraph[jena.graph.Node, EdgeNode]

	// filename without (last) extension
	val dir = "./tdb/"+filename.split('.').head	

	val countProperty = Node.createURI( "http://fit.vutbr.cz/query-analysis#timesEncountered" )
	val containedInProperty = Node.createURI( "http://fit.vutbr.cz/query-analysis#containedIn" )

	val motifVarName = "motif"
	val countVarName = "count"
	val motifURITemplate = "http://fit.vutbr.cz/query-analysis#motif-"

	// var uniqueMotifsCount = 0
	// var totalMotifsCount = 0

	var datasetGraph = TDBFactory.createDatasetGraph( dir )
	var graphStore = update.GraphStoreFactory.create( datasetGraph )

	println( "optimizer strategy: "+datasetGraph.getTransform.toString )
	

	def handle( subgraph: DirectedGraph[jena.graph.Node, EdgeNode], queryURI: String ) = {
		// println( "s: "+subgraph.toString )

		TDB.sync( graphStore )
		// println( "syncing tdb" )

		// println( graphStore.toString )

		// val queryURI = Node.createURI( queryURI )

		val query = QueryHelper.queryFor( subgraph )
		// println( query.toString )
		
		handled += 1
		// println( "q: "+query.toString )
		
		val plan = QueryExecutionFactory.createPlan( query, graphStore )
		val result = plan.iterator

		// there is a solution - motif was encountered before
		// println( "new motif: " + (!result.hasNext).toString)
		if( result.hasNext ) {
			val binding = result.next
			val motifURI = binding.get( Var.alloc( motifVarName ) )

			result.cancel

			// println( "existing: "+motifURI.toString )

			// val countLiteral = graphStore.getDefaultGraph.find( motif, countProperty, null ).next.getMatchObject
			val countLiteral = binding.get( Var.alloc( countVarName ) )
			
			graphStore.getDefaultGraph.delete( new Triple( motifURI, countProperty, countLiteral ) )

			val count = countLiteral.getLiteral.getValue.asInstanceOf[Int]
			val newCount = QueryHelper.createUnsignedIntLiteral( count + 1 )

			graphStore.getDefaultGraph.add( new Triple( motifURI, countProperty, newCount ) )

			graphStore.getDefaultGraph.add( new Triple( motifURI, containedInProperty,  Node.createURI( queryURI ) ) )
		}
		else {
			result.close

			unique += 1
			
			val graph = QueryHelper.subgraphToRDFGraph( subgraph )
			val motifURI = Node.createURI( motifURITemplate + unique.toString )

			// println( "r: "+graph.toString )
			// println( "inserting motif "+motifURI.toString+":\n\t"+graph.toString+"\n" )

			// insert motif as a named graph
			graphStore.addGraph( motifURI, graph )

			// (motif timesEncountered count) into default model
			// val motif = 
			graphStore.getDefaultGraph.add( new Triple( motifURI, countProperty, QueryHelper.createUnsignedIntLiteral( 1 ) ) )

			graphStore.getDefaultGraph.add( new Triple( motifURI, containedInProperty,  Node.createURI( queryURI ) ) )
		}

		if( handled == 1000 || handled == 3000 || handled == 5000 || handled % 10000 == 0 ) {
			reopenWithGeneratedStats
		}
	}

	def reopenWithGeneratedStats = {
		println( "\ngenerating stats\n" )

		close

		val pb = new ProcessBuilder("bash", "generate-stats.sh", dir)
		val p = pb.start
		p.waitFor

		open
	}

	def open = {
		datasetGraph = TDBFactory.createDatasetGraph( dir )
		graphStore = update.GraphStoreFactory.create( datasetGraph )

		println( "optimizer strategy: "+datasetGraph.getTransform.toString )
	}

	def close = {
		// println( "syncing tdb & closing" )
		TDB.sync( graphStore )
		java.lang.Thread.sleep( 1000 )
		graphStore.close 
	}
}

// class StardogMotifsCounter( filename: String ) {
// 	StardogDBMS.startEmbeddedServer
// 	val dbms = StardogDBMS.login( "admin", "admin".toCharArray() )

// 	// val connection = dbms.createMemory( filename ).connect
// 	val connection = dbms.createMemory( filename ).connect
// 	dbms.logout

// 	// val connection = ConnectionConfiguration
// 	// 		.to( filename )
// 	// 		.credentials( "admin", "admin" )
// 	// 		.connect	

// 	// val dir = "./tdb/"+filename.split('.').head

// 	val dataset = SDJenaFactory.createDataset( connection )
// 	val graphStore = update.GraphStoreFactory.create( dataset )

// 	val countProperty = Node.createURI( "http://fit.vutbr.cz/query-analysis#timesEncountered" )
// 	val containedInProperty = Node.createURI( "http://fit.vutbr.cz/query-analysis#containedIn" )

// 	val motifVarName = "motif"
// 	val countVarName = "count"
// 	val motifURITemplate = "http://fit.vutbr.cz/query-analysis#motif-"

// 	var uniqueMotifsCount = 0
// 	var totalMotifsCount = 0

// 	def countIn( subgraph: DirectedGraph[jena.graph.Node, EdgeNode], queryURIString: String ) = {
// 		// println( "s: "+subgraph.toString )

// 		// TDB.sync( graphStore )
// 		// println( "syncing tdb" )

// 		// println( graphStore.toString )

// 		val queryURI = Node.createURI( queryURIString )

// 		val query = QueryHelper.queryFor( subgraph )
// 		println( query.toString )
		
// 		totalMotifsCount += 1
// 		// println( "q: "+query.toString )
		
// 		val plan = QueryExecutionFactory.createPlan( query, graphStore )
// 		val result = plan.iterator

// 		// there is a solution - motif was encountered before
// 		// println( "new motif: " + (!result.hasNext).toString)
// 		if( result.hasNext ) {
// 			val binding = result.next
// 			val motifURI = binding.get( Var.alloc( motifVarName ) )

// 			result.cancel

// 			// println( "existing: "+motifURI.toString )

// 			// val countLiteral = graphStore.getDefaultGraph.find( motif, countProperty, null ).next.getMatchObject
// 			val countLiteral = binding.get( Var.alloc( countVarName ) )
			
// 			graphStore.getDefaultGraph.delete( new Triple( motifURI, countProperty, countLiteral ) )

// 			val count = countLiteral.getLiteral.getValue.asInstanceOf[Int]
// 			val newCount = QueryHelper.createUnsignedIntLiteral( count + 1 )

// 			graphStore.getDefaultGraph.add( new Triple( motifURI, countProperty, newCount ) )

// 			graphStore.getDefaultGraph.add( new Triple( motifURI, containedInProperty, queryURI ) )
// 		}
// 		else {
// 			result.close

// 			uniqueMotifsCount = uniqueMotifsCount + 1
			
// 			val graph = QueryHelper.subgraphToRDFGraph( subgraph )
// 			val motifURI = Node.createURI( motifURITemplate + uniqueMotifsCount.toString )

// 			// println( "r: "+graph.toString )
// 			// println( "inserting motif "+motifURI.toString+":\n\t"+graph.toString+"\n" )

// 			// insert motif as a named graph
// 			graphStore.addGraph( motifURI, graph )

// 			// (motif timesEncountered count) into default model
// 			// val motif = 
// 			graphStore.getDefaultGraph.add( new Triple( motifURI, countProperty, QueryHelper.createUnsignedIntLiteral( 1 ) ) )

// 			graphStore.getDefaultGraph.add( new Triple( motifURI, containedInProperty, queryURI ) )
// 		}
// 	}

// 	def close = {
// 		// println( "syncing tdb & closing" )
// 		// TDB.sync( graphStore )
// 		graphStore.close 
// 	}
// }

class PredicateVarSubstitutor( endpointURL: String ) {
	def substitutionsFor( query: Query, predicateVars: Set[Var] )
			: List[sparql.engine.binding.Binding] = {
		
		val substitutionsQuery = QueryFactory.create
		substitutionsQuery.setQueryPattern( query.getQueryPattern )
		substitutionsQuery.setQuerySelectType
		substitutionsQuery.setDistinct( true )
		substitutionsQuery.addProjectVars( predicateVars )

		// println( substitutionsQuery.toString )

		val qe = QueryExecutionFactory.sparqlService( endpointURL, substitutionsQuery )
		val resultSet = qe.execSelect

		var bindings = List[sparql.engine.binding.Binding]()

		while( resultSet.hasNext ) {
			bindings = resultSet.nextBinding :: bindings
		}
		
		println( "\n[debug] substitutions count: "+bindings.size )
		return bindings
	}
}

object PredicateVarSubstitutor {
	private def createGraphWithSubstitutions( graph: DirectedGraph[jena.graph.Node, EdgeNode], substitution: Map[Var, jena.graph.Node] )
			: Motif = {

		val newGraph = new Motif()

		graph.edgeSet.foreach( e => {
			if( e.p.isVariable ) {
				val property = substitution( Var.alloc( e.p ) )

				if( property == null ) {
					return null
				} else {
					newGraph.addVertex( e.s )
					newGraph.addVertex( e.o )
					newGraph.addEdge( e.s, e.o, new EdgeNode( property, e.s, e.o ) )					
				}
			}
			else {
				newGraph.addVertex( e.s )
				newGraph.addVertex( e.o )
				newGraph.addEdge( e.s, e.o, e )					
			}
		})

		return newGraph
	}

	def uniqueSubstitutionsForGraph( graph: DirectedGraph[jena.graph.Node, EdgeNode], predicateVarsInGraph: Set[Var], substitutions: List[sparql.engine.binding.Binding] )
				: Set[Map[Var, jena.graph.Node]] = {

		val set = mutable.Set[Map[Var, jena.graph.Node]]()

		substitutions.foreach( s => {
			val map = mutable.Map[Var, jena.graph.Node]()

			predicateVarsInGraph.foreach( v => {
				map.put( v, s.get( v ) )
			})

			set.add( map )
		})

		return set
	}
	
	// TODO rewrite using Stream ?	
	def substitute( graph: DirectedGraph[jena.graph.Node, EdgeNode], predicateVarsInGraph: Set[Var], substitutions: List[sparql.engine.binding.Binding] ): 
			Set[Motif] = {
		
		var graphs = Set[Motif]()

		val uniqueSubs = uniqueSubstitutionsForGraph( graph, predicateVarsInGraph, substitutions )
		// println( uniqueSubs.size )

		uniqueSubs.foreach( s => {
			val graphWithSubstitutions = createGraphWithSubstitutions( graph, s )
			if( graphWithSubstitutions != null ) {
				graphs = graphs + graphWithSubstitutions
			}
		})

		return graphs
	}
}


abstract class QueryGraph

case class CompleteGraph( graph: DirectedPseudograph[jena.graph.Node, EdgeNode], uri: String ) extends QueryGraph
case class SubstitutionsNeedingGraph( graph: DirectedPseudograph[jena.graph.Node, EdgeNode], uri: String, query: Query, predicateVars: Set[Var] ) extends QueryGraph

case class InvalidGraph() extends QueryGraph



class QueryReader( filename: String, predefinedPrefixes: String = "", skipPredicateVarQueries: Boolean = false ) {
	val scanner = new Scanner( new FileInputStream( filename ), "UTF-8" )
	val errors = new PrintWriter( filename+".errors" )
	val predicateVarQueries = new PrintWriter( filename+".predicate-vars")

	val queryURITemplate = "http://fit.vutbr.cz/query-analysis/query"

	var queryCount = 0
	var substitutionNeedingQueryCount = 0
	// var totalSubGraphCount = 0
	// var theoreticTotalSubGraphCount = 0

	@tailrec final def nextQueryGraph: Option[QueryGraph] = {
		val queryGraph = readAndBuildQueryGraph

		queryGraph match {
			case Some( InvalidGraph() ) => return nextQueryGraph
			case None => return None
			case graph => {
				return graph
			}
		}
	}

	def readAndBuildQueryGraph: Option[QueryGraph] = {
		// val startTime = System.currentTimeMillis

		if( scanner.hasNext ) {
			val line = scanner.nextLine

			try {	
				return Some( buildQueryGraph( line ) )
			}
			catch {
				case e: Exception => {
					val msg = e.toString.split('\n')(0)

					msg match {
						case motifs.Prefixes.UnresolvedPrefix( prefix ) => {
							try {
								return Some( buildQueryGraph( line, predefinedPrefixes ) )
							}
							catch {
								case e1 => {
									val error = "\n"+e1.toString.split('\n')(0)+"\n"+line+"\n"
									errors.println( error )
									println( error )
									return Some( InvalidGraph() )
								}
							}
						}

						case _ => {
							val error = "\n"+msg+"\n"+line+"\n"
							errors.println( error )
							println( error )
							return Some( InvalidGraph() )
						}
					}
				} 
			}
		}
		else {
			return None
		}
	}

	private def buildQueryGraph( line: String, prefixes: String = "" ): QueryGraph = {
		val builder = new GraphBuilder
				
		val query = QueryFactory.create( prefixes + line )
		val op = Algebra.compile( query )
		OpWalker.walk( op, builder )

		queryCount += 1
		val queryURI = queryURITemplate+"/"+filename+"#"+queryCount.toString

		if( builder.patternsContainPredicateVar ) {
			substitutionNeedingQueryCount += 1

			if( skipPredicateVarQueries ) {
				print( "s" )
				predicateVarQueries.println( line )
				return InvalidGraph()
			}
			else{
				return SubstitutionsNeedingGraph( builder.graph, queryURI, query, builder.predicateVars )
			}
		}
		else {
			return CompleteGraph( builder.graph, queryURI )
		}
	}

	def close = {
		scanner.close
		errors.close
	}
}


class MotifEnumerator( val filename: String, val dataset: String, val substitutionsEndpointURL: String, val skipPredicateVarQueries: Boolean = false, val handler: MotifHandler[jena.graph.Node, EdgeNode] ) {
	val reader = new QueryReader( filename, motifs.Prefixes.forDataset( dataset), skipPredicateVarQueries )
	val subgraphEnumerator = new ConnectedSubgraphEnumerator[jena.graph.Node, motifs.EdgeNode]()
	val substitutor = new PredicateVarSubstitutor( substitutionsEndpointURL )
	// val counter = new MotifsCounter( filename )

	val dateFormat = new SimpleDateFormat( "HH:mm:ss.S" )
	val startTime = System.currentTimeMillis

	val waitMs = 100
	var lastSubstitutionsTime = System.currentTimeMillis - waitMs

	val failedSubstitutions = new PrintWriter( filename+".subs" )

	@tailrec final def run: Unit = {
		reader.nextQueryGraph match {
			case None => {
				// val diffTime = (System.currentTimeMillis - startTime) / 1000.0
				// val queriesPerSecond = reader.queryCount / diffTime
				// val motifsPerSecond = handler.motifsHandled / diffTime

				// val line = "\n\n"+dateFormat.format( new Date )+" DONE\n"+reader.queryCount+" queries read\t\t"+reader.substitutionNeedingQueryCount+" with predicate vars\n"+handler.motifsHandled.toString+" motifs\t\t"+handler.uniqueMotifsEncountered+" unique motifs)\n"+queriesPerSecond.toString+" query/s\t\t"+motifsPerSecond.toString+" motif/s"

				// println( "\n\nDONE: "+reader.queryCount+" queries. "+counter.totalMotifsCount+" motifs" )

				// println( line )
				
				reader.close
				handler.close
				failedSubstitutions.close
			}
			case Some( CompleteGraph( graph, uri ) ) => {
				findMotifs( subgraphEnumerator.streamFor( graph ), uri )

				run
			}
			case Some( SubstitutionsNeedingGraph( graph, uri, query, predicateVars ) ) => {
				if( System.currentTimeMillis - lastSubstitutionsTime < waitMs ) {
					Thread.sleep( waitMs )
				}

				try {
					val substitutions = substitutor.substitutionsFor( query, predicateVars )
					// println(substitutions.toString)
					
					findMotifsWithSubstitutions( subgraphEnumerator.streamFor( graph ), uri, substitutions )
				}
				catch {
					case e: Exception => {
						println( "\nFAILED TO OBTAIN SUBSTITUTITIONS" )
						
						println( e.getMessage+"\n" )
						println( query.toString )

						// failedSubstitutions.println( e.getMessage+"\n" )
						failedSubstitutions.println( query.toString )
						
						// scala.sys.exit(1)
					}
				}

				run
			}
		}
		
	}

	@tailrec private def findMotifs( stream: Stream[DirectedGraph[jena.graph.Node, EdgeNode]], queryURI: String ): Unit = {
		if( stream.isEmpty ) {
			return
		}

		countIn( stream.head, queryURI )
		findMotifs( stream.tail, queryURI )
	}

	implicit def wrapDirectedGraph( graph: DirectedGraph[jena.graph.Node, motifs.EdgeNode] ) = new WrappedDirectedGraph( graph )

	@tailrec private def findMotifsWithSubstitutions( stream: Stream[DirectedGraph[jena.graph.Node, EdgeNode]], queryURI: String, substitutions: List[sparql.engine.binding.Binding] ): Unit = {
		if( stream.isEmpty ) {
			return
		}

		val motif = stream.head
		val predicateVars = motif.predicateVars

		if( !predicateVars.isEmpty ) {
			val withSubstitutions = PredicateVarSubstitutor.substitute( motif, predicateVars, substitutions )

			withSubstitutions.foreach( s => {
				countIn( s, queryURI )
			})
		}
		else {
			countIn( motif, queryURI )
		}

		findMotifsWithSubstitutions( stream.tail, queryURI, substitutions )
	}

	def countIn( motif: DirectedGraph[jena.graph.Node, EdgeNode], queryURI: String ) = {
		handler.handle( motif, queryURI )

		print( "." )

		// if( handler.motifsHandled % 1000 == 0 ) {
		// 	val diffTime = (System.currentTimeMillis - startTime) / 1000.0
		// 	val queriesPerSecond = reader.queryCount / diffTime
		// 	val motifsPerSecond = handler.motifsHandled / diffTime

		// 	val line = dateFormat.format( new Date )+"\t\t\t"+filename+"\n"+reader.queryCount+" queries read\t\t\t"+reader.substitutionNeedingQueryCount+" with predicate vars\n"+handler.motifsHandled.toString+" motifs\t\t\t"+counter.uniqueMotifsEncountered+" unique motifs\n"+queriesPerSecond.toString+" query/s\t\t\t"+motifsPerSecond.toString+" motif/s"

		// 	println( "\n\n"+line+"\n" )
		// }
	}

	class WrappedDirectedGraph( var graph: DirectedGraph[jena.graph.Node, motifs.EdgeNode] ) {
		def predicateVars: Set[Var] = {
			var vars = Set[Var]()
			// println( vars.getClass.toString )

			for( e <- graph.edgeSet; if e.p.isVariable ) {
				vars = vars + e.p.asInstanceOf[Var]
			}

			// graph.edgeSet.foreach( e => {
			// 	if( e.p.isVariable ) {
			// 		// println( e.p.getClass.toString )
			// 		vars = vars + e.p.asInstanceOf[Var]
			// 	}
			// })

			return vars
		}
	}
}
