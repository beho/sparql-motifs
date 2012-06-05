package motifs.experiments

import motifs._

import com.hp.hpl._
import com.hp.hpl.jena.query._
import com.hp.hpl.jena.sparql.algebra._
import com.hp.hpl.jena.sparql.core.Var
import org.jgrapht.graph._

import java.io._
import java.util.Scanner
import scala.collection._
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object RunBuilder {
	def run( filename: String ) {
		val scanner = new Scanner( new FileInputStream( filename ), "UTF-8" )
		val line = scanner.nextLine

		println( line+"\n" )

		val query = QueryFactory.create( line )
		val op = Algebra.compile( query )

		println( op.toString() )

		val graph = GraphBuilder.buildFrom( op )

		for( e <- graph.edgeSet ) { println(e.p+" "+e.unionPath) }

		// val builder = new GraphBuilderWithUnion

		// OpWalker.walk( op, builder )

		// println( "graph count: "+builder.graphs.length+"\n" )

		// for( g <- builder.graphs ) {
		// 	println( "graph edge count: "+g.edgeSet.size+"" )
		// 	// println("\t"+g.toString)
		// 	for( e <- g.edgeSet ) {
		// 		println("\t"+e.toString+"\n")
		// 	}
		// }

		scanner.close
	}
}

object GraphBuilder {
	val graph = newGraph
	val termToBNode = new mutable.HashMap[jena.graph.Node, jena.graph.Node]
	var predicateVars = Set[Var]()
	var patternsContainPredicateVar = false

	def buildFrom( operator: Op ): DirectedPseudograph[jena.graph.Node, motifs.EdgeNode] = {
		walk( operator, "00c", 0 )
		graph
	}

	private def newGraph = new DirectedPseudograph[jena.graph.Node, EdgeNode]( classOf[EdgeNode] )

	private def walk( operator: Op, unionPath: String, unionDepth: Int ) {
		operator match {
			case o: op.OpBGP => addEdgesFrom( o.getPattern.getList, unionPath )
			case o: op.OpUnion => {
				val newUnionDepth = unionDepth + 1
				walk( o.getLeft, unionPath+"%02dl".format( newUnionDepth ), newUnionDepth )
				walk( o.getRight, unionPath+"%02dr".format( newUnionDepth ), newUnionDepth )
			}
			case o: op.Op0 => {}
			case o: op.Op1 => walk( o.getSubOp, unionPath, unionDepth )
			case o: op.Op2 => {
				walk( o.getLeft, unionPath, unionDepth )
				walk( o.getRight, unionPath, unionDepth )
			}
			case _ => // TODO OpN, OpExt
		}
	}

	private def addEdgesFrom( triples: java.util.List[jena.graph.Triple], unionPath: String ) {
		triples.foreach( pattern => {
			val s = pattern.getSubject
			val p = pattern.getPredicate
			val o = pattern.getObject

			if( !termToBNode.contains( s ) ) {
				termToBNode.put( s, jena.graph.Node.createAnon )
			}
			
			if( !termToBNode.contains( o ) ) {
				termToBNode.put( o, jena.graph.Node.createAnon )
			}

			val source = termToBNode( s )
			val target = termToBNode( o )

			graph.addVertex( source )
			graph.addVertex( target )
			graph.addEdge( source, target, new EdgeNode( p, source, target, unionPath ) )

			val predicate = pattern.getPredicate
			if( predicate.isVariable ) {
				patternsContainPredicateVar = true
				predicateVars = predicateVars + Var.alloc( predicate )
			}
		})
	}
}

class GraphBuilderWithUnion extends OpVisitorBase {
	// var patterns = immutable.Set[jena.graph.Triple]()
	var patternsContainPredicateVar = false
	var predicateVars = Set[Var]()

	val termToBNode = new mutable.HashMap[jena.graph.Node, jena.graph.Node]

	var currentGraph = newGraph
	var graphs = ArrayBuffer[DirectedPseudograph[jena.graph.Node, motifs.EdgeNode]]( currentGraph )

	private def newGraph = new DirectedPseudograph[jena.graph.Node, EdgeNode]( classOf[EdgeNode] )

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

			currentGraph.addVertex( source )
			currentGraph.addVertex( target )
			currentGraph.addEdge( source, target, new EdgeNode( p, source, target ) )

			val predicate = pattern.getPredicate
			if( predicate.isVariable ) {
				patternsContainPredicateVar = true
				predicateVars = predicateVars + Var.alloc( predicate )
			}

			// println( p.toString+"\n" )
		})
	}

	override def visit( union: op.OpUnion ) {
		// println( "forking" )

		val lWalker = new GraphBuilderWithUnion
		val rWalker = new GraphBuilderWithUnion

		OpWalker.walk( union.getLeft, lWalker )
		OpWalker.walk( union.getRight, rWalker )

		val newGraphs = lWalker.graphs ++ rWalker.graphs
		newGraphs.foreach{ g => {
			for( v <- currentGraph.vertexSet ) g.addVertex( v )
			for( e <- currentGraph.edgeSet ) g.addEdge( e.s, e.o, e )
		}}
		graphs -= currentGraph
		graphs ++= newGraphs

		// graphs = graphs ++ lWalker.graphs ++ rWalker.graphs
	}
}