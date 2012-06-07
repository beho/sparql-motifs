package motifs.experiments

import motifs.{EdgeNode, UnionBranches}

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

		val graph = new GraphBuilder( op ).build

		for( e <- graph.edgeSet ) { println(e.p+" "+e.unionBranches+" "+e.isOptional) }

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

class GraphBuilder( operator: Op ) {
	val graph = newGraph
	val termToBNode = new mutable.HashMap[jena.graph.Node, jena.graph.Node]
	var predicateVars = Set[Var]()
	var patternsContainPredicateVar = false

	var unionIdx = 0

	def build: DirectedPseudograph[jena.graph.Node, motifs.EdgeNode] = {
		walk( operator, new UnionBranches )
		graph
	}

	private def newGraph = new DirectedPseudograph[jena.graph.Node, EdgeNode]( classOf[EdgeNode] )

	private def walk( operator: Op, branches: Map[Int, Boolean], optional: Boolean = false ) {
		operator match {
			case o: op.OpBGP => addEdgesFrom( o.getPattern.getList, branches, optional )
			case o: op.OpUnion => {
				unionIdx += 1
				walk( o.getLeft, branches + ((unionIdx, false)), optional )
				walk( o.getRight, branches + ((unionIdx, true)), optional )
			}
			case o: op.OpLeftJoin => {
				walk( o.getLeft, branches, optional )
				walk( o.getRight, branches, true )
			}
			case o: op.Op0 => {}
			case o: op.Op1 => walk( o.getSubOp, branches, optional )
			case o: op.Op2 => {
				walk( o.getLeft, branches, optional )
				walk( o.getRight, branches, optional )
			}
			case _ => // TODO OpN, OpExt
		}
	}

	private def addEdgesFrom( triples: java.util.List[jena.graph.Triple], branches: Map[Int, Boolean], optional: Boolean ) {
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
			graph.addEdge( source, target, new EdgeNode( p, source, target, branches, optional ) )

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