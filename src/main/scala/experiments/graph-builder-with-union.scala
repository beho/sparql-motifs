package motifs.experiments

import motifs.{EdgeNode, UnionBranches}

import com.hp.hpl._
import com.hp.hpl.jena.query._
import com.hp.hpl.jena.sparql.algebra._
import com.hp.hpl.jena.sparql.core.Var
import org.jgrapht.graph._
import org.jgrapht.alg.ConnectivityInspector

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

		val inspector = new ConnectivityInspector[jena.graph.Node, EdgeNode]( graph )

		println( "connected: "+inspector.isGraphConnected )

		scanner.close
	}
}

class GraphBuilder( operator: Op ) {
	val graph = newGraph
	val termToBNode = new mutable.HashMap[jena.graph.Node, jena.graph.Node]
	var predicateVars = mutable.Set[Var]()
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
				val idx = unionIdx // must hold unionIdx locally in order to not to be rewritten
				// println("starting "+unionIdx)
				walk( o.getLeft, branches + ((idx, false)), optional )
				walk( o.getRight, branches + ((idx, true)), optional )
			}
			// not used - it makes sense to analyse motifs in optional patterns
			// case o: op.OpLeftJoin => {
			// 	walk( o.getLeft, branches, optional )
			// 	walk( o.getRight, branches, true )
			// }
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
				predicateVars += Var.alloc( predicate )
			}
		})
	}
}
