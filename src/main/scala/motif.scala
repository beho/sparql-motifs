package motifs

import com.hp.hpl._
import com.hp.hpl.jena.sparql.algebra.{OpVisitorBase}
import com.hp.hpl.jena.sparql.algebra.op.OpBGP
import com.hp.hpl.jena.sparql.core.Var
import motifs.fix.NodecSSEFixed
import org.jgrapht.graph.DirectedPseudograph

import java.io.{Serializable, ObjectOutputStream, ObjectInputStream}
import java.nio.ByteBuffer
import scala.collection._
import scala.collection.JavaConversions.asScalaSet

// jung visualisation
// import edu.uci.ics.screencap.PNGDump
// import java.awt.Dimension;


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

object NodecHolder {
	val nodecSSE = new NodecSSEFixed

	// def nodecSSE = nodec
}


class EdgeNode( var p: jena.graph.Node, var s: jena.graph.Node, var o: jena.graph.Node, var unionBranches: Map[Int, Boolean] = new UnionBranches(), var isOptional: Boolean = false ) extends java.io.Serializable {

	override def toString: String = {
		p.toString
	}

	override def equals( o: Any ): Boolean = {
		if( o.isInstanceOf[EdgeNode] ) {
			val e = o.asInstanceOf[EdgeNode]

			// more edges can have same spo (when using unions), the union branch identifies the edge uniquely
			return( e.s == this.s && e.p == this.p && e.o == this.o && e.unionBranches == this.unionBranches )
		}

		return false
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

		// out.writeInt( unionPath.length )
		// out.writeChars( unionPath )

		// println( "writing: "+s.toString+" "+p.toString+" "+o.toString )
	}

	private def readObject( in: ObjectInputStream ) = {
		val nodecSSE = NodecHolder.nodecSSE

		s = nodecSSE.decode( ByteBuffer.wrap( in.readObject.asInstanceOf[Array[Byte]] ), null )
		p = nodecSSE.decode( ByteBuffer.wrap( in.readObject.asInstanceOf[Array[Byte]] ), null )
		o = nodecSSE.decode( ByteBuffer.wrap( in.readObject.asInstanceOf[Array[Byte]] ), null )

		// val pathLength = in.readInt
		// val buffer = Array[Char]()

		// 0.to( pathLength - 1 ).foreach( i => { buffer(i) = in.readChar })
		// unionPath = new String( buffer )

		// println( "reading: "+s.toString+" "+p.toString+" "+o.toString)
	}
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


// class GraphBuilder extends OpVisitorBase {
// 	// var patterns = immutable.Set[jena.graph.Triple]()
// 	var patternsContainPredicateVar = false
// 	var predicateVars = Set[Var]()

// 	val termToBNode = new mutable.HashMap[jena.graph.Node, jena.graph.Node]

// 	var graph = new DirectedPseudograph[jena.graph.Node, EdgeNode]( classOf[EdgeNode] )

// 	override def visit( opBGP: OpBGP ) {
// 		val newPatterns = scala.collection.JavaConversions.asScalaBuffer( opBGP.getPattern.getList )
// 		// patterns = patterns ++ newPatterns

// 		newPatterns.foreach( pattern => {
// 			val s = pattern.getSubject
// 			val p = pattern.getPredicate
// 			val o = pattern.getObject

// 			if( !termToBNode.contains( s ) ) {
// 				termToBNode.put( s, jena.graph.Node.createAnon )
// 			}
			
// 			if( !termToBNode.contains( o ) ){
// 				termToBNode.put( o, jena.graph.Node.createAnon )
// 			}

// 			val source = termToBNode( s )
// 			val target = termToBNode( o )

// 			graph.addVertex( source )
// 			graph.addVertex( target )
// 			graph.addEdge( source, target, new EdgeNode( p, source, target ) )

// 			val predicate = pattern.getPredicate
// 			if( predicate.isVariable ) {
// 				patternsContainPredicateVar = true
// 				predicateVars = predicateVars + Var.alloc( predicate )
// 			}
// 		})
// 	}
// }

