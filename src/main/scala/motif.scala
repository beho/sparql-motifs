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
			// otherwise the right one doesn't necessarily make it into the motif
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
	}

	private def readObject( in: ObjectInputStream ) = {
		val nodecSSE = NodecHolder.nodecSSE

		s = nodecSSE.decode( ByteBuffer.wrap( in.readObject.asInstanceOf[Array[Byte]] ), null )
		p = nodecSSE.decode( ByteBuffer.wrap( in.readObject.asInstanceOf[Array[Byte]] ), null )
		o = nodecSSE.decode( ByteBuffer.wrap( in.readObject.asInstanceOf[Array[Byte]] ), null )
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
