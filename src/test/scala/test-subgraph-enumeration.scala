import org.scalatest.FunSuite

import motifs._
import org.jgrapht.graph._

import com.hp.hpl._

class SubgraphEnumerationTest extends FunSuite {
	test( "enumerating star with 3 edges" ) {
		val enumerator = new ConnectedSubgraphEnumerator[jena.graph.Node, EdgeNode]()
	
		val g = new DirectedPseudograph[jena.graph.Node, EdgeNode]( classOf[EdgeNode] )

		// val v1 = jena.graph.Node.createURI( "http://a.com/1" )
		// val v2 = jena.graph.Node.createURI( "http://a.com/2" ) 
		// val v3 = jena.graph.Node.createURI( "http://a.com/3" ) 
		// val v4 = jena.graph.Node.createURI( "http://a.com/4" ) 

		val v1 = jena.graph.Node.createAnon
		val v2 = jena.graph.Node.createAnon
		val v3 = jena.graph.Node.createAnon
		val v4 = jena.graph.Node.createAnon


		val e1 = new EdgeNode( jena.graph.Node.createURI( "http://a.com/e1" ), v1, v2 )
		val e2 = new EdgeNode( jena.graph.Node.createURI( "http://a.com/e2" ), v1, v3 )
		val e3 = new EdgeNode( jena.graph.Node.createURI( "http://a.com/e2" ), v1, v4 )
	
		g.addVertex( v1 )
		g.addVertex( v2 )
		g.addVertex( v3 )
		g.addVertex( v4 )
	
		g.addEdge( v1, v2, e1 )
		g.addEdge( v1, v3, e2 )
		g.addEdge( v1, v4, e3 )
	
		val stream = enumerator.streamFor( g )
		
		var count = 0
		stream.foreach( subgraph => { 
			println( subgraph.toString ) 
			count += 1
		})

		assert( count == 7 )
	}
}

// SubgraphEnumerationTest.main( args )