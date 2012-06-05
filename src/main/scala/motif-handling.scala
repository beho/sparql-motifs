package motifs

import com.hp.hpl._
import com.hp.hpl.jena._
import com.hp.hpl.jena.graph._
import com.hp.hpl.jena.query._
import com.hp.hpl.jena.sparql.core.Var
import com.hp.hpl.jena.sparql.expr._
import com.hp.hpl.jena.tdb._

import org.jgrapht._

import java.io._
import scala.collection._
import scala.collection.JavaConversions.asScalaSet

trait MotifHandler[N, E] {
	def handle( subgraph: DirectedGraph[N, E], queryURI: String )
	def close: Unit
}

trait MotifCounter[N, E] extends MotifHandler[N, E] {
	protected var handled = 0
	protected var unique = 0

	def motifsHandled: Int = handled
	def uniqueMotifsEncountered: Int = unique

	override def handle( subgraph: DirectedGraph[N, E], queryURI: String ) {
		handled += 1
	}
}

class TDBMotifCounter( filename: String ) extends MotifCounter[jena.graph.Node, EdgeNode] {
	// type Subgraph = AbstractGraph[jena.graph.Node, EdgeNode]

	// filename without (last) extension

	val countProperty = Node.createURI( "http://fit.vutbr.cz/query-analysis#timesEncountered" )
	val containedInProperty = Node.createURI( "http://fit.vutbr.cz/query-analysis#containedIn" )

	val motifVarName = "motif"
	val countVarName = "count"
	val motifURITemplate = "http://fit.vutbr.cz/query-analysis#motif-"

	// var uniqueMotifsCount = 0
	// var totalMotifsCount = 0

	val dir = "./tdb/"+filename
	val dirFile = new File( dir )
	if( !dirFile.exists ) dirFile.mkdirs


	var datasetGraph = TDBFactory.createDatasetGraph( dir )
	var graphStore = update.GraphStoreFactory.create( datasetGraph )

	println( "optimizer strategy: "+datasetGraph.getTransform.toString )
	

	override def handle( subgraph: DirectedGraph[jena.graph.Node, EdgeNode], queryURI: String ) = {
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
