package motifs

import com.hp.hpl._
import com.hp.hpl.jena._
import com.hp.hpl.jena.tdb._

import org._

object MergeHelper {
	val sVar = sparql.core.Var.alloc( "s" )
	val pVar = sparql.core.Var.alloc( "p" )
	val oVar = sparql.core.Var.alloc( "o" )

	def buildDirectedGraph( iterator: sparql.engine.QueryIterator ): jgrapht.DirectedGraph[jena.graph.Node, EdgeNode] = {
		val g = new jgrapht.graph.DirectedPseudograph[jena.graph.Node, EdgeNode]( classOf[EdgeNode] )

		// iterator.foreach( solution => {
		while( iterator.hasNext ) {
			val solution = iterator.nextBinding

			val s = solution.get( sVar )
			val p = solution.get( pVar )
			val o = solution.get( oVar )

			g.addVertex( s )
			g.addVertex( o )
			g.addEdge( s, o, new EdgeNode( p, s, o) )
		}

		return g
	}
}

class Merger( sourceDir: String, targetDir: String ) {
	val motifURITemplate = "http://fit.vutbr.cz/query-analysis#motif-"
	val countProperty = jena.graph.Node.createURI( "http://fit.vutbr.cz/query-analysis#timesEncountered" )
	val containedInProperty = jena.graph.Node.createURI( "http://fit.vutbr.cz/query-analysis#containedIn" )
	val motifVar = sparql.core.Var.alloc( "motif" )
	val countVar = sparql.core.Var.alloc( "count" )
	val queryVar = sparql.core.Var.alloc( "query" )
	val motifCountVar = sparql.core.Var.alloc( "motifsCount" )
	val motifsCountQuery = query.QueryFactory.create( "SELECT (COUNT(?motif) AS ?motifsCount) WHERE { GRAPH ?motif {} }" )

	def run {
		println( "source : "+sourceDir )
		println( "target : "+targetDir )

		val source = TDBFactory.createDatasetGraph( sourceDir )

		val sourceMotifsCountIterator = query.QueryExecutionFactory.createPlan( motifsCountQuery, source ).iterator
		val sourceMotifsCount = sourceMotifsCountIterator.next.get( motifCountVar ).getLiteral.getValue.asInstanceOf[Int]
		println( "source motifs count : "+sourceMotifsCount.toString )
		// println( "src: "+source.toString )

		val targetDatasetGraph = TDBFactory.createDatasetGraph( targetDir )
		val target = update.GraphStoreFactory.create( targetDatasetGraph )

		val targetMotifsCountIterator = query.QueryExecutionFactory.createPlan( motifsCountQuery, target ).iterator
		var targetMotifsCount = targetMotifsCountIterator.next.get( motifCountVar ).getLiteral.getValue.asInstanceOf[Int]
		println( "target motifs count : "+targetMotifsCount.toString )

		val motifsQuery = query.QueryFactory.create( "SELECT ?motif ?count WHERE { GRAPH ?motif {}. ?motif <http://fit.vutbr.cz/query-analysis#timesEncountered> ?count }" )
		val motifs = query.QueryExecutionFactory.createPlan( motifsQuery, source ).iterator

		var processed = 0
		val start = System.currentTimeMillis

		while( motifs.hasNext ) {
			TDB.sync( target )

			val binding = motifs.nextBinding

			val sourceMotifURI = binding.get( motifVar )

			val sourceCountLiteral = binding.get( countVar )
			val sourceCount = sourceCountLiteral.getLiteral.getValue.asInstanceOf[Int]

			val motifTriplesQuery = query.QueryFactory.create( "SELECT ?s ?p ?o WHERE { GRAPH <"+sourceMotifURI.toString+"> { ?s ?p ?o } }" )
			val triples = query.QueryExecutionFactory.createPlan( motifTriplesQuery, source ).iterator

			val motifGraph = MergeHelper.buildDirectedGraph( triples )
			val targetMotifQuery = QueryHelper.queryFor( motifGraph )

			val targetMotifIterator = query.QueryExecutionFactory.createPlan( targetMotifQuery, target ).iterator

			if( targetMotifIterator.hasNext ) {
				// print( "found in target : " )

				val targetMotif = targetMotifIterator.nextBinding
				val targetMotifURI = targetMotif.get( motifVar )

				val targetCountLiteral = targetMotif.get( countVar )
				val targetCount = targetCountLiteral.getLiteral.getValue.asInstanceOf[Int]

				target.getDefaultGraph.delete( new jena.graph.Triple( targetMotifURI, countProperty, targetCountLiteral ) )

				val newCount = sourceCount + targetCount
				val newCountLiteral = QueryHelper.createUnsignedIntLiteral( newCount )

				target.getDefaultGraph.add( new jena.graph.Triple( targetMotifURI, countProperty, newCountLiteral ) )

				val containedInQuery = query.QueryFactory.create( "select ?query where { <"+sourceMotifURI.toString+"> <http://fit.vutbr.cz/query-analysis#containedIn> ?query }" )
				val containedIn = query.QueryExecutionFactory.createPlan( containedInQuery, source ).iterator

				while( containedIn.hasNext ) {
					val queryURI = containedIn.nextBinding.get( queryVar )

					target.getDefaultGraph.add( new jena.graph.Triple( targetMotifURI, containedInProperty, queryURI ) )
				}
			}
			else {
				// println( "not found in target" )

				targetMotifsCount = targetMotifsCount + 1
				val newTargetMotifURI = jena.graph.Node.createURI( motifURITemplate + targetMotifsCount.toString )

				val motifRDFGraph = QueryHelper.subgraphToRDFGraph( motifGraph )

				target.addGraph( newTargetMotifURI, motifRDFGraph )

				target.getDefaultGraph.add( new jena.graph.Triple( newTargetMotifURI, countProperty, sourceCountLiteral ) )
				// create RDF graph and insert to "to" with fromCount

				val containedInQuery = query.QueryFactory.create( "select ?query where { <"+sourceMotifURI.toString+"> <http://fit.vutbr.cz/query-analysis#containedIn> ?query }" )
				val containedIn = query.QueryExecutionFactory.createPlan( containedInQuery, source ).iterator

				while( containedIn.hasNext ) {
					val queryURI = containedIn.nextBinding.get( queryVar )

					target.getDefaultGraph.add( new jena.graph.Triple( newTargetMotifURI, containedInProperty, queryURI ) )
				}
			}

			processed = processed + 1

			print(".")

			if( processed % 1000 == 0 ) {
				println( "\n"+processed.toString+" / "+sourceMotifsCount.toString )
			}
		}

		TDB.sync( target )
		source.close
		target.close

		val end = System.currentTimeMillis
		val diff = (end - start) / 1000.0

		println( "\n"+diff.toString+"s ("+sourceMotifsCount/diff+" motif/s)" )
	}
}
