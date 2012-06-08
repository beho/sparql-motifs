package motifs

import com.hp.hpl._
import com.hp.hpl.jena._
import com.hp.hpl.jena.query.{Query, QueryExecutionFactory, QueryFactory}
import com.hp.hpl.jena.sparql.core.Var
import org.jgrapht._

import java.io._
import java.util.Date
import java.text.SimpleDateFormat
import scala.annotation.tailrec
import scala.collection._
import scala.collection.JavaConversions.{asScalaSet, setAsJavaSet}


class MotifEnumerator( val filename: String, val dataset: String, val substitutionsEndpointURL: String, val skipPredicateVarQueries: Boolean = false, val handler: MotifHandler[jena.graph.Node, EdgeNode] ) {
	val reader = new QueryReader( filename, motifs.Prefixes.forDataset( dataset), skipPredicateVarQueries )
	val subgraphEnumerator = new ConnectedSubgraphEnumerator[jena.graph.Node, motifs.EdgeNode]()
	val substitutor = new PredicateVarSubstitutor( substitutionsEndpointURL )

	val predicateVarQueries = new PrintWriter( filename+".predicate-vars" )
	val disconnectedQueries = new PrintWriter( filename+".disconnected" )
	// val counter = new MotifsCounter( filename )

	val dateFormat = new SimpleDateFormat( "HH:mm:ss.S" )
	val startTime = System.currentTimeMillis

	val waitMs = 100
	var lastSubstitutionsTime = System.currentTimeMillis - waitMs

	val failedSubstitutions = new PrintWriter( filename+".subs" )

	def queriesRead = reader.queryCount
	def substitutionNeedingQueryCount = reader.substitutionNeedingQueryCount

	println( "[info] skipping disconnected queries - writing to "+filename+".disconnected" )

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
				predicateVarQueries.close
				disconnectedQueries.close
			}
			case Some( CompleteGraph( graph, uri, line, connected ) ) => {
				if( !connected ) {
					disconnectedQueries.println( line )
				}
				else {
					findMotifs( subgraphEnumerator.streamFor( graph ), uri )
				}

				run
			}
			case Some( SubstitutionsNeedingGraph( graph, uri, query, line, predicateVars, connected ) ) => {
				if( !connected ) {
					disconnectedQueries.println( line )
				}
				else	
				{
					if( skipPredicateVarQueries ) {
						print( "s" )
						predicateVarQueries.println( line )
					}
					else{ 				
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
								failedSubstitutions.println( line )
								
								// scala.sys.exit(1)
							}
						}
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
	}

	class WrappedDirectedGraph( var graph: DirectedGraph[jena.graph.Node, motifs.EdgeNode] ) {
		def predicateVars: Set[Var] = {
			var vars = Set[Var]()
			// println( vars.getClass.toString )

			for( e <- graph.edgeSet; if e.p.isVariable ) {
				vars = vars + e.p.asInstanceOf[Var]
			}

			return vars
		}
	}
}


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



