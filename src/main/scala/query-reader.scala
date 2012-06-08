package motifs

import motifs.experiments.GraphBuilder

import com.hp.hpl._
import com.hp.hpl.jena.query._
import com.hp.hpl.jena.sparql.algebra._
import com.hp.hpl.jena.sparql.core.Var
import org.jgrapht.alg.ConnectivityInspector
import org.jgrapht.graph._

import java.io._
import java.util.Scanner
import scala.annotation.tailrec
import scala.collection._


abstract class QueryGraph
case class CompleteGraph( graph: DirectedPseudograph[jena.graph.Node, EdgeNode], uri: String, line:String, connected: Boolean ) extends QueryGraph
case class SubstitutionsNeedingGraph( graph: DirectedPseudograph[jena.graph.Node, EdgeNode], uri: String, query: Query, line: String, predicateVars: Set[Var], connected: Boolean ) extends QueryGraph
case class InvalidGraph() extends QueryGraph


class QueryReader( filename: String, predefinedPrefixes: String = "", skipPredicateVarQueries: Boolean = false ) {
	val scanner = new Scanner( new FileInputStream( filename ), "UTF-8" )
	val errors = new PrintWriter( filename+".errors" )

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
		// val builder = new GraphBuilder
				
		val query = QueryFactory.create( prefixes + line )
		val op = Algebra.compile( query )

		val builder = new GraphBuilder( op )
		builder.build

		val graph = builder.graph
		// OpWalker.walk( op, builder )

		queryCount += 1
		val queryURI = queryURITemplate+"/"+filename+"#"+queryCount.toString

		val inspector = new ConnectivityInspector[jena.graph.Node, EdgeNode]( graph )
		val connected = inspector.isGraphConnected

		if( builder.patternsContainPredicateVar ) {
			substitutionNeedingQueryCount += 1

			return SubstitutionsNeedingGraph( graph, queryURI, query, line, builder.predicateVars, connected )

		}
		else {
			return CompleteGraph( graph, queryURI, line, connected )
		}
	}

	def close = {
		scanner.close
		errors.close
	}
}