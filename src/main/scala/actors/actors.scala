package motifs.actors

import motifs.UnionBranches

import akka.actor._
import akka.actor.SupervisorStrategy._
import akka.dispatch.Await
import akka.pattern.ask
import akka.util._
import akka.util.duration._

import com.hp.hpl._
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.sparql.core.Var
import com.hp.hpl.jena.sparql.engine.binding.Binding

import org.jgrapht.DirectedGraph
import org.jgrapht.graph.{DirectedPseudograph, DirectedSubgraph}

import java.io.{Serializable, ObjectOutputStream, ObjectInputStream}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection._
import scala.collection.JavaConversions.asScalaSet
import scala.annotation.tailrec

// import org.jivesoftware._


// messages
abstract class Message
case class StartCounting( filename: String, part: Int ) extends Message
case class Handle( motif: WrappedDirectedSubgraph, queryURI: String ) extends Message
case object Ack extends Message
case object Sync extends Message
case object Done extends Message

class WrappedDirectedSubgraph( var graph: DirectedGraph[jena.graph.Node, motifs.EdgeNode] ) extends Serializable {
	def predicateVars: Set[Var] = {
		var vars = Set[Var]()
		// println( vars.getClass.toString )

		graph.edgeSet.foreach( e => {
			if( e.p.isVariable ) {
				// println( e.p.getClass.toString )
				vars = vars + e.p.asInstanceOf[Var]
			}
		})

		return vars
	}

	private def writeObject( out: ObjectOutputStream ) = {
		val edges = graph.edgeSet

		out.writeInt( edges.size )

		edges.foreach( e =>	{
			out.writeObject( e )
		})

	}

	private def readObject( in: ObjectInputStream ) = {
		graph = new DirectedPseudograph[jena.graph.Node, motifs.EdgeNode]( classOf[motifs.EdgeNode])

		val edgeCount = in.readInt

		1.to( edgeCount ).foreach( _ => {
			val edgeNode = in.readObject.asInstanceOf[motifs.EdgeNode]

			graph.addVertex( edgeNode.s )
			graph.addVertex( edgeNode.o )

			graph.addEdge( edgeNode.s, edgeNode.o, edgeNode )
		})
	}
}

object DisjunctiveTriplesMotifFilter {
	def apply( motif: DirectedGraph[jena.graph.Node, motifs.EdgeNode] ): Boolean = {
		var motifBranches = new UnionBranches()
		// println( motif.edgeSet.size+" edges" )
		for( e <- motif.edgeSet ) {
			// println("motif "+motifBranches.toString+" edge "+e.p.toString+" "+e.unionBranches.toString )
			for( (union: Int, branch: Boolean) <- e.unionBranches ) {
				motifBranches.get( union ) match {
					case Some( motifBranch ) => {
						if( motifBranch != branch ) {
							// println( "\tfalse" )
							return false
						}
					}
					case None => motifBranches.put( union, branch )
				}
			}
		}

		// println( "\ttrue" )
		return true
	}
}

// not used - it makes sense to analyse motifs in optional patterns
object OnlyOptionalPatternsFilter {
	def apply( motif: DirectedGraph[jena.graph.Node, motifs.EdgeNode] ): Boolean = {
		for( e <- motif.edgeSet ) {
			if( !e.isOptional ) return true
		}
		print("o")
		return false
	}
}

class MotifEnumerator( val filename: String, val dataset: String, val substitutionsEndpointURL: String, counterAddrs: Array[(String, Int)] ) extends Actor with motifs.MotifCounter[jena.graph.Node, motifs.EdgeNode] {
	val enumerator = new motifs.MotifEnumerator( filename, dataset, substitutionsEndpointURL, this )
	val dateFormat = new SimpleDateFormat( "HH:mm:ss.S" )

	var counters: Array[ActorRef] = _
	var idx = 0
	var acks = 0

	var startTime: Long = _
	var filtered = 0

	override def preStart = {
		println( "[info] starting enumerator actor "+filename )

		counters = counterAddrs.map( (addr: (String, Int)) => {
			val addrString = "akka://counter@"+addr._1+":"+addr._2+"/user/motif-counter"
			println( "[info] connecting to counter @ "+addrString )
			context.actorFor( addrString )
		})

		for( i <- counters.indices ) {
			val c = counters(i)
			println( "[info] registering with counter "+i+" @ "+c.toString)
			c ! StartCounting( filename, i )
		}
	}

	override def postStop = {
		println( "\n\n[info] enumerator stopped" )
	}

	def receive = {
		case Ack => {
			acks += 1
			if( acks == counters.length ) {
				startTime = System.currentTimeMillis
				enumerator.run
			}
		}
	}

	override def handle( motif: DirectedGraph[jena.graph.Node, motifs.EdgeNode], queryURI: String ) {
		if( !DisjunctiveTriplesMotifFilter( motif ) ) { //|| !OnlyOptionalPatternsFilter( motif ) ) {
			filtered += 1
			if( filtered % 10000 == 0 ) println( "\n[info] filtered: "+filtered )
			return
		}

		super.handle( motif, queryURI )

		idx = (idx + 1) % counters.length
		// println( "sending to counter "+idx )
		counters(idx) ! Handle( new WrappedDirectedSubgraph( motif ), queryURI )

		print( "." )

		if( motifsHandled % 1000 == 0 ) {
			val diffTime = (System.currentTimeMillis - startTime) / 1000.0
			val queriesPerSecond = enumerator.queriesRead / diffTime
			val motifsPerSecond = motifsHandled / diffTime

			val line = dateFormat.format( new Date )+"\t\t\t"+filename+"\n"+enumerator.queriesRead+" queries read\t\t\t"+enumerator.substitutionNeedingQueryCount+" with predicate vars\n"+motifsHandled+" motifs\t\t\t"+filtered+" filtered\n"+queriesPerSecond+" query/s\t\t\t"+motifsPerSecond+" motif/s"

			println( "\n\n"+line+"\n" )
		}

		if( motifsHandled % 100000 == 0) {
			print( "\n\nsyncing "+counters.length+" counters after "+motifsHandled+" motifs ... ")
			implicit val timeout = Timeout( Duration( 168, "hours" ) )
			for( i <- counters.indices ) {
				val c = counters(i)
				val future = c ? Sync
				Await.ready( future, Duration.Inf )
				print( i+" " )
			}
			println( "\n\n" )
		}
	}

	def close = {
		for( c <- counters ) {
			c ! Done

			context.system.shutdown
		}

		val diffTime = (System.currentTimeMillis - startTime) / 1000.0
		val queriesPerSecond = enumerator.queriesRead / diffTime
		val motifsPerSecond = motifsHandled / diffTime

		val line = dateFormat.format( new Date )+"\t\t\t"+filename+"\n"+enumerator.queriesRead+" queries read\t\t\t"+enumerator.substitutionNeedingQueryCount+" with predicate vars\n"+motifsHandled+" motifs\t\t\t"+filtered+" filtered\n"+queriesPerSecond+" query/s\t\t\t"+motifsPerSecond+" motif/s"

		println( "\n\n"+line+"\n" )
	}
}

class MotifCounter extends Actor {
	var counter: motifs.TDBMotifCounter = null
	var dbName: String = _
	val dateFormat = new SimpleDateFormat( "HH:mm:ss.S" )
	var startTime: Long = _

	override val supervisorStrategy = OneForOneStrategy( 0, 1 minute) {
		case _ => Stop
	}

	override def preStart = {
		println( "[info] starting counter actor" )
		println( self.toString )
	}

	override def postStop = {
		println( "\n\n[info] counter stopped" )
	}

	def receive = {
		case StartCounting( filename, part ) => {
			dbName = filename+"-"+part
			println( "[info] starting counting '"+dbName+"'" )

			counter = new motifs.TDBMotifCounter( dbName )

			sender ! Ack

			startTime = System.currentTimeMillis
		}

		case Handle( wrappedMotif, queryURI ) => {
			counter.handle( wrappedMotif.graph, queryURI )

			print( "." )

			if( counter.motifsHandled % 1000 == 0 ) {
				val diffTime = (System.currentTimeMillis - startTime) / 1000.0
				val motifsPerSecond = counter.motifsHandled / diffTime

				val line = dateFormat.format( new Date )+"\t\t\t"+dbName+"\n"+counter.motifsHandled+" motifs\t\t\t"+counter.uniqueMotifsEncountered+" unique motifs\n"+motifsPerSecond+" motif/s"

				println( "\n\n"+line+"\n" )
			}
		}

		case Sync => {
			println( "\n[info] syncing" )
			sender ! Ack
		}

		case Done => {
			println( "\n[info] done" )

			val diffTime = (System.currentTimeMillis - startTime) / 1000.0
			val motifsPerSecond = counter.motifsHandled / diffTime
			val line = dateFormat.format( new Date )+"\t\t\t"+dbName+"\n"+counter.motifsHandled+" motifs\t\t\t"+counter.uniqueMotifsEncountered+" unique motifs\n"+motifsPerSecond+" motif/s"

			println( "\n\n"+line+"\n" )

			counter.close

			context.system.shutdown
		}

		case x:AnyRef => println( x.toString )
	}
}
