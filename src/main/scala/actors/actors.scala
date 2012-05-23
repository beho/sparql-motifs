package motifs.actors

import java.io.{Serializable, ObjectOutputStream, ObjectInputStream}
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection._
import scala.collection.JavaConversions.asScalaSet
import scala.annotation.tailrec

import akka.actor.Actor._
import akka.actor.{ActorRegistry, Actor, ActorRef}
import akka.dispatch.Dispatchers
import akka.serialization.RemoteActorSerialization._
// import akka.event.EventHandler

import com.hp.hpl._
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.sparql.core.Var
import com.hp.hpl.jena.sparql.engine.binding.Binding

import org.jgrapht.DirectedGraph
import org.jgrapht.graph.{DirectedPseudograph, DirectedSubgraph}

// import org.jivesoftware._


// messages

abstract class Message

case class File( filename: String, part: Int )
case class Handle( motif: WrappedDirectedSubgraph, queryURI: String )
case class Ack

case class Register( parts: Set[String] ) extends Message
case class Run() extends Message
case class Done() extends Message

case class GetSubstitutions( query: Query, predicateVars: Set[Var] ) extends Message
case class Substitutions( substitutions: Option[List[Binding]] ) extends Message

case class CountIn( filename: String, motif: WrappedDirectedSubgraph, queryURI: String ) extends Message

case class EndOfBatch( filename: String ) extends Message
case class NextBatch() extends Message

case class NextQuery() extends Message

case class Status( line: String )



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


class MotifEnumerator( val filename: String, val dataset: String, val substitutionsEndpointURL: String, val skipPredicateQueries: Boolean, counterAddrs: Array[(String, Int)] ) extends Actor with motifs.MotifHandler[jena.graph.Node, motifs.EdgeNode] {
	val enumerator = new motifs.MotifEnumerator( filename, dataset, substitutionsEndpointURL, skipPredicateQueries, this )
	val counters = counterAddrs.map( (addr: (String, Int)) => { remote.actorFor( "motif-counter", addr._1, addr._2 ) } )
	val idx = 0

	override def preStart = {
		println( "[info] starting enumerator actor "+filename )

		Actor.remote.registerByUuid( self )
		for( c <- counters; i <- counters.indices ) { c ! File( filename, i ) }
	}
	
	def receive = {
		case Ack => {
			enumerator.run
		}
	}

	def handle( motif: DirectedGraph[jena.graph.Node, motifs.EdgeNode], queryURI: String ) {
		counters(idx) ! Handle( new WrappedDirectedSubgraph( motif ), queryURI )
	}

	def close = {
		for( c <- counters ) { c ! Done }
	}
}

class MotifCounter extends Actor {
	var counter: motifs.TDBMotifCounter = null

	def receive = {
		case File( filename, part ) => {
			counter = new motifs.TDBMotifCounter( filename+"-"+part )
		}

		case Handle( wrappedMotif, queryURI ) => {
			counter.handle( wrappedMotif.graph, queryURI )
		}

		case Done => {
			counter.close

			Actor.registry.shutdownAll
			Actor.remote.shutdownClientModule
			Actor.remote.shutdownServerModule
		}
	}
}

// class NodeMaster( filenames: Set[String], endpointURL: String, motifsCounterAddress: (String, Int) ) extends Actor {
// 	self.dispatcher = Dispatchers.newThreadBasedDispatcher( self )

// 	val motifsCounter = remote.actorFor( "motifs-counter", motifsCounterAddress._1, motifsCounterAddress._2 )
	
// 	val substitutionsObtainer = actorOf( new SubstitutionsObtainer( endpointURL ) )
// 	val enumerators = filenames.map( f => actorOf( new MotifsEnumerator( f, self, substitutionsObtainer, motifsCounter ) ) )
// 	var enumeratorsDone = 0

// 	override def preStart = {
// 		println( "starting master")

// 		if( !motifsCounter.isRunning ) {
// 			substitutionsObtainer.stop
// 			enumerators.foreach( e => e.stop )

// 			println( "ERROR: motifs counter is not running" )
// 			scala.sys.exit(0)
// 		}

// 		Actor.remote.registerByUuid( substitutionsObtainer )
// 		enumerators.foreach( e => Actor.remote.registerByUuid( e ) )

// 		// substitutionsObtainer.start
// 		// enumerators.foreach( e => e.start )

// 		// val selfAsBytes = toRemoteActorRefProtocol( self ).toByteArray
// 		motifsCounter ! Register( filenames )
// 	}

// 	def receive = {
// 		case Run => {
// 			enumerators.foreach{ f => f ! Run }
// 		}
// 		case Done => {
// 			enumeratorsDone += 1
			
// 			if( enumeratorsDone == enumerators.size ) {
// 				println( "MASTER DONE" )

// 				motifsCounter ! Done
// 				// substitutionsObtainer.exit
// 				// self.exit

// 				Actor.registry.shutdownAll

// 				Actor.remote.shutdownClientModule
// 				Actor.remote.shutdownServerModule
// 			}
// 		}
// 	}
	
// }




// class MotifsEnumerator( filename: String, master: ActorRef, substitutionsObtainer: ActorRef, motifsCounter: ActorRef ) extends Actor {
// 	self.dispatcher = Dispatchers.newThreadBasedDispatcher( self )

// 	val reader = new motifs.QueryReader( filename )
// 	val subgraphEnumerator = new motifs.ConnectedSubgraphEnumerator[jena.graph.Node, motifs.EdgeNode]()
// 	val dateFormat = new SimpleDateFormat( "HH:mm:ss.S" )

// 	val batchSize = 1000
	
// 	var currentBatchSize = 0
// 	var edgesInBatch = 0
	
// 	var batchesSent = 0

// 	var motifsEnumerated = 0

// 	var currentQueryURI: Option[String] = None

// 	var enumerationContinuation = () => ()
// 	var graphToSubstituteIn: Option[DirectedGraph[jena.graph.Node, motifs.EdgeNode]] = None

// 	implicit def directedPseudographWrapper( graph: DirectedSubgraph[jena.graph.Node, motifs.EdgeNode]) = new WrappedDirectedSubgraph( graph )

// 	override def preStart = {
// 		println( "starting enumerator ["+filename+"]")
// 	}

// 	def receive = {
// 		case Run => {
// 			println( filename+" : running" )
// 			processNextQuery
// 		}
// 		case NextQuery => {
// 			processNextQuery
// 		}
// 		case NextBatch => {
// 			println( filename+" : starting new batch" )
// 			enumerationContinuation()
// 		}

// 		case Substitutions( maybeSubstitutions ) => {
// 			maybeSubstitutions match {
// 				case Some( substitutions ) => {
// 					// println("received substitutions")
// 					// val found = findMotifsWithSubstitutions( graphToSubstituteIn.get, substitutions )
// 					findMotifsWithSubstitutions( graphToSubstituteIn.get, substitutions )
// 					// foundInBatch += found

// 					// println( filename+": "+found+" motifs S" )

// 					// if( foundInBatch < batchSize ) {
// 					// 	// println( filename+" S "+foundInBatch)
// 					// 	processNextQuery
// 					// }
// 					// else {
// 					// 	println( "["+filename+"] endofbatch S")
// 					// 	foundInBatch = 0
// 					// 	batchesSent += 1
// 					// 	motifsCounter ! EndOfBatch( filename )
// 					// }
// 				}
// 				case None => {
// 					processNextQuery
// 				}
// 			}
// 		}
// 	}

// 	private def processNextQuery: Unit = {
// 		reader.nextQueryGraph match {
// 			case None => {
// 				println( "DONE: "+reader.queryCount+" queries. "+motifsEnumerated.toString+" motifs" )

// 				reader.close
// 				master ! Done

// 				// self.exit
// 			}
// 			case Some( motifs.CompleteGraph( graph, uri ) ) => {
// 				// val found = findMotifs( graph )
// 				currentQueryURI = Some( uri )

// 				findMotifs( graph )
// 				// foundInBatch += found

// 				// println( filename+": "+found+" motifs C" )

// 				// if( foundInBatch < batchSize ) {
// 				// 	// println( filename+" C "+foundInBatch)
// 				// 	processNextQuery
// 				// }
// 				// else {
// 				// 	println( "["+filename+"] endofbatch C")
// 				// 	foundInBatch = 0
// 				// 	batchesSent += 1
// 				// 	motifsCounter ! EndOfBatch( filename )
// 				// }
// 			}
// 			case Some( motifs.SubstitutionsNeedingGraph( graph, uri, query, predicateVars ) ) => {
// 				currentQueryURI = Some( uri )
// 				graphToSubstituteIn = Some( graph )

// 				substitutionsObtainer ! GetSubstitutions( query, predicateVars )
// 			}
// 		}
// 	}

// 	// def findMotifs( graph: DirectedGraph[jena.graph.Node, motifs.EdgeNode] ): Int = {
// 	// 	val subgraphStream = subgraphEnumerator.streamFor( graph )

// 	// 	var motifsCount = 0
// 	// 	subgraphStream.foreach( motif => {
// 	// 		// println( "NOS "+motif.toString )

// 	// 		sendToCounter( motif )
// 	// 		motifsCounter += 1
// 	// 	})

// 	// 	// println( motifsCount+" motifs")

// 	// 	return motifsCount
// 	// }

// 	def endOfBatch( continuation: () => Unit ) = {
// 		println( filename+": "+(edgesInBatch.toFloat / currentBatchSize).toString+" avg edges/motif" )

// 		currentBatchSize = 0
// 		edgesInBatch = 0

// 		enumerationContinuation = continuation

// 		motifsCounter ! EndOfBatch( filename )

// 		batchesSent += 1
// 		println( dateFormat.format( new Date )+"\t"+filename+" : "+batchesSent+". batch sent ("+reader.queryCount+" queries read, "+motifsEnumerated+" motifs)")
// 	}

// 	def findMotifs( graph: DirectedGraph[jena.graph.Node, motifs.EdgeNode] ) = {
// 		nextMotif( subgraphEnumerator.streamFor( graph ) )
// 	}

// 	// @tailrec 
// 	final def nextMotif( stream: Stream[DirectedSubgraph[jena.graph.Node, motifs.EdgeNode]], motifsInGraph: Int = 0 ): Unit = {
// 		if( stream.isEmpty ) {
// 			// println( filename+" : "+motifsInGraph+" motifs" )

// 			// processNextQuery
// 			self ! NextQuery
// 			return
// 		}

// 		if( currentBatchSize > batchSize ) {
// 			endOfBatch( () => nextMotif( stream, motifsInGraph ) )
// 			return
// 		}

// 		sendToCounter( stream.head )
// 		currentBatchSize += 1
		
// 		nextMotif( stream.tail, motifsInGraph + 1 )
// 	}

// 	// def findMotifsWithSubstitutions( graph: DirectedGraph[jena.graph.Node, motifs.EdgeNode], substitutions: List[Binding] ): Int = {
// 	// 	val subgraphStream = subgraphEnumerator.streamFor( graph )
// 	// 	var motifsCount = 0

// 	// 	subgraphStream.foreach( subgraph => {
// 	// 		val predicateVars = subgraph.predicateVars
// 	// 		// println( subgraph.toString+" "+predicateVars.toString )

// 	// 		if( !predicateVars.isEmpty ) {
// 	// 			val withSubstitutions = motifs.PredicateVarSubstitutor.substitute( subgraph, predicateVars, substitutions )

// 	// 			// println("withSubs "+withSubstitutions.toString )
// 	// 			withSubstitutions.foreach( s => {
// 	// 				// println("S "+s.toString)

// 	// 				sendToCounter( s )
// 	// 				motifsCount += 1
// 	// 			})
// 	// 		}
// 	// 		else {
// 	// 			// println("SNOT "+subgraph.toString)
							
// 	// 			sendToCounter( subgraph )
// 	// 			motifsCount += 1
// 	// 		}	
// 	// 	})

// 	// 	// println( motifsCount+" motifs")

// 	// 	return motifsCount
// 	// }

// 	def findMotifsWithSubstitutions( graph: DirectedGraph[jena.graph.Node, motifs.EdgeNode], substitutions: List[Binding] ) = {
// 		nextMotifWithSubstitions( subgraphEnumerator.streamFor( graph ), substitutions )
// 	}

// 	// @tailrec 
// 	final def nextMotifWithSubstitions( stream: Stream[DirectedSubgraph[jena.graph.Node, motifs.EdgeNode]], substitutions: List[Binding], motifsInGraph: Int = 0 ): Unit = {

// 		if( stream.isEmpty ) {
// 			// println( filename+" : "+motifsInGraph+" motifs" )

// 			// processNextQuery
// 			self ! NextQuery
// 			return
// 		}

// 		if( currentBatchSize > batchSize ) {
// 			endOfBatch( () => nextMotifWithSubstitions( stream, substitutions, motifsInGraph ) )
// 			return
// 		}

// 		val motif = stream.head
// 		val predicateVars = motif.predicateVars

// 		val count =
// 			if( !predicateVars.isEmpty ) {
// 				val withSubstitutions = motifs.PredicateVarSubstitutor.substitute( motif, predicateVars, substitutions )

// 				withSubstitutions.foreach( s => {
// 					sendToCounter( s )
// 					currentBatchSize += 1
// 				})

// 				withSubstitutions.size
// 				// nextMotifWithSubstitions( stream.tail, substitutions, motifsInGraph + withSubstitutions.size )
// 			}
// 			else {
// 				sendToCounter( motif )
// 				currentBatchSize += 1

// 				1
// 				// nextMotifWithSubstitions( stream.tail, substitutions, motifsInGraph + 1)
// 			}

// 		nextMotifWithSubstitions( stream.tail, substitutions, motifsInGraph + count )
// 	}

// 	def sendToCounter( motif: DirectedGraph[jena.graph.Node, motifs.EdgeNode] ) = {
// 		motifsCounter ! CountIn( filename, new WrappedDirectedSubgraph( motif ), currentQueryURI.get )
// 		// println(filename+" : "+motif.edgeSet.size+" edges")
// 		edgesInBatch += motif.edgeSet.size

// 		motifsEnumerated += 1
// 		// if( motifsEnumerated % 1000 == 0 ) {
// 		// 	println( filename+": "+motifsEnumerated.toString+" ("+currentBatchSize+","+batchesSent+" sent)" )
// 		// }
// 	}
// }

// class SubstitutionsObtainer( endpointURL: String ) extends Actor {
// 	self.dispatcher = Dispatchers.newThreadBasedDispatcher( self )

// 	val substitutor = new motifs.PredicateVarSubstitutor( endpointURL )

// 	val waitMs = 100
// 	var lastRequestTime = System.currentTimeMillis - waitMs

// 	override def preStart = {
// 		println( "starting substitutions obtainer")
// 	}

// 	def receive = {
// 		case GetSubstitutions( query, vars ) => {
// 			if( System.currentTimeMillis - lastRequestTime < waitMs ) {
// 				Thread.sleep( waitMs )
// 			}

// 			try {
// 				val substitutions = substitutor.substitutionsFor( query, vars )

// 				self.reply( Substitutions( Some( substitutions ) ) )
// 				lastRequestTime = System.currentTimeMillis
// 			}
// 			catch {
// 				case e: Exception => {
// 					println( "\nFAILED TO OBTAIN SUBSTITUTITIONS" )
// 					println( query.toString )
// 					println( e.getMessage+"\n" )

// 					self.reply( Substitutions( None ) )
// 				}
// 			}
// 		}
// 	}
// }

// object MotifsCounter( )

// class MotifsCounter( querySetName: String, nodes: Int ) extends Actor {
// 	self.dispatcher = Dispatchers.newThreadBasedDispatcher( self )

// 	val counter = new motifs.MotifsCounter( querySetName )

// 	val dateFormat = new SimpleDateFormat( "HH:mm:ss.S" )

// 	// var enumerators = Set[ActorRef]()
// 	var enumeratorsRegistered = 0
// 	var enumeratorsDone = 0

// 	var motifsReceived = 0
// 	val motifCountsFromEnumerators = mutable.Map[String, Int]()
// 	val batchCountsFromEnumerators = mutable.Map[String, Int]()

// 	val startTime = System.currentTimeMillis

// 	// val xmppLogger = actorOf[XMPPLogger]
// 	// xmppLogger.start

// 	override def preStart = {
// 		println( "starting motifs counter")
// 	}

// 	def receive = {
// 		case Register( parts ) => {
// 			// val enumeratorActorRef = fromBinaryToRemoteActorRef( enumeratorActorRefAsBytes )
// 			// enumerators = enumerators + enumeratorActorRef
// 			parts.foreach( p => {
// 				motifCountsFromEnumerators.put( p, 0 )
// 				batchCountsFromEnumerators.put( p, 0 )
// 			})

// 			// if( nodes == motifCountsFromEnumerators.size ) {
// 			// 	enumerators.foreach( e => e ! Run )
// 			// }

// 			enumeratorsRegistered += 1

// 			self.reply( Run )

// 			println( "registering parts: "+parts.mkString(" ") )
// 		}
// 		case CountIn( part, wrappedMotif, queryURI ) => {
// 			// val date = new java.util.Date
// 			// println( " counting: "+wrappedMotif.graph.toString )

// 			counter.countIn( wrappedMotif.graph, queryURI )

// 			motifCountsFromEnumerators.put( part, motifCountsFromEnumerators( part ) + 1 )

// 			print(".")
// 			motifsReceived += 1
// 			if( motifsReceived % 1000 == 0 ) {
// 				val diffTime = System.currentTimeMillis - startTime
// 				val motifsPerSecond = motifsReceived / (diffTime / 1000.0)

// 				val line = dateFormat.format( new Date )+"\n"+motifsReceived.toString+"\nbatches : "+batchCountsFromEnumerators.mkString( " " )+"\nmotifs : "+motifCountsFromEnumerators.mkString( " " )+"\n"+motifsPerSecond.toString+" motif/s"

// 				// println( "\n\n"+dateFormat.format( new Date ) )
// 				// println( motifsReceived.toString )
// 				// println( "batches : "+batchCountsFromEnumerators.mkString( " " ) )
// 				// println( "motifs : "+motifCountsFromEnumerators.mkString( " " )+"\n\n" )
				
// 				println( "\n\n"+line+"\n\n")

// 				// xmppLogger ! Status( line )
// 			}
// 		}
// 		case EndOfBatch( part ) => {
// 			batchCountsFromEnumerators.put( part, batchCountsFromEnumerators( part ) + 1 )
// 			self.reply( NextBatch )
// 		}
// 		case Done => {
// 			enumeratorsDone += 1

// 			if( enumeratorsRegistered == enumeratorsDone ) {
// 				val line = "COUNTING DONE: "+motifsReceived.toString+" motifs received"
				
// 				println( "\n\n"+line )
				
// 				// xmppLogger ! Status( line )

// 				counter.close
// 				// println( "exitting" )
// 				// self.exit

// 				Actor.registry.shutdownAll
// 				Actor.remote.shutdownClientModule
// 				Actor.remote.shutdownServerModule
				
// 			}
// 		}
// 	}
// }

// class XMPPLogger extends Actor {
// 	self.dispatcher = Dispatchers.newThreadBasedDispatcher( self )

// 	val config = new smack.ConnectionConfiguration( "jabber.org", 5222 )
// 	config.setReconnectionAllowed( true )
// 	val connection = new smack.XMPPConnection( config )

// 	connection.connect
// 	connection.login( "motifs", ".abc.123#" )

// 	val chat = connection.getChatManager.createChat( "beho@jabbim.cz", new Listener )

// 	def receive = {
// 		case Status( line ) => chat.sendMessage( line ) 
// 	}

// 	override def postStop = {
// 		connection.disconnect()
// 	}

// 	class Listener extends smack.MessageListener {
// 		def processMessage( chat: smack.Chat, message: smack.packet.Message ) = {
// 			// println( message.toXML )
// 	}
// }

// }