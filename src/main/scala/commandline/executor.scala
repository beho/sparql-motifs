package motifs.commandline

// import motifs.MotifEnumerator
import motifs.actors._

import akka.actor._
import com.typesafe.config.ConfigFactory

import scala.collection.mutable.Set
import java.net._

object Executor {
	val nodes = List( ("localhost", 2552), ("localhost", 2553) )


	type Options = Map[Symbol, Any]

	private def getOptions( args: Array[String] ): Options = {
		val argsList = args.toList
		val o = Map( 'counters -> Set() )

		return nextOption( o, argsList )
	}

	private def nextOption( o: Options, args: List[String] ): Options = {
		args match {
			case "-role" :: "both" :: tail => {
				nextOption( o ++ Map( 'role -> 'both ), tail )
			}

			case "-role" :: "counter" :: port :: tail => {
				if( o.contains( 'role ) ) {
					println( "[error] cannot set both counter and enumerator role." )
					scala.sys.exit(1)
				}

				nextOption( o ++ Map( 'role -> 'counter, 'port -> port.toInt ), tail )
			}

			case "-role" :: "enumerator" :: tail => {
				if( o.contains( 'role ) ) {
					println( "[error] cannot set both counter and enumerator role." )
					scala.sys.exit(1)				
				}
					
				nextOption( o ++ Map( 'role -> 'enumerator ), tail )
			}

			case "-counter" :: addr :: tail => {
				val array = addr.split(":")
				println( "adding counter @ "+array(0)+":"+array(1) )
				o( 'counters ).asInstanceOf[Set[(String, Int)]].add( (array(0), array(1).toInt) )
				nextOption( o, tail )
				// o ++ Map( 'counters -> counters )
			}

			case "-dataset" :: name :: tail => {
				val url = name match {
					// case "dbpedia" => "http://nlpmosaic.fit.vutbr.cz:8890/sparql"
					case "dbpedia" => "http://dbpedia.org/sparql"
					case "swdf" => "http://147.229.13.234:8890/sparql"
					// case "swdf" => "http://data.semanticweb.org/sparql"
					case _ => {
						println( "[error] unknown dataset "+name )
						scala.sys.exit(1)
					}
				}
				nextOption( o ++ Map( 'endpoint -> url, 'dataset -> name ), tail )
			}

			case "-skip-pvq" :: tail => {
				nextOption( o ++ Map( 'skip -> true ), tail )
			}

			case file :: Nil => {
				// val files = o( 'files ).asInstanceOf[Set[String]] + file
				o ++ Map( 'filename -> file )
			}

			case _ => o
		}
	}

	private def optionsOk( options: Options ): Boolean = {
		if( !options.contains( 'role ) ) {
			return false
		}

		if( options('role).asInstanceOf[Symbol] == 'enumerator ) {
			if( !options.contains( 'dataset ) ) {
				println( "[error] missing dataset" )
				return false
			}

			if( !options.contains( 'endpoint ) ) {
				println( "[error] missing endpoint" )
				return false
			}

			if( !options.contains( 'filename ) ) {
				println( "[error] missing filename" )
				return false
			}

			// if( options.contains( 'counterAddress ) )
			// 	return false

			if( options( 'counters ).asInstanceOf[Set[_]].size == 0 ) {
				println( "[error] no counters" )
				return false
			}

			return true
		}

		if( options( 'role ).asInstanceOf[Symbol] == 'counter ) {
			if( !options.contains( 'port ) )
				return false

			return true
		}

		return true
	}

	
	def main( args: Array[String] ): Unit = {
		val options = getOptions( args )

		if( !optionsOk( options ) ) {
			println( "-role counter <port>" )
			println( "-role enumerator -dataset <dbpedia | swdf> [-skip-pvq] (-counter <host:port>)+ filename" )
			println( "-role both -dataset <dbpedia | swdf> [-skip-pvq] filename+" )
			sys.exit(1)
		}

		val role = options( 'role ).asInstanceOf[Symbol]

		println( "role: "+role.toString )

		val ip = InetAddress.getLocalHost.getHostAddress
		println( "ip address: "+ip+"\n" )

		role match {
			case 'counter => {
				// val querySetName = options( 'querySetName ).asInstanceOf[String]
				// val nodes = options( 'nodes ).asInstanceOf[Int]
				val port = options( 'port ).asInstanceOf[Int]

				// println( "waiting for "+nodes+" nodes to connect\n" )
				val config = ConfigFactory.load
				// println( config.toString )

				val system = ActorSystem( "counter", config.getConfig("counter") )
				val counter = system.actorOf( Props[MotifCounter], name = "motif-counter" )

				// Actor.remote.start( ip, port )
				// Actor.remote.register( "motifs-counter", counter )
			}

			case 'enumerator => {
				val dataset = options( 'dataset ).asInstanceOf[String]
				val endpointURL = options( 'endpoint ).asInstanceOf[String]
				val filename = options( 'filename ).asInstanceOf[String]
				val skipPredicateVars = options.contains( 'skip )

				// val port = options( 'port ).asInstanceOf[Int]
				val counters = options( 'counters ).asInstanceOf[Set[(String, Int)]].toArray
				

				// val master = actorOf( new NodeMaster( filenames, endpointURL, counterAddress ) )
				val system = ActorSystem( "enumerator", ConfigFactory.load.getConfig("enumerator") )
				val enumerator = system.actorOf( Props( new MotifEnumerator( filename, dataset, endpointURL, skipPredicateVars, counters ) ), name = "motif-enumerator" ) 

				// Actor.remote.start( ip, port )
				// Actor.remote.register( "enumberator", enumerator )
			}

			case 'both => {
				val dataset = options( 'dataset ).asInstanceOf[String]
				val endpointURL = options( 'endpoint ).asInstanceOf[String]
				val filename = options( 'files ).asInstanceOf[String]
				val skipPredicateVars = options.contains( 'skip )

				// filenames.foreach( f => {
					println( "running "+filename )

					val counter = new motifs.TDBMotifCounter( filename)
					val enumerator = new motifs.MotifEnumerator( filename, dataset, endpointURL, skipPredicateVars, counter )
					enumerator.run
				// })
			}
		}
	}
}