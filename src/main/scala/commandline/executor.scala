package motifs.commandline

// import motifs.MotifEnumerator
import motifs.actors._

import akka.actor.Actor._
import akka.actor.{ActorRegistry, Actor}

import java.net._

object Executor extends App {
	val nodes = List( ("localhost", 2552), ("localhost", 2553) )


	type Options = Map[Symbol, Any]

	private def getOptions( args: Array[String] ): Options = {
		val argsList = args.toList
		val o = Map( 'files -> Set() )

		return nextOption( o, argsList )
	}

	private def nextOption( o: Options, args: List[String] ): Options = {
		args match {
			case Nil => o

			case "-role" :: "counter" :: port :: querySetName :: nodesCount :: tail => {
				if( o.contains( 'role ) ) {
					println( "cannot set both counter and enumerator role." )
					scala.sys.exit(1)
				}

				nextOption( o ++ Map( 'role -> 'counter, 'querySetName -> querySetName, 'nodes -> nodesCount.toInt, 'port -> port.toInt ), tail )
			}

			case "-role" :: "enumerator" :: port :: counterHost :: counterPort :: tail => {
				if( o.contains( 'role ) ) {
					println( "cannot set both counter and enumerator role." )
					scala.sys.exit(1)				
				}
					
				nextOption( o ++ Map( 'role -> 'enumerator, 'counterAddress -> (counterHost, counterPort.toInt), 'port -> port.toInt ) , tail )				
			}

			case "-role" :: "both" :: tail => {
				nextOption( o ++ Map( 'role -> 'both ), tail )
			}

			case "-dataset" :: name :: tail => {
				val url = name match {
					// case "dbpedia" => "http://nlpmosaic.fit.vutbr.cz:8890/sparql"
					case "dbpedia" => "http://dbpedia.org/sparql"
					case "swdf" => "http://147.229.13.234:8890/sparql"
					// case "swdf" => "http://data.semanticweb.org/sparql"
					case _ => {
						println( "unknown dataset "+name )
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
				o ++ Map( 'file -> file )
			}
		}
	}

	private def optionsOk( options: Options ): Boolean = {
		if( !options.contains( 'role ) ) {
			return false
		}

		if( options('role).asInstanceOf[Symbol] == 'enumerator ) {
			if( !options.contains( 'counterAddress ) || !options.contains( 'endpoint ) )
				return false

			return ( options( 'files ).asInstanceOf[Set[String]].size > 0 )
		}

		return true
	}

	
	override def main( args: Array[String] ) = {
		val options = getOptions( args )

		if( !optionsOk( options ) ) {
			println( "-role counter <port> <name of a query set> <nr of parts>" )
			println( "-role enumerator <port> <counter host> <counter port> -dataset <dbpedia | swdf> filename+" )
			println( "-role both -dataset <dbpedia | swdf> [-skip-pvq] filename+" )
			sys.exit(1)
		}

		val role = options( 'role ).asInstanceOf[Symbol]

		println( "role: "+role.toString )

		val ip = InetAddress.getLocalHost.getHostAddress
		println( "ip address: "+ip+"\n" )

		role match {
			case 'counter => {
				val querySetName = options( 'querySetName ).asInstanceOf[String]
				val nodes = options( 'nodes ).asInstanceOf[Int]
				val port = options( 'port ).asInstanceOf[Int]

				println( "waiting for "+nodes+" nodes to connect\n" )

				val counter = actorOf( new MotifCounter( querySetName, nodes ) )

				Actor.remote.start( ip, port )
				Actor.remote.register( "motifs-counter", counter )
			}

			case 'enumerator => {
				val endpointURL = options( 'endpoint ).asInstanceOf[String]
				val counterAddress = options( 'counterAddress ).asInstanceOf[(String, Int)]
				val fileneme = options( 'file ).asInstanceOf[String]
				val port = options( 'port ).asInstanceOf[Int]

				// val master = actorOf( new NodeMaster( filenames, endpointURL, counterAddress ) )
				val enumerator = actorOf( new MotifEnumerator( filename, dataset, endpointURL, skipPredicateVars, counterAddrs) )

				Actor.remote.start( ip, port )
				Actor.remote.register( "master", master )
			}

			case 'both => {
				val dataset = options( 'dataset ).asInstanceOf[String]
				val endpointURL = options( 'endpoint ).asInstanceOf[String]
				val filenames = options( 'files ).asInstanceOf[Set[String]]
				val skipPredicateVars = options.contains( 'skip )

				filenames.foreach( f => {
					println( "running "+f )

					val finder = new motifs.MotifEnumerator( f, dataset, endpointURL, skipPredicateVars )
					finder.run
				})
			}
		}
	}
}