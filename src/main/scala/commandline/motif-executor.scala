package motifs.commandline

// import motifs.MotifEnumerator
import motifs.Prefixes
import motifs.actors._

import akka.actor._
import com.typesafe.config.ConfigFactory

import scala.collection.mutable.Set
import scala.collection.JavaConversions._
import java.net._

object MotifExecutor {
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

			case "-role" :: "counter" :: tail => {
				if( o.contains( 'role ) ) {
					println( "[error] cannot set both counter and enumerator role." )
					scala.sys.exit(1)
				}

				nextOption( o ++ Map( 'role -> 'counter ), tail )
			}

			case "-role" :: "enumerator" :: tail => {
				if( o.contains( 'role ) ) {
					println( "[error] cannot set both counter and enumerator role." )
					scala.sys.exit(1)
				}

				nextOption( o ++ Map( 'role -> 'enumerator ), tail )
			}

			case "-port" :: port :: tail => {
				nextOption( o ++ Map( 'port -> port.toInt ), tail )
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
					case "dbpedia" => "http://147.229.8.52:8890/sparql"
					// case "dbpedia" => "http://dbpedia.org/sparql"
					case "swdf" => "http://147.229.13.234:8890/sparql"
					// case "swdf" => "http://data.semanticweb.org/sparql"
					case _ => ""
				}

				val maybePrefixes = Prefixes.forDataset( name )
        maybePrefixes match {
          case Some( prefixes ) => nextOption( o ++ Map( 'endpoint -> url, 'prefixes -> prefixes ), tail )
          case None => o
        }
			}

			// case "-skip-pvq" :: tail => {
			// 	nextOption( o ++ Map( 'skip -> true ), tail )
			// }

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
			if( !options.contains( 'port ) ) {
				println( "[error] missing port" )
				return false
			}

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
			if( !options.contains( 'port ) ) {
				println( "[error] missing port" )
				return false
			}

			return true
		}

		return true
	}


	def main( args: Array[String] ): Unit = {
		val options = getOptions( args )

		if( !optionsOk( options ) ) {
			println( "-role counter <port>" )
			println( "-role enumerator -dataset <dbpedia | swdf> (-counter <host:port>)+ filename" )
			println( "-role both -dataset <dbpedia | swdf> filename+" )
			sys.exit(1)
		}

		val role = options( 'role ).asInstanceOf[Symbol]

		println( "role: "+role.toString )

		val ip = InetAddress.getLocalHost.getHostAddress
		println( "ip address: "+ip+"\n" )

		val config = ConfigFactory.load

		role match {
			case 'counter => {
				// val querySetName = options( 'querySetName ).asInstanceOf[String]
				// val nodes = options( 'nodes ).asInstanceOf[Int]
				val port = options( 'port ).asInstanceOf[Int]

				// println( "waiting for "+nodes+" nodes to connect\n" )
				val configPrefix = "counter.akka.remote.netty"
				val counterConfig = ConfigFactory.parseMap( Map( configPrefix+".hostname" -> ip.toString, configPrefix+".port" -> port ) )
				val completeConfig = counterConfig.getConfig( "counter" ).withFallback( config.getConfig( "counter" ) )

				// println( completeConfig.toString )

				val system = ActorSystem( "counter", completeConfig )
				val counter = system.actorOf( Props[MotifCounter], name = "motif-counter" )
			}

			case 'enumerator => {
				val dataset = options( 'dataset ).asInstanceOf[String]
				val endpointURL = options( 'endpoint ).asInstanceOf[String]
				val filename = options( 'filename ).asInstanceOf[String]
				val skipPredicateVars = options.contains( 'skip )

				val port = options( 'port ).asInstanceOf[Int]
				println( "[info] starting on port "+port )

				val counters = options( 'counters ).asInstanceOf[Set[(String, Int)]].toArray

				val configPrefix = "enumerator.akka.remote.netty"
				val enumeratorConfig = ConfigFactory.parseMap( Map( configPrefix+".hostname" -> ip.toString, configPrefix+".port" -> port.toString ) )
				// val master = actorOf( new NodeMaster( filenames, endpointURL, counterAddress ) )
				val completeConfig = enumeratorConfig.getConfig( "enumerator" ).withFallback( config.getConfig( "enumerator" ) )

				// println( completeConfig.toString )

				val system = ActorSystem( "enumerator", completeConfig )
				val enumerator = system.actorOf( Props( new MotifEnumerator( filename, dataset, endpointURL, counters ) ), name = "motif-enumerator" )
			}

			case 'both => {
				val dataset = options( 'dataset ).asInstanceOf[String]
				val endpointURL = options( 'endpoint ).asInstanceOf[String]
				val filename = options( 'files ).asInstanceOf[String]

				// filenames.foreach( f => {
					println( "running "+filename )

					val counter = new motifs.TDBMotifCounter( filename)
					val enumerator = new motifs.MotifEnumerator( filename, dataset, endpointURL, counter )
					enumerator.run
				// })
			}
		}
	}
}
