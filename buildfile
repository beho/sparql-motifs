require 'buildr/scala'
# require 'ruby_gntp'

repositories.remote << 'http://repo1.maven.org/maven2' << 'http://conjars.org/repo' << 'http://repo.akka.io/releases/' << 'http://repo.typesafe.com/typesafe/releases/' << 'https://oss.sonatype.org/content/groups/public' << 'http://guiceyfruit.googlecode.com/svn/repo/releases' << 'http://oss.sonatype.org/content/repositories/releases'

JENA_ARQ = transitive('com.hp.hpl.jena:arq:jar:2.8.8')
JENA_TDB = transitive('com.hp.hpl.jena:tdb:jar:0.8.10')
JGRAPHT = transitive('thirdparty:jgrapht-jdk1.6:jar:0.8.2')
AKKA_ACTOR = transitive('com.typesafe.akka:akka-actor:jar:2.0.1')
AKKA_REMOTE = transitive('com.typesafe.akka:akka-remote:jar:2.0.1')
# SCALA_TEST = 'org.scalatest:scalatest:jar:1.8'

def add_dependencies(pkg)
  tempfile = pkg.to_s.sub(/.jar$/, "-without-dependencies.jar")
  mv pkg.to_s, tempfile

  dependencies = compile.dependencies.map { |d| "-c #{d}"}.join(" ")
  puts "Creating fat jar"
  sh "java -jar tools/autojar.jar -bae -o #{pkg} #{dependencies} #{tempfile}" # v param for details
  # -m target/MANIFEST.MF 
end

def create_classpath_script( dependencies, version )
	puts "Creating classpath script"
	cp = dependencies.map( &:to_s ).join( File::PATH_SEPARATOR )

	File.open( "classpath.sh", "w" ) do |f|
		# f.write( '!#/bin/sh' )
		f.write( "\nexport CLASSPATH=#{cp}\n" )
		f.write( "export CLASSPATH=target/sparql-motifs-#{version}-without-dependencies.jar#{File::PATH_SEPARATOR}.#{File::PATH_SEPARATOR}$CLASSPATH\n" )
	end
end

define 'sparql-motifs' do
	project.version = '0.0.1'
	# puts Buildr.settings.build['scala.test']

	compile.with JENA_ARQ, JENA_TDB, JGRAPHT, AKKA_ACTOR, AKKA_REMOTE
	# compile.using( {:optimise => true } )
	compile.using( {:deprecation => true, :warnings => true, :other => "-unchecked" } )

	create_classpath_script( compile.dependencies, project.version )

	package(:jar).with :manifest => manifest.merge('Main-Class' => 'motifs.commandline.Executor')
	# package(:jar).enhance{ |pkg| pkg.enhance {|pkg| add_dependencies(pkg) } }.with :manifest => manifest.merge('Main-Class' => 'motifs.commandline.Executor')
end

# Growl setup
 # Buildr.application.on_completion do |title, message|
 #   GNTP.notify({
 #     :app_name => "buildr",
 #     :title    => title, 
 #     :text     => message,
 #   })
 # end

 # Buildr.application.on_failure do |title, message|
 #   GNTP.notify({
 #     :app_name => "buildr",
 #     :title    => title, 
 #     :text     => message,
 #   })
 # end