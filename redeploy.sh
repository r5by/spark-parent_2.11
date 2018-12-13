#/bin/shell
 ./dev/make-distribution.sh --name spark-pigeon --tgz -Phadoop-2.6 -Pyarn
 rm -rf /Users/ruby_/VirtualBox-sf/spark-pigeon-deploy/spark-2.0.0-SNAPSHOT-bin-spark-pigeon.tgz
 cp ./spark-2.0.0-SNAPSHOT-bin-spark-pigeon.tgz /Users/ruby_/VirtualBox-sf/spark-pigeon-deploy/
 rm -rf /Users/ruby_/VirtualBox-sf/spark-pigeon-deploy/examples.jar
 cp ./examples/target/spark-examples_2.11-2.0.0-SNAPSHOT.jar /Users/ruby_/VirtualBox-sf/spark-pigeon-deploy/
 mv /Users/ruby_/VirtualBox-sf/spark-pigeon-deploy/spark-examples_2.11-2.0.0-SNAPSHOT.jar /Users/ruby_/VirtualBox-sf/spark-pigeon-deploy/examples.jar
