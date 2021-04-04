kda-app/amazon-kinesis-connector-flink-*.jar:
	mvn dependency:get -Dartifact=software.amazon.kinesis:amazon-kinesis-connector-flink:2.0.2 -Ddest=./kda-app

clean:
	rm kda-app/amazon-kinesis-connector-flink-*.jar

package: kda-app/amazon-kinesis-connector-flink-*.jar
	zip -j kda-app.zip kda-app/*

upload: package
	aws s3 mv kda-app.zip s3://hogefuga/
