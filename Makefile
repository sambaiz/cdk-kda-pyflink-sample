kda-app/amazon-kinesis-sql-connector-flink-*.jar:
	mvn dependency:get -Dartifact=software.amazon.kinesis:amazon-kinesis-sql-connector-flink:2.0.3 -Ddest=./kda-app

clean:
	rm kda-app/amazon-kinesis-sql-connector-flink-*.jar

package: kda-app/amazon-kinesis-sql-connector-flink-*.jar
	zip -j kda-app.zip kda-app/*

upload: package
	aws s3 mv kda-app.zip s3://hogefuga/
