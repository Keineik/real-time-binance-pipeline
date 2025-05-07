TOPIC ?= btc-price
PYSPARK_FILE ?= src/Transform/22127218_moving.py

up:
	docker compose up --detach --scale spark-worker=2

down:
	docker compose down --volumes

stop:
	docker compose stop

start:
	docker compose start

extract:
	python3 src/Extract/22127218.py

# Use make consume TOPIC="<topic>" to consume from a different topic
consume:
	docker exec -it kafka kafka-console-consumer.sh \
		--bootstrap-server localhost:9094 \
		--topic $(TOPIC)
		
spark-submit:
	docker cp $(PYSPARK_FILE) spark-master:/opt/bitnami/spark
	docker exec -it spark-master \
		spark-submit --master spark://spark-master:7077 \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
		/opt/bitnami/spark/$(notdir $(PYSPARK_FILE))

spark-submit-scala:
	(cd src/Bonus && sbt package)
	docker cp src/Bonus/target/scala-2.12/btcpricestream_2.12-1.0.jar spark-master:/opt/bitnami/spark/
	docker exec -it spark-master \
		spark-submit --master spark://spark-master:7077 \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
		--class BonusPriceStream \
		/opt/bitnami/spark/btcpricestream_2.12-1.0.jar