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
consume:
	docker exec -it kafka kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 \
		--topic btc-price \
		--from-beginning