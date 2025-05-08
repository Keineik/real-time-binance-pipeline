# Prerequisites
1. **Java**: Ensure you have Java 8 or Java 11 installed.
2. **Scala**: [Install Scala](https://www.scala-lang.org/download/) any version. 
3. **SBT (Scala Build Tool)**: Install SBT for building and running the project. This tool come with scala install by default. If not, please install it
# Running scripts
## Install python libraries
A python virtual environment is recommended, please run 
```bash
python -m venv .venv
source .venv/bin/activate
```
Install requirements
```bash
pip install -r requirements.txt
```
## All 'make' commands
- Start Services

```bash
  make up
```

- Extract – Simulate BTC Price Stream

```bash
  make extract
  make consume  # Check data on topic: btc-price
```

- Transform 1 – Moving Average & Std. Deviation

```bash
  make transform1
  make consume TOPIC="btc-price-moving"
```

- Transform 2 – Compute Z-Score

```bash
  make transform2
  make consume TOPIC="btc-price-zscore"
```

- Load

```bash
  make load
```

- Bonus

```bash
  make bonus
```

- Stop Services (without removing data)

```bash
  make stop
```

- Resume Services
  Start services again after stopping:

```bash
  make start
```

- Tear Down & Clean Up

```bash
  make down
```
## Verifying Results

After running the load step, check data in MongoDB:
```bash
docker exec -it mongodb mongosh
> use crypto
> show collections
> db["btc-price-zscore-5m"].find().limit(5)
```
## Troubleshooting

- If you have problem running the Extract script (Kafka not found) please wait patiently for the Kafka to startup
- If you have problem "could not find checkpoint..." while running Spark, please run ```make down``` to clean up, ```make up``` to start the containers and run the script again