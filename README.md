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
