docker compose down && docker compose up -d
sleep 600

curl -X POST http://localhost:5000/start -d "begin=2018-01-01" -d "end=2018-02-01"
