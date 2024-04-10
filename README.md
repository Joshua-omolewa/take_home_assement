# Data Engineering Take Home Assignment:

### How to Run:

Run the entire application:
```bash
docker-compose up --build
```

Run the python application only:
```bash
docker-compose up --build pythonapp
```

Connect to postgres database:
```bash
docker exec -it $(docker-compose ps -q  db) psql -U user -d mydatabase
```

# PROJECT ARCHITECTURE

<img src="https://github.com/Joshua-omolewa/take_home_assement/blob/main/images/Project%20Architecture.gif"  width="100%" height="100%">