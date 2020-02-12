# esmanage
Simple python script to manage elasticsearch indexes and aliases

## Build image

```
$ docker build -t esmanage:latest .
```

## Running the container

```
$ docker run -ti --network yournet esmanage:latest --help
```

List indexes

```
$ docker run -ti --network yournet esmanage:latest --host=ELASTICHOST index list
```

List aliases

```
$ docker run -ti --network yournet esmanage:latest --host=ELASTICHOST alias list
```
