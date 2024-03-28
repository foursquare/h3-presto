# Sample Docker File

It can be difficult to test h3-presto if you don't have a Presto server running already. These instructions help you get a Presto node with h3-presto running for development. (These instructions are based on the [Presto Docker deployment](https://prestodb.io/docs/current/installation/deployment.html#an-example-deployment-with-docker) instructions.)

To build, starting from the root of the repository:

```sh
mvn clean package -DskipTests
cd contrib/docker
cp ../../target/h3-presto-4.0.0.jar plugin/h3/h3-presto-4.0.0.jar
docker build -f Dockerfile -t presto --build-arg PRESTO_VERSION=0.286 .
```

To run:

```sh
docker run -p 8080:8080 --rm -it presto
```

And then in another terminal, run the [Presto command line interface](https://prestodb.io/docs/current/installation/cli.html):

```sh
./presto
```

And try a sample query:

```sql
select h3_latlng_to_cell(0,0,0);
```
