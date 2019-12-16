# SPF-client
A multi-threaded TPF, brTPF, and SPF client written in Java 8.

## Build
Execute the following command to create a JAR file:
```
$ mvn install
```

## Usage
Use the following command
```
java -jar [filename].jar -t false -f [Starting Fragment] -q [Query File]
```

## Run tests
To run tests similar to the ones in the paper, you must run the jar file once per client you are running. Then, use the following command per client.
```
java -jar [filename].jar -t true [Starting Fragment] [Client Directory] [Approach] [Output Directory] [No. Clients] [Client No.] [Query Load]
```

In the [Client Directory], there must be one subdirectory per query load entitled [Query Load].

To full test setup used in the paper will be available soon on our website.

---   
Any questions, feel free to contact Christian Aebeloe at caebel@cs.aau.dk
