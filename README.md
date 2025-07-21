## _(Origional Work)_ Star Pattern Fragments: Accessing Knowledge Graphs through Star Patterns
## _(To be extended)_ Full Pattern Fragments 
A github repository for Star Pattern Fragments server and client in Java (and client in Node.js).

_(To be extended)_ _We are planning to extend this work by implmenting Path and Sink shapped query patterns, and much more._ 

_(original work)_ This Readme and repository are works in progress. The SPF version available here is the one used for experiments in our paper (see also our website at http://relweb.cs.aau.dk/spf/).

_(original work)_ The server is available in the SPF.Server/ folder.

_(original work)_ The client is available for both Java (SPF.Client/) and Node.js (Client.js/).

_(To be extended)_ We will be working in Java SPF.Client 

Any questions, feel free to contact Christian Aebeloe at caebel@cs.aau.dk

---   
## (to be extended work -- steps to follow)
### 🚀 How to Run the Server
> 📌 **Note:** Sample dataset of about 1000 triples is created for test purpose.
1. Navigate to the server directory:

   ```bash
   cd FullPatternFragments/SPF.Server

2. Make sure the following sample files are available in the directory:

   - sampleDataset.hdt

   - sampleDataset.json

3. Start the server using the following command:

   ```bash
   java -jar ./target/ldf-server.jar sampleDataset.json

4. Once the server is running, open your browser and visit:

   http://localhost:8080/sampleDataSet


### How to Run the Client

1. Navigate to the client directory:

   ```bash
   cd FullPatternFragments/SPF.Client

2. Make sure the following files and folders:

   - sample-queries

   - ./target/spf-client.jar

3. Execute one query at a time through:

   ```bash
   java -jar ./target/spf-client.jar -q sample-queries/star1.sparql -f http://localhost:8080/sampleDataset
