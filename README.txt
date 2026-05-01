README.txt
==========

Submission by:
Kadar Daas
240052662
kadar.daas@city.ac.uk

BUILD INSTRUCTIONS
------------------
Navigate to the directory containing the .java files and run:
    javac *.java

RUN INSTRUCTIONS
----------------
To run the local test:
    java LocalTest

To run with more nodes:
    java LocalTest 5

To run the Azure lab test:
    java AzureLabTest your.email@city.ac.uk 10.x.x.x [port]

FUNCTIONALITY COMPLETE
----------------------
- Name messages (G/H): fully implemented
- Nearest messages (N/O): fully implemented
- Key Existence messages (E/F): fully implemented
- Read messages (R/S): fully implemented
- Write messages (W/X): fully implemented
- Compare-and-Swap messages (C/D): fully implemented
- Relay messages (V): fully implemented, non-blocking
- UDP reliability: retransmission after 5 seconds, max 3 retries
- Address key/value pair storage with max 3 per distance bucket
- Data key/value pair storage with correct placement logic
- Passive mapping via Name requests
- Active mapping via Nearest requests

KNOWN LIMITATIONS
-----------------
- Rebalancing of data after discovering closer nodes is not fully implemented
- Packet duplication and reordering handling is basic

