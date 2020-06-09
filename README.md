# Distributed-Ssytem-with-JMS
Development of distributed application that supports replication through the use of the “quorum based protocol”.
The application involve some clients submitting read and write requests and some coordinators answering with votes to the clients.
Clients can read or write on the replicas when the number of the vote messages, received from the coordinators, is equal or greater than the corresponding read or write quorum.
The execution of the read and write operations is simulated by simply waiting for a random time before submitting the release message.
Clients that do not reach the necessary quorum need to wait for a random time before resubmitting the request.

For execution on Eclipse:
1. Create different configurations of Subscriber class
2. Every subscriber configuration must have as parameter the ID (ex. 1,2,3,...)
3. Do the same with the publisher class ( at the moment only one configuration is required)
