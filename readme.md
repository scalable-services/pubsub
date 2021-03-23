**HOW TO RUN**

1. Tools needed:
- Docker
- JDK 1.8 or superior (ideally JDK 15)
- SBT build tool (1.4.5) (Scala)
- Download and publish to local repository the project index: https://github.com/scalable-services/index
    * cd into index
    * run $ sbt publishLocal

- Google Cloud account with billing enabled 
- Google APIS activated 
- Google Cloud Pub/Sub 
- Google Cloud Storage
- Google service account with permissions (put json credentials in a file at the root of this
  project with name google_cloud_credentials.json)
  
2. For the first time to create topics on Google Pub/Sub, execute: 
    
    $ sbt "testOnly services.scalable.pubsub.PubSubSpec"
        
3.  Open two separate command prompts: 
    
    First (server): $ sbt "runMain services.scalable.pubsub.Main"
    Second (client): $ sbt "runMain services.scalable.pubsub.BrokerClient"
    
4. Download some MQTT tool like MQTTBox create a client and start send messages to topic test or demo! 
   You will see at the second prompt 20 clients receiving messages sent from the tool.  