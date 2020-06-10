
import javax.jms.Message;

import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.util.Random;


public class Publisher 
{
  private static final String BROKER_URL   = "tcp://localhost:61616";
  private static final String TOPIC_NAME   = "topic";

  private String ID;

  
  //quorum
  private static final int Vr = 1;
  private static final int Vw = 3;
  private static final int V  = 4;
  
  //tipo di richiesta
  private int requestType; // se 1 read se 0 write
  private static final int REQUEST = 0;
  private static final int USE = 1;
  private static final int RELEASE = 2;
  
  //code dei messaggi
  private ActiveMQConnection connection;
  private QueueSession queueSession;
  private TemporaryQueue tempQ;
  private QueueReceiver receiver;
  
  //topic
  private TopicSession topicSession;
  private TopicPublisher publisher;
  private Topic topic;
  
 
  
  public Publisher(String id)
  {
	  ID = id;
  }

  public void run()
  {
	  try {
		  
			  ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(Publisher.BROKER_URL);
		      connection = (ActiveMQConnection) cf.createConnection();
		      connection.start();
	
		      //configurazione per il TOPIC
		      topicSession = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
		      topic = topicSession.createTopic(TOPIC_NAME);
		      publisher = topicSession.createPublisher(topic);
		      
		      //configurazione per la QUEUE
		      queueSession = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		      tempQ = queueSession.createTemporaryQueue();
		      receiver = queueSession.createReceiver(tempQ);
	
		      while(true)
			  {
				  //decide se vuole fare read o write
				  Random rand = new Random();
				  int choice = rand.nextInt(50);
				  
				  if(choice % 2 == 0)
					requestType = 1;
				  else
					requestType = 0;
				  
				 publish(requestType,REQUEST);
				 
				 //contatore per il quorum
				 int count_si = 0;
				 int quorum;
				 
				 //attende la risposta dei server
				 for(int i = 0; i < V; i++)
				 {
					 Message r = receive();
					 String m = ((TextMessage) r).getText();
					 if(m.equals("SI")) 
						 count_si++;
				 }		
				 
				 //controllo quorum
				 String log = "";
				 int computation_time;
				 
				 if (requestType == 0)
				 {
					 quorum = Vw;
					 log = "sto scrivendo";
					 computation_time = 6000;
				 }
				 else
				 {
					 quorum = Vr;
					 log = "sto leggendo";
					 computation_time = 3000;
				 }
				 
				 if (count_si > quorum) // quorum raggiunto (R o W)
				 {
					 publish(requestType,USE); //primo server disponibile metti la risorsa occupata
					 
					 System.out.println(log);
					 
					 //uso la risorsa
					 Thread.sleep(computation_time);
					 
					 //gli dico di liberare la risorsa
					 publish(requestType,RELEASE);
				 }
				 
				 else // quorum non raggiunto
				 {
					 System.out.println("Quorum non raggiunto");
					 Thread.sleep(1000 + rand.nextInt(2000));	// sleep prima di una nuova richiesta
				 }

			  }
	  }
	  catch(Exception e) {
		  e.printStackTrace();
	  }
  }
  
  public void publish(int requestType,int messageType)
  { 
    try
    {
      TextMessage message = topicSession.createTextMessage();
      String text = " ";
      message.setStringProperty("clientID", ID);
      message.setIntProperty("type", messageType);
      
      if(messageType == REQUEST)
      {
    	  if(requestType == 0)
        	  text = "voglio scrivere";
          else
        	  text = "voglio leggere";
      }
      
      message.setText(text);
      message.setJMSReplyTo(tempQ);  
      publisher.publish(message);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  public Message receive()
  {
	 Message response;
	  
	 try {
		 
		  response = receiver.receive();
		  
		  String r = ((TextMessage) response).getText();
		  
		  System.out.println("Messaggio: " + r);
		  
		  return response;
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
	 
	response = null;
	return response;
  }
  
  public static void main(final String[] args)
  {
	  new Publisher(args[0]).run();
  }

}
