
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
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
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

import java.util.Random;


public class Publisher implements MessageListener
{
  private static final String BROKER_URL   = "tcp://localhost:61616";
  private static final String TOPIC_NAME   = "topic";

  private String ID;
  
  //quorum
  private static final int Vr = 1;
  private static final int Vw = 3;
  private static final int V = 4;
  
  //tipo di richiesta
  private int MessageType; // se 1 read se 0 write
  
  //code dei messaggi
  private ActiveMQConnection connection;
  private static final String BROKER_PROPS = "persistent=false&useJmx=false";
  private  String QUEUE_NAME = "client";
  private QueueSession queueSession;
  private TemporaryQueue tempQ;
  private MessageConsumer consumer;
  
  private QueueReceiver receiver;
  
  //topic
  private TopicSession topicSession;
  private TopicPublisher publisher;
  private Topic topic;
  
  
  public Publisher(String id)
  {
	  ID = id;
	  QUEUE_NAME += id;
  }

  
  @Override
	public void onMessage(final Message m)
	{
	  if (m instanceof TextMessage)
	  {
	    try
	    {
	      System.out.println("Message: " + ((TextMessage) m).getText());
	    }
	    catch (JMSException e)
	    {
	      e.printStackTrace();
	    }
	  }
	  else if (connection != null)
	  {
	    try
	    {
	      connection.close();
	    }
	    catch (JMSException e)
	    {
	      e.printStackTrace();
	    }
	  }
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
	      consumer = queueSession.createConsumer(tempQ);
	      consumer.setMessageListener(this);
	      receiver = queueSession.createReceiver(tempQ);

	      while(true)
		  {
			  //decide se vuole fare read o write
			  Random rand = new Random();
			  int choice = rand.nextInt(50);
			  
			  if(choice % 2 == 0) {
				//read
				MessageType = 1;
			  }
			  else {
				//write
				MessageType = 0;
			  }
			  // pubblica sul topic il suo messaggio
			 publish();
			 
			 //contatori per il quorum
			 int count_si = 0;
			 
			 //attende la risposta del server
			 for(int i=0; i<V; i++)
			 {
				 String r = receive();
				 if(r.equals("SI")) {
					 count_si++;
				 }
			 }
			 
			 
			 //controllo quorum
			 if(MessageType == 0)
			 {
				//controllo quorum per la write 
				 if(count_si >= Vw)
				 {
					 //eseguo la write simulando un tempo casuale
					 int computation_time = rand.nextInt(2000);
					 Thread.sleep(computation_time);
					 
					 //manda messaggio di release
				 }
				 else
				 {
					 System.out.println("Quorum per la write non raggiunto");
					//FACCIO COMUNQUE UNA SLEEP prima di una nuova richiesta
					 int computation_time = rand.nextInt(2000);
					 Thread.sleep(computation_time);
					 
				 }
			 }
			 else
			 {
				 //controllo qorum per la read
				 if(count_si >= Vr)
				 {
					 //eseguo la read simulando un tempo casuale
					 int computation_time = rand.nextInt(2000);
					 Thread.sleep(computation_time);
					 
					 //manda messaggio di release
					 
				 }
				 else
				 {
					 System.out.println("Quorum per la read non raggiunto");
					 //FACCIO COMUNQUE UNA SLEEP prima di una nuova richiesta
					 int computation_time = rand.nextInt(2000);
					 Thread.sleep(computation_time);
				 }
			 }
		  }
	  }
	  catch(Exception e) {
		  e.printStackTrace();
	  }
  }
  
  public void publish()
  { 
    try
    {
      TextMessage message = topicSession.createTextMessage();
      message.setText("Vediamo");
      message.setJMSReplyTo(tempQ);
      
      publisher.publish(message);
      publisher.publish(topicSession.createMessage());
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  public String receive()
  {
	 try {
		  Message response = receiver.receive();
		  String r = ((TextMessage) response).getText();
		  System.out.println("Message: " + r);
		  
		  return r;
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
	 
	return "Agg sbagliat";
  }
  
  
  
  public static void main(final String[] args)
  {
	  new Publisher(args[0]).run();
  }

	
}
