
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;



public class Subscriber
{
  private static final String BROKER_URL   = "tcp://localhost:61616";
  private static final String BROKER_PROPS = "persistent=false&useJmx=false";
  private static final String TOPIC_NAME   = "topic";
  
  private int ID;
  private Boolean resourceOccupied = false;
  
  private BrokerService broker;
  private ActiveMQConnectionFactory cf;
  private TopicSession topicSession;
  private ActiveMQConnection connection;
  private Topic topic;
  private TopicSubscriber subscriber;
  private QueueSession queueSession;
  private MessageProducer producer;
  
  
  public Subscriber(String id)
  {
	  ID = Integer.parseInt(id);
  }

  
  public void run()
  {
	  try {
			  try {
				  	broker = BrokerFactory.createBroker( "broker:(" + BROKER_URL + ")?" + BROKER_PROPS);
				    broker.start();
			  }
			  catch(Exception e) {
				  System.out.println("Broker già creato");
			  }
	
		      cf = new ActiveMQConnectionFactory(Subscriber.BROKER_URL);
		      connection = (ActiveMQConnection) cf.createConnection();
		      connection.start();
		      topicSession = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
		      topic = topicSession.createTopic(TOPIC_NAME);
		      subscriber = topicSession.createSubscriber(topic);
		      
		      //queue
		      queueSession = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		      producer = queueSession.createProducer(null);
		      
		      while(true)
		      {
		    	receive(); 
		      }
		  }
	  catch(Exception e) {
		  e.printStackTrace();
	  }  
  }
  public void receive()
  {
    try
    {
        Message message = subscriber.receive();

        if (message instanceof TextMessage)
        {
          if(message.getIntProperty("type") == 2)
		  {
        	  //ho ricevuto un messaggio che mi dice che devo occupare la risorsa
        	  int id = message.getIntProperty("server_id");
        	  if(id == ID)
        	  {
        		  resourceOccupied = true;
            	  System.out.println("La mia risorsa è occupata adesso");
        	  }       	    
		  }
          
          if(message.getIntProperty("type") == 3)
          {
        	  //ho ricevuto un messaggio di liberare la risorsa
        	  int id = message.getIntProperty("server_id");
        	  if(id == ID)
        	  {
        		  resourceOccupied = false;	
            	  System.out.println("La mia risorsa è libera adesso");
        	  }     		  
          }
          else {
        	   System.out.println("Message: " + ((TextMessage) message).getText());
               TextMessage reply = queueSession.createTextMessage();//riposta p2p
               String text;  
               
               if(!resourceOccupied)
             	  text = "SI";
               else 
             	  text = "NO";
               
               reply.setText(text);
               reply.setIntProperty("server_id", ID);
               
               //Thread.sleep(2000*ID);//debug?
               System.out.println("sto mandando la mia risposta" + text);
               producer.send(message.getJMSReplyTo(), reply);  	  
               System.out.println("mandato");
          }  
        }  
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }


  public static void main(final String[] args)
  {
	  new Subscriber(args[0]).run();
  }
}
