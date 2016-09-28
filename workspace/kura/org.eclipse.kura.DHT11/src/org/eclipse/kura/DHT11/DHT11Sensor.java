package org.eclipse.kura.DHT11;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.eclipse.kura.cloud.CloudClient;
import org.eclipse.kura.cloud.CloudClientListener;
import org.eclipse.kura.cloud.CloudService;
import org.eclipse.kura.configuration.ConfigurableComponent;
import org.eclipse.kura.message.KuraPayload;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.ComponentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.ptr.FloatByReference;

public class DHT11Sensor implements ConfigurableComponent,CloudClientListener {
	
	private CloudService                m_cloudService;
	private CloudClient      			m_cloudClient;
	private ScheduledExecutorService    m_worker;
	private Map<String, Object>         m_properties;
	private ScheduledFuture<?>          m_handle;
	

	
	
	private static final Logger s_logger = LoggerFactory.getLogger(DHT11Sensor.class);
    private static final String APP_ID = "DHT11Sensor";
    
    private Map<String, Object> properties;
	
	public DHT11Sensor(){
		super();
		m_worker = Executors.newSingleThreadScheduledExecutor();
	}
    public interface CLib extends Library {
        public int readDHT(int pin,FloatByReference tempByRef,FloatByReference humidByRef);
    }
    
	public void setCloudService(CloudService cloudService) {
		m_cloudService = cloudService;
	}

	public void unsetCloudService(CloudService cloudService) {
		m_cloudService = null;
	}
    
    protected void activate(ComponentContext componentContext,Map<String,Object> properties) {
    	 	
    	 m_properties = properties;
 		for (String s : properties.keySet()) {
 			s_logger.info("Activate - "+s+": "+properties.get(s));
 		}
 		
 		// get the mqtt client for this application
 		try  {
 			
 			// Acquire a Cloud Application Client for this Application 
 			s_logger.info("Getting CloudClient for {}...", APP_ID);
 			m_cloudClient = m_cloudService.newCloudClient(APP_ID);
 			m_cloudClient.addCloudClientListener(this);
 			
 			// Don't subscribe because these are handled by the default 
 			// subscriptions and we don't want to get messages twice			
 			doUpdate(false);
 		}
 		catch (Exception e) {
 			s_logger.error("Error during component activation", e);
 			throw new ComponentException(e);
 		}
    	  
        s_logger.info("Bundle " + APP_ID + " has started!");       
  
    }

    protected void deactivate(ComponentContext componentContext) {
    	m_worker.shutdown();
		
		// Releasing the CloudApplicationClient
		s_logger.info("Releasing CloudApplicationClient for {}...", APP_ID);
		m_cloudClient.release();

		s_logger.debug("Deactivating DHT11... Done.");
        s_logger.info("Bundle " + APP_ID + " has stopped!");

    }
    
    public void updated(Map<String, Object> properties) {
		s_logger.info("Updated DHT11...");

		// store the properties received
		m_properties = properties;
		for (String s : properties.keySet()) {
			s_logger.info("Update - "+s+": "+properties.get(s));
		}
		
		// try to kick off a new job
		doUpdate(true);
		s_logger.info("Updated DHT11... Done.");
    }

	private void doUpdate(boolean onUpdate) 
	{
		// cancel a current worker handle if one if active
		if (m_handle != null) {
			m_handle.cancel(true);
		}
		
		// schedule a new worker based on the properties of the service
		int pubrate = 2;
		m_handle = m_worker.scheduleAtFixedRate(new Runnable() {		
			@Override
			public void run() {
				Thread.currentThread().setName(getClass().getSimpleName());
				doPublish();
			}
		}, 0, pubrate, TimeUnit.SECONDS);
	}

	private void doPublish() 
	{				
		// fetch the publishing configuration from the publishing properties
		String  topic  = "DHT11Data";
		Integer qos    = 0;
		Boolean retain = false;
	
		CLib ctest = (CLib) Native.loadLibrary("dht11sensor", CLib.class);
   	 	final FloatByReference temp_valRef = new FloatByReference();
   	 	final FloatByReference humid_valRef = new FloatByReference();
   	    ctest.readDHT(7,temp_valRef, humid_valRef);
		// Increment the simulated temperature value
		float setPoint = 0;
					
		// Allocate a new payload
		KuraPayload payload = new KuraPayload();
		
		// Timestamp the message
		payload.setTimestamp(new Date());
		
		// Add the temperature as a metric to the payload
	
		//s_logger.info("Data"+temp_valRef.getValue()+humid_valRef.getValue());
		for(int i=0;i<100;i++){
			if(temp_valRef.getValue()>5)
				break;
			ctest.readDHT(7,temp_valRef, humid_valRef);
			
		}
		payload.addMetric("Temperature",Float.toString(temp_valRef.getValue()));
		payload.addMetric("Humidity", Float.toString(humid_valRef.getValue()));

	
		payload.addMetric("errorCode", 0);
	
		// Publish the message
		try {
			m_cloudClient.publish(topic, payload, qos, retain);
			s_logger.info("Published to {} message: {}", topic, payload);
		} 
		catch (Exception e) {
			s_logger.error("Cannot publish topic: "+topic, e);
		}
	}
	@Override
	public void onConnectionEstablished() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onConnectionLost() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onControlMessageArrived(String arg0, String arg1, KuraPayload arg2, int arg3, boolean arg4) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onMessageArrived(String arg0, String arg1, KuraPayload arg2, int arg3, boolean arg4) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onMessageConfirmed(int arg0, String arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onMessagePublished(int arg0, String arg1) {
		// TODO Auto-generated method stub
		
	}
}

