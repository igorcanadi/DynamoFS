/*
 * Copyright 2010-2012 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package advanced;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.BlockDeviceMapping;
import com.amazonaws.services.ec2.model.CancelSpotInstanceRequestsRequest;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsRequest;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsResult;
import com.amazonaws.services.ec2.model.EbsBlockDevice;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.LaunchSpecification;
import com.amazonaws.services.ec2.model.RequestSpotInstancesRequest;
import com.amazonaws.services.ec2.model.RequestSpotInstancesResult;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.SpotInstanceRequest;
import com.amazonaws.services.ec2.model.SpotPlacement;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;

public class Requests {
    private AmazonEC2      ec2;
    private ArrayList<String> instanceIds;
    private ArrayList<String> spotInstanceRequestIds;
    private String instanceType;
    private String amiID;
    private String bidPrice;
    private String securityGroup;
    private String placementGroupName;
    private boolean deleteOnTermination;
	private String availabilityZoneName;
	private String availabilityZoneGroupName;
	private String launchGroupName;
	private Date validFrom;
	private Date validTo;
	private String requestType;
    
    /**
     * Public constructor.
     * @throws Exception 
     */
    public Requests (String instanceType, String amiID, String bidPrice, String securityGroup) throws Exception {
        init(instanceType, amiID, bidPrice,securityGroup);
    }
    
    /**
     * The only information needed to create a client are security credentials
     * consisting of the AWS Access Key ID and Secret Access Key. All other
     * configuration, such as the service endpoints, are performed
     * automatically. Client parameters, such as proxies, can be specified in an
     * optional ClientConfiguration object when constructing a client.
     *
     * @see com.amazonaws.auth.BasicAWSCredentials
     * @see com.amazonaws.auth.PropertiesCredentials
     * @see com.amazonaws.ClientConfiguration
     */
    private void init(String instanceType, String amiID, String bidPrice, String securityGroup) throws Exception {
        AWSCredentials credentials = new PropertiesCredentials(
                InlineTaggingCodeSampleApp.class.getResourceAsStream("AwsCredentials.properties"));

        ec2 = new AmazonEC2Client(credentials);
        this.instanceType = instanceType;
        this.amiID = amiID;
        this.bidPrice = bidPrice;
        this.securityGroup = securityGroup;
        this.deleteOnTermination = true;
        this.placementGroupName = null;
    }

    /**
     * The submit method will create 1 x one-time t1.micro request with a maximum bid
     * price of $0.03 using the Amazon Linux AMI. 
     * 
     * Note the AMI id may change after the release of this code sample, and it is important 
     * to use the latest. You can find the latest version by logging into the AWS Management 
     * console, and attempting to perform a launch. You will be presented with AMI options, 
     * one of which will be Amazon Linux. Simply use that AMI id.
     */
    public void submitRequests() {
    	//==========================================================================//
    	//================= Submit a Spot Instance Request =====================//
    	//==========================================================================//

    	// Initializes a Spot Instance Request
    	RequestSpotInstancesRequest requestRequest = new RequestSpotInstancesRequest();
   
    	// Request 1 x t1.micro instance with a bid price of $0.03. 
    	requestRequest.setSpotPrice(bidPrice);
    	requestRequest.setInstanceCount(Integer.valueOf(1));
    	
    	// Setup the specifications of the launch. This includes the instance type (e.g. t1.micro)
    	// and the latest Amazon Linux AMI id available. Note, you should always use the latest 
    	// Amazon Linux AMI id or another of your choosing.
    	LaunchSpecification launchSpecification = new LaunchSpecification();
    	launchSpecification.setImageId(amiID);
    	launchSpecification.setInstanceType(instanceType);

    	// Add the security group to the request.
    	ArrayList<String> securityGroups = new ArrayList<String>();
    	securityGroups.add(securityGroup);
    	launchSpecification.setSecurityGroups(securityGroups); 

    	// If a placement group has been set, then we will use it in the request.
    	if (placementGroupName != null && !placementGroupName.equals("")) {
        	// Setup the placement group to use with whatever name you desire.
        	SpotPlacement placement = new SpotPlacement();
        	placement.setGroupName(placementGroupName);
        	launchSpecification.setPlacement(placement);
    	}

    	// Check to see if we need to set the availability zone name.
    	if (availabilityZoneName != null && !availabilityZoneName.equals("")) {
    		// Setup the availability zone to use. Note we could retrieve the availability 
        	// zones using the ec2.describeAvailabilityZones() API. 
        	SpotPlacement placement = new SpotPlacement(availabilityZoneName);
        	launchSpecification.setPlacement(placement);
    	}
    	
    	if (availabilityZoneGroupName != null && !availabilityZoneGroupName.equals("")) {
    		// Set the availability zone group.
        	requestRequest.setAvailabilityZoneGroup(availabilityZoneGroupName);
    	}
    	
    	// Check to see if we need to set the launch group.
    	if (launchGroupName != null && !launchGroupName.equals("")) {
    		// Set the availability launch group.
        	requestRequest.setLaunchGroup(launchGroupName);
    	}
    	
    	// Check to see if we need to set the valid from option.
    	if (validFrom != null) {
   			requestRequest.setValidFrom(validFrom);
    	}
    	
    	// Check to see if we need to set the valid until option.
    	if (validTo != null) {
    		requestRequest.setValidUntil(validFrom);    		
    	}
    	
    	// Check to see if we need to set the request type.
    	if (requestType != null && !requestType.equals("")) {
        	// Set the type of the bid.
        	requestRequest.setType(requestType);
    	}

    	
    	// If we should delete the EBS boot partition on termination.
    	if (!deleteOnTermination) {
        	// Create the block device mapping to describe the root partition.
        	BlockDeviceMapping blockDeviceMapping = new BlockDeviceMapping();
        	blockDeviceMapping.setDeviceName("/dev/sda1");

        	// Set the delete on termination flag to false.
        	EbsBlockDevice ebs = new EbsBlockDevice();
        	ebs.setDeleteOnTermination(Boolean.FALSE);
        	blockDeviceMapping.setEbs(ebs);
        	
        	// Add the block device mapping to the block list.
        	ArrayList<BlockDeviceMapping> blockList = new ArrayList<BlockDeviceMapping>();
        	blockList.add(blockDeviceMapping);
        	
        	// Set the block device mapping configuration in the launch specifications.
        	launchSpecification.setBlockDeviceMappings(blockList);
    	}
    	
    	// Add the launch specifications to the request.
    	requestRequest.setLaunchSpecification(launchSpecification);
    	
    	// Call the RequestSpotInstance API. 
    	RequestSpotInstancesResult requestResult = ec2.requestSpotInstances(requestRequest);        	
    	List<SpotInstanceRequest> requestResponses = requestResult.getSpotInstanceRequests();
    	
    	// Setup an arraylist to collect all of the request ids we want to watch hit the running
    	// state.
    	spotInstanceRequestIds = new ArrayList<String>();
    	
    	// Add all of the request ids to the hashset, so we can determine when they hit the 
    	// active state.
    	for (SpotInstanceRequest requestResponse : requestResponses) {
    		System.out.println("Created Spot Request: "+requestResponse.getSpotInstanceRequestId());
    		spotInstanceRequestIds.add(requestResponse.getSpotInstanceRequestId());
    	}

    }
    
    public void launchOnDemand () {
    	//============================================================================================//
    	//====================================== Launch an On-Demand Instance ========================// 
    	//====================================== If we Didn't Get a Spot Instance ====================// 
    	//============================================================================================//

	    	// Setup the request for 1 x t1.micro using the same security group and 
	    	// AMI id as the Spot request.
	    	RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
	    	runInstancesRequest.setInstanceType(instanceType);
	    	runInstancesRequest.setImageId(amiID);
	    	runInstancesRequest.setMinCount(Integer.valueOf(1));
	    	runInstancesRequest.setMaxCount(Integer.valueOf(1));
	    	
	    	// Add the security group to the request.
	    	ArrayList<String> securityGroups = new ArrayList<String>();
	    	securityGroups.add(securityGroup);
	    	runInstancesRequest.setSecurityGroups(securityGroups);
	    	
	    	// Launch the instance.
	    	RunInstancesResult runResult = ec2.runInstances(runInstancesRequest);
	    	
	    	// Add the instance id into the instance id list, so we can potentially later
	    	// terminate that list.
	    	for (Instance instance: runResult.getReservation().getInstances()) {
	    		System.out.println("Launched On-Demand Instace: "+instance.getInstanceId());
	    		instanceIds.add(instance.getInstanceId());
	    	}
    }

    /**
     * The areOpen method will determine if any of the requests that were started are still
     * in the open state. If all of them have transitioned to either active, cancelled, or
     * closed, then this will return false. 
     * @return
     */
    public boolean areAnyOpen() {
    	//==========================================================================//
    	//============== Describe Spot Instance Requests to determine =============//
    	//==========================================================================//

    	// Create the describeRequest with tall of the request id to monitor (e.g. that we started).
    	DescribeSpotInstanceRequestsRequest describeRequest = new DescribeSpotInstanceRequestsRequest();    	
    	describeRequest.setSpotInstanceRequestIds(spotInstanceRequestIds);

		System.out.println("Checking to determine if Spot Bids have reached the active state...");

		// Initialize variables.
		instanceIds = new ArrayList<String>();
		
		try
		{
        	// Retrieve all of the requests we want to monitor. 
			DescribeSpotInstanceRequestsResult describeResult = ec2.describeSpotInstanceRequests(describeRequest);
			List<SpotInstanceRequest> describeResponses = describeResult.getSpotInstanceRequests();

        	// Look through each request and determine if they are all in the active state.
        	for (SpotInstanceRequest describeResponse : describeResponses) {
        		System.out.println(" " +describeResponse.getSpotInstanceRequestId() + 
        						   " is in the "+describeResponse.getState() + " state.");
        		
        		// If the state is open, it hasn't changed since we attempted to request it.
        		// There is the potential for it to transition almost immediately to closed or
        		// cancelled so we compare against open instead of active.
        		if (describeResponse.getState().equals("open")) {
        			return true;
        		}
        		
        		// Add the instance id to the list we will eventually terminate.
        		instanceIds.add(describeResponse.getInstanceId());
        	}
		} catch (AmazonServiceException e) {
			// Print out the error.
			System.out.println("Error when calling describeSpotInstances");
            System.out.println("Caught Exception: " + e.getMessage());
            System.out.println("Reponse Status Code: " + e.getStatusCode());
            System.out.println("Error Code: " + e.getErrorCode());
            System.out.println("Request ID: " + e.getRequestId());

            // If we have an exception, ensure we don't break out of the loop.
			// This prevents the scenario where there was blip on the wire.
			return true;
        }
		
		return false; 	
    }

    /**
     * Tag any of the resources we specify.   
     * @param resources
     * @param tags
     */
    private void tagResources(List<String> resources, List<Tag> tags) {
    	// Create a tag request.
    	CreateTagsRequest createTagsRequest = new CreateTagsRequest();
    	createTagsRequest.setResources(resources);
    	createTagsRequest.setTags(tags);
    		
    	// Try to tag the Spot request submitted.
    	try {
    		ec2.createTags(createTagsRequest);
    	} catch (AmazonServiceException e) {
        	// Write out any exceptions that may have occurred.
            System.out.println("Error terminating instances");
        	System.out.println("Caught Exception: " + e.getMessage());
            System.out.println("Reponse Status Code: " + e.getStatusCode());
            System.out.println("Error Code: " + e.getErrorCode());
            System.out.println("Request ID: " + e.getRequestId());			
    	}

    }
    
    /**
     * Tags all of the instances started with this object with the tags specified.
     * @param tags
     */
    public void tagInstances(List<Tag> tags) {
    	tagResources(instanceIds, tags);
    }
    
    /**
     * Tags all of the requests started with this object with the tags specified.
     * @param tags
     */
    public void tagRequests(List<Tag> tags) {
    	tagResources(spotInstanceRequestIds, tags);
    }

    /**
     * The cleanup method will cancel and active requests and terminate any running instances
     * that were created using this object. 
     */
    public void cleanup () {
    	//==========================================================================//
    	//================= Cancel/Terminate Your Spot Request =====================//
    	//==========================================================================//
    	try {
        	// Cancel requests.
        	System.out.println("Cancelling requests.");
        	CancelSpotInstanceRequestsRequest cancelRequest = new CancelSpotInstanceRequestsRequest(spotInstanceRequestIds);
        	ec2.cancelSpotInstanceRequests(cancelRequest);
    	} catch (AmazonServiceException e) {
    		// Write out any exceptions that may have occurred.
            System.out.println("Error cancelling instances");
		    System.out.println("Caught Exception: " + e.getMessage());
            System.out.println("Reponse Status Code: " + e.getStatusCode());
            System.out.println("Error Code: " + e.getErrorCode());
            System.out.println("Request ID: " + e.getRequestId());
        }
    	
    	try {
        	// Terminate instances.
        	System.out.println("Terminate instances");
        	TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest(instanceIds);
        	ec2.terminateInstances(terminateRequest);
    	} catch (AmazonServiceException e) {
    		// Write out any exceptions that may have occurred.
            System.out.println("Error terminating instances");
    		System.out.println("Caught Exception: " + e.getMessage());
            System.out.println("Reponse Status Code: " + e.getStatusCode());
            System.out.println("Error Code: " + e.getErrorCode());
            System.out.println("Request ID: " + e.getRequestId());
        }
    	
    	// Delete all requests and instances that we have terminated.
    	instanceIds.clear();
    	spotInstanceRequestIds.clear();
    }
    
    /**
     * Sets the request type to either persistent or one-time. 
     */
    public void setRequestType(String type) {
    	this.requestType = type;
    }
    
    /**
     * Sets the valid to and from time. If you set either value to null
     * or "" then the period will not be set.
     * @param from
     * @param to
     */
    public void setValidPeriod(Date from, Date to) {
    	this.validFrom = from;
    	this.validTo = to;
    }
    
    /**
     * Sets the launch group to be used. If you set this to null
     * or "" then launch group will be used.
     * @param az
     */
    public void setLaunchGroup(String launchGroup) {
    	this.launchGroupName = launchGroup;
    }
    
    /**
     * Sets the availability zone group to be used. If you set this to null
     * or "" then availability zone group will be used.
     * @param az
     */
    public void setAvailabilityZoneGroup(String azGroup) {
    	this.availabilityZoneGroupName = azGroup;
    }
    
    /**
     * Sets the availability zone to be used. If you set this to null
     * or "" then availability zone will be used.
     * @param az
     */
    public void setAvailabilityZone(String az) {
    	this.availabilityZoneName = az;
    }
    
    /**
     * Sets the placementGroupName to be used. If you set this to null
     * or "" then no placementgroup will be used.
     * @param pg
     */
    public void setPlacementGroup(String pg) {
    	this.placementGroupName = pg; 
    }
    
    /**
     * This sets the deleteOnTermination flag, so that we can determine whether or not
     * we should delete the root partition if the instance is interrupted or terminated.
     * @param terminate
     */
    public void setDeleteOnTermination(boolean terminate) {
    	this.deleteOnTermination = terminate;
    }
}

