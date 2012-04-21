/*
 * Copyright 2012 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.simpleworkflow.flow.examples.booking;

import com.amazonaws.services.simpleworkflow.flow.annotations.Activities;
import com.amazonaws.services.simpleworkflow.flow.annotations.ActivityRegistrationOptions;
import com.amazonaws.services.simpleworkflow.flow.common.FlowConstants;

@Activities(version = "1.0")
@ActivityRegistrationOptions(
		defaultTaskHeartbeatTimeoutSeconds = FlowConstants.NONE, 
		defaultTaskScheduleToCloseTimeoutSeconds = 300,
        defaultTaskScheduleToStartTimeoutSeconds = 60, 
        defaultTaskStartToCloseTimeoutSeconds = 60)
public interface BookingActivities {

    void reserveCar(int requestId);

    void reserveAirline(int requestId);

    void sendConfirmationActivity(int customerId);
    
}
