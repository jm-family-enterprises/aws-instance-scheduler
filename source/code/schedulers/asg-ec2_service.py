######################################################################################################################
#  Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           #
#                                                                                                                    #
#  Licensed under the Apache License Version 2.0 (the "License"). You may not use this file except in compliance     #
#  with the License. A copy of the License is located at                                                             #
#                                                                                                                    #
#      http://www.apache.org/licenses/                                                                               #
#                                                                                                                    #
#  or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES #
#  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    #
#  and limitations under the License.                                                                                #
######################################################################################################################
import os
from datetime import timedelta

import dateutil
import jmespath
from botocore.exceptions import ClientError

import configuration
import schedulers
import time
from boto_retry import get_client_with_retries
from configuration.instance_schedule import InstanceSchedule
from .ec2_service import Ec2Service

# instances are started in batches, larger bathes are more efficient but smaller batches allow more instances
# to start if we run into resource limits

# ERR_RESIZING_INSTANCE_ = "Error resizing instance {}, ({})"
#
# START_BATCH_SIZE = 5
# STOP_BATCH_SIZE = 50
#
# ERR_STARTING_INSTANCES = "Error starting instances {}, ({})"
# ERR_STOPPING_INSTANCES = "Error stopping instances {}, ({})"
# ERR_MAINT_WINDOW_NOT_FOUND_OR_DISABLED = "SSM maintenance window {} used in schedule {} not found or disabled"
#
# INF_FETCHED_INSTANCES = "Number of fetched ec2 instances is {}, number of instances in a schedulable state is {}"
# INF_FETCHING_INSTANCES = "Fetching ec2 instances for account {} in region {}"
# INF_SETTING_SIZE = "Setting size for ec2 instance {} to {}"
# INF_ADD_KEYS = "Adding {} key(s) {} to instance(s) {}"
# INFO_REMOVING_KEYS = "Removing {} key(s) {} from instance(s) {}"
# INF_MAINT_WINDOW = "Created schedule {} from SSM maintence window, start is {}, end is {}"
# INF_MAINT_WINDOW_DISABLED = "SSM maintenance window {} ({}) is disabled"
#
# WARN_STARTED_INSTANCES_TAGGING = "Error deleting or creating tags for started instances {} ({})"
# WARN_STOPPED_INSTANCES_TAGGING = "Error deleting or creating tags for stopped instances {} ({})"
# WARNING_INSTANCE_NOT_STARTING = "Ec2 instance {} is not started"
# WARNING_INSTANCE_NOT_STOPPING = "Ec2 instance {} is not stopped"
# WARN_NOT_HIBERNATED = "Instance {} could not be hibernated, retry to stop without hibernation, {}"
# WARN_NO_HIBERNATE_RESIZED = "Instance {} is not hibernated because it is stopped for resizing the instance"
#
# DEBUG_SKIPPED_INSTANCE = "Skipping ec2 instance {} because it it not in a schedulable state ({})"
# DEBUG_SELECTED_INSTANCE = "Selected ec2 instance {} in state ({})"


class ASGEc2Service(Ec2Service):
    """
    Implements service start/stop/resize functions for EC2 service
    """

    def __init__(self):
        super(ASGEc2Service, self).__init__()
        self.service_name = "asg-ec2"

    def get_tags(self,instance_id):
        c = get_client_with_retries("ec2", ["describe_instances"], context=self._context,
                                    session=self._session,
                                    region=self._region)
        the_instance = c.describe_instances(InstanceIds=[instance_id])
        tags = the_instance.get('Reservations')[0].get('Instances')[0].get('Tags')
        tag_dict = {}
        for tag in tags:
            tag_dict[tag['Key']] = tag['Value']

        return tag_dict

    def is_asg_in_desired_state(self, autoscale_group_name, instance_id, desired_state):
        c = get_client_with_retries("autoscaling", ["describe_auto_scaling_groups"], context=self._context,
                                    session=self._session,
                                    region=self._region)
        response = c.describe_auto_scaling_groups(AutoScalingGroupNames=[autoscale_group_name])
        instance_objects = response.get("AutoScalingGroups")[0].get("Instances")
        self._logger.debug("Instance Object is {}".format(instance_objects))
        for io in instance_objects:
            self._logger.debug("io is {}".format(io))
            self._logger.debug("instance in io is {}".format(io.get("InstanceId")))
            if io.get("InstanceId") == instance_id:
                self._logger.debug("instance in state {}".format(io.get("LifecycleState")))
                return desired_state == io.get("LifecycleState")
        return False

    # noinspection PyMethodMayBeStatic
    def stop_instances(self, kwargs):
        def is_in_stopping_state(state):
            return (state & 0xFF) in Ec2Service.EC2_STOPPING_STATES

        self._init_scheduler(kwargs)

        stopped_instances = kwargs[schedulers.PARAM_STOPPED_INSTANCES]
        stop_tags = kwargs[schedulers.PARAM_CONFIG].stopped_tags
        if stop_tags is None:
            stop_tags = []
        stop_tags_key_names = [t["Key"] for t in stop_tags]

        start_tags_keys = [{"Key": t["Key"]} for t in kwargs[schedulers.PARAM_CONFIG].started_tags if
                           t["Key"] not in stop_tags_key_names]

        methods = ["stop_instances", "create_tags", "delete_tags", "describe_instances"]
        client = get_client_with_retries("ec2", methods=methods, context=self._context, session=self._session,
                                         region=self._region)
        autoscale_client = get_client_with_retries("autoscaling", ["exit_standby", "describe_auto_scaling_groups", "update_auto_scaling_group"],
                                                   context=self._context, session=self._session,
                                                   region=self._region)

        for instance_batch in list(self.instance_batches(stopped_instances, STOP_BATCH_SIZE)):
            instance_ids = [i.id for i in instance_batch]

            for instance in instance_ids:
                tags = self.get_tags(instance)
                if 'aws:autoscaling:groupName' in tags.keys():
                    autoscale_group_name = tags['aws:autoscaling:groupName']
                    # TODO: Save off the state of the autoscale group min size to the asg-state table.
                    # TODO: Modify the Autoscale Min to 0 to allow all ASG instances to be shutdown.
                    autoscale_client.update_auto_scaling_group(AutoScalingGroupName=autoscale_group_name, MinSize=0)
                    if not (self.is_asg_in_desired_state(autoscale_group_name, instance, "Standby")):
                        self._logger.debug(
                            'Instance is an autoscale member with groupName = {}'.format(
                                autoscale_group_name))
                        self._logger.debug(
                            'Instance in Autoscale Group Entering Standby for Scheduled Shutdown')
                        autoscale_client.enter_standby(
                            InstanceIds=[instance],
                            AutoScalingGroupName=autoscale_group_name,
                            ShouldDecrementDesiredCapacity=True
                        )

                        stop_resp = autoscale_client.stop_instances_with_retries(InstanceIds=hibernated, Hibernate=True)
                        instances_stopping += [i["InstanceId"] for i in stop_resp.get("StoppingInstances", []) if
                                               is_in_stopping_state(i.get("CurrentState", {}).get("Code", ""))]
                        break
                    except ClientError as ex:
                        instance_id = None
                        if ex.response.get("Error", {}).get("Code") == "UnsupportedHibernationConfiguration":
                            instance_id = ex.response["Error"]["Message"].split(":")[-1].strip()
                        elif ex.response.get("Error", {}).get("Code") == "UnsupportedOperation":
                            instance_id = ex.response["Error"]["Message"].split(" ")[1].strip()
                        if instance_id in hibernated:
                            self._logger.warning(WARN_NOT_HIBERNATED, instance_id, ex)
                            hibernated.remove(instance_id)
                            not_hibernated.append(instance_id)
                        else:
                            self._logger.error(ERR_STOPPING_INSTANCES, ",".join(hibernated), str(ex))
                # TODO: Add EnterStandby and ExitStandby to the rights of the role for the scheduler.
                if len(not_hibernated) > 0:
                    try:
                        for instance in not_hibernated:
                            tags = get_tags(instance)
                            if 'aws:autoscaling:groupName' in tags.keys():
                                autoscale_group_name = tags['aws:autoscaling:groupName']
                                if not (is_asg_in_desired_state(autoscale_group_name, instance, "Standby")):
                                    self._logger.debug(
                                        'Instance is an autoscale member with groupName = {}'.format(
                                            autoscale_group_name))
                                    self._logger.debug(
                                        'Instance in Autoscale Group Entering Standby for Scheduled Shutdown')
                                    autoscale_client.enter_standby(
                                        InstanceIds=[instance],
                                        AutoScalingGroupName=autoscale_group_name,
                                        ShouldDecrementDesiredCapacity=True
                                    )

                        stop_resp = client.stop_instances_with_retries(InstanceIds=not_hibernated, Hibernate=False)
                        instances_stopping += [i["InstanceId"] for i in stop_resp.get("StoppingInstances", []) if
                                               is_in_stopping_state(i.get("CurrentState", {}).get("Code", ""))]
                    except Exception as ex:
                        self._logger.error(ERR_STOPPING_INSTANCES, ",".join(not_hibernated), str(ex))

                get_status_count = 0
                if len(instances_stopping) < len(instance_ids):
                    time.sleep(5)

                    instances_stopping = [i["InstanceId"] for i in self.get_instance_status(client, instance_ids) if
                                          is_in_stopping_state(i.get("State", {}).get("Code", ""))]

                    if len(instances_stopping) == len(instance_ids):
                        break

                    get_status_count += 1
                    if get_status_count > 3:
                        for i in instance_ids:
                            if i not in instances_stopping:
                                self._logger.warning(WARNING_INSTANCE_NOT_STOPPING, i)
                        break

                if len(instances_stopping) > 0:
                    try:
                        if start_tags_keys is not None and len(start_tags_keys):
                            self._logger.info(INFO_REMOVING_KEYS, "start",
                                              ",".join(["\"{}\"".format(k["Key"]) for k in start_tags_keys]),
                                              ",".join(instances_stopping))
                            client.delete_tags_with_retries(Resources=instances_stopping, Tags=start_tags_keys)
                        if len(stop_tags) > 0:
                            self._logger.info(INF_ADD_KEYS, "stop", str(stop_tags), ",".join(instances_stopping))
                            client.create_tags_with_retries(Resources=instances_stopping, Tags=stop_tags)
                    except Exception as ex:
                        self._logger.warning(WARN_STOPPED_INSTANCES_TAGGING, ','.join(instances_stopping), str(ex))

                for i in instances_stopping:
                    yield i, InstanceSchedule.STATE_STOPPED

            except Exception as ex:
                self._logger.error(ERR_STOPPING_INSTANCES, ",".join(instance_ids), str(ex))

    # noinspection PyMethodMayBeStatic
    def start_instances(self, kwargs):

        def get_tags(instance_id):
            c = get_client_with_retries("ec2", ["describe_instances"], context=self._context,
                                        session=self._session,
                                        region=self._region)
            the_instance = c.describe_instances(InstanceIds=[instance_id])
            tags = the_instance.get('Reservations')[0].get('Instances')[0].get('Tags')
            tag_dict = {}
            for tag in tags:
                tag_dict[tag['Key']] = tag['Value']

            return tag_dict

        def is_asg_in_desired_state(autoscale_group_name, instance_id, desired_state):
            c = get_client_with_retries("autoscaling", ["describe_auto_scaling_groups"], context=self._context,
                                        session=self._session,
                                        region=self._region)
            response = c.describe_auto_scaling_groups(AutoScalingGroupNames=[autoscale_group_name])
            instance_objects = response.get("AutoScalingGroups")[0].get("Instances")
            self._logger.debug("Instance Object is {}".format(instance_objects))
            for io in instance_objects:
                self._logger.debug("io is {}".format(io))
                self._logger.debug("instance in io is {}".format(io.get("InstanceId")))
                if io.get("InstanceId") == instance_id:
                    self._logger.debug("instance in state {}".format(io.get("LifecycleState")))
                    return desired_state == io.get("LifecycleState")
            return False

        def is_in_starting_state(state):
            return (state & 0xFF) in Ec2Service.EC2_STARTING_STATES

        self._init_scheduler(kwargs)

        instances_to_start = kwargs[schedulers.PARAM_STARTED_INSTANCES]
        start_tags = kwargs[schedulers.PARAM_CONFIG].started_tags
        if start_tags is None:
            start_tags = []
        start_tags_key_names = [t["Key"] for t in start_tags]
        stop_tags_keys = [{"Key": t["Key"]} for t in kwargs[schedulers.PARAM_CONFIG].stopped_tags if
                          t["Key"] not in start_tags_key_names]
        client = get_client_with_retries("ec2", ["start_instances", "describe_instances", "create_tags", "delete_tags"],
                                         context=self._context, session=self._session, region=self._region)
        autoscale_client = get_client_with_retries("autoscaling", ["exit_standby"],
                                                   context=self._context, session=self._session,
                                                   region=self._region)
        for instance_batch in self.instance_batches(instances_to_start, START_BATCH_SIZE):

            instance_ids = [i.id for i in list(instance_batch)]
            try:
                self._logger.debug("Instance IDS {}".format(instance_ids))
                start_resp = client.start_instances_with_retries(InstanceIds=instance_ids)
                instances_starting = [i["InstanceId"] for i in start_resp.get("StartingInstances", []) if
                                      is_in_starting_state(i.get("CurrentState", {}).get("Code", ""))]

                for instance in instance_ids:
                    tags = get_tags(instance)
                    if 'aws:autoscaling:groupName' in tags.keys():
                        autoscale_group_name = tags['aws:autoscaling:groupName']
                        if not (is_asg_in_desired_state(autoscale_group_name, instance, "InService")):
                            self._logger.debug(
                                'Instance is an autoscale member with groupName = {}'.format(autoscale_group_name))
                            self._logger.debug('Instance in Autoscale Group Entering Standby for Scheduled Shutdown')
                            autoscale_client.exit_standby(
                                InstanceIds=[instance],
                                AutoScalingGroupName=autoscale_group_name
                            )
                get_status_count = 0
                if len(instances_starting) < len(instance_ids):
                    time.sleep(5)

                    instances_starting = [i["InstanceId"] for i in self.get_instance_status(client, instance_ids) if
                                          is_in_starting_state(i.get("State", {}).get("Code", ""))]

                    if len(instances_starting) == len(instance_ids):
                        break

                    get_status_count += 1
                    if get_status_count > 3:
                        for i in instance_ids:
                            if i not in instances_starting:
                                self._logger.warning(WARNING_INSTANCE_NOT_STARTING, i)
                        break

                if len(instances_starting) > 0:
                    try:
                        if stop_tags_keys is not None and len(stop_tags_keys) > 0:
                            self._logger.info(INFO_REMOVING_KEYS, "stop",
                                              ",".join(["\"{}\"".format(k["Key"]) for k in stop_tags_keys]),
                                              ",".join(instances_starting))
                            client.delete_tags_with_retries(Resources=instances_starting, Tags=stop_tags_keys)
                        if len(start_tags) > 0:
                            self._logger.info(INF_ADD_KEYS, "start", str(start_tags), ",".join(instances_starting))
                            client.create_tags_with_retries(Resources=instances_starting, Tags=start_tags)
                    except Exception as ex:
                        self._logger.warning(WARN_STARTED_INSTANCES_TAGGING, ','.join(instances_starting), str(ex))

                for i in instances_starting:
                    yield i, InstanceSchedule.STATE_RUNNING

            except Exception as ex:
                self._logger.error(ERR_STARTING_INSTANCES, ",".join(instance_ids), str(ex))
