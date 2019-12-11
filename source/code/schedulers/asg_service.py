
import configuration
from boto_retry import get_client_with_retries
import schedulers

class ASGService:
    """
    Implements service to start/stop instances that are under ASG control
    Resize is not supported as it should be handled by the Launch Template/Config
    """
    def __init__(self):
        self.service_name = "asg"
        self._session = None
        self._logger = None

    def _init_scheduler(self, args):
        self._session = args.get(schedulers.PARAM_SESSION)
        self._context = args.get(schedulers.PARAM_CONTEXT)
        self._region = args.get(schedulers.PARAM_REGION)
        self._logger = args.get(schedulers.PARAM_LOGGER)
        self._account = args.get(schedulers.PARAM_ACCOUNT)
        self._tagname = args.get(schedulers.PARAM_TAG_NAME)

    def get_schedulable_asg(self, kwargs):
        self._session = kwargs[schedulers.PARAM_SESSION]
        context = kwargs[schedulers.PARAM_CONTEXT]
        region = kwargs[schedulers.PARAM_REGION]
        account = kwargs[schedulers.PARAM_ACCOUNT]
        self._logger = kwargs[schedulers.PARAM_LOGGER]
        tagname = kwargs[schedulers.PARAM_CONFIG].tag_name
        config = kwargs[schedulers.PARAM_CONFIG]

        client = get_client_with_retries(
                                            "autoscaling", ["describe_auto_scaling_groups"],
                                            context=context,
                                            session=self._session,
                                            region=region
                                        )

        asg_jmes = "AutoScalingGroups[*].{AutoScalingGroupName:AutoScalingGroupName, MaxSize:MaxSize " \
               "DesiredCapacity:DesiredCapacity, MinSize:MinSize, Tags:Tags, " \
               "|[?Tags]|[?contains(Tags[*].Key, '{}')]".format(tagname)

        inst_jmes = "Reservations[*].Instances[*].{InstanceId:InstanceId, EbsOptimized:EbsOptimized, Tags:Tags, " \
               "InstanceType:InstanceType,State:State}[]" + \
               "|[?(Tags[?Key=='aws:autoscaling:groupName'])]" + \
               "|[?(Tags[?Key=='aws:autoscaling:groupName'"
               "|[?Tags]|[?contains(Tags[*].Key, '{}')]".format(tagname)

    # def get_schedulable_instances(self, kwargs):
    #     self._session = kwargs[schedulers.PARAM_SESSION]
    #     context = kwargs[schedulers.PARAM_CONTEXT]
    #     region = kwargs[schedulers.PARAM_REGION]
    #     account = kwargs[schedulers.PARAM_ACCOUNT]
    #     self._logger = kwargs[schedulers.PARAM_LOGGER]
    #     tagname = kwargs[schedulers.PARAM_CONFIG].tag_name
    #     config = kwargs[schedulers.PARAM_CONFIG]
    #
    #     self.schedules_with_hibernation = [s.name for s in config.schedules.values() if s.hibernate]
    #
    #     client = get_client_with_retries("ec2", ["describe_instances"], context=context, session=self._session,
    #                                      region=region)
    #
    #     def is_in_schedulable_state(ec2_inst):
    #         state = ec2_inst["state"] & 0xFF
    #         return state in Ec2Service.EC2_SCHEDULABLE_STATES
    #
    #     jmes = "Reservations[*].Instances[*].{InstanceId:InstanceId, EbsOptimized:EbsOptimized, Tags:Tags, " \
    #            "InstanceType:InstanceType,State:State}[]" + \
    #            "|[?!(Tags[?Key=='aws:autoscaling:groupName'])]" + \
    #            "|[?Tags]|[?contains(Tags[*].Key, '{}')]".format(tagname)