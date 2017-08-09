from scrimp import SimpleStringifiable
from scrimp import ProvisionerConfig
import datetime


class SimResource(SimpleStringifiable):
    """
    A class to manage AWS instance types.
    """

    def __init__(self, price, subnet, ins_type, req_time,
                 reqid, insid, context_time, jobid):
        self.type = ins_type
        self.price = price
        self.subnet = subnet
        self.reqid = reqid
        self.id = insid
        self.launch_time = ProvisionerConfig().simulate_time
        self.job_runner_id = jobid
        self.terminate_time = None
        self.request_time = req_time
        self.state = 'CONTEXTUALIZING'
        self.context_time = self.launch_time + \
            datetime.timedelta(seconds=int(context_time))
        self.job_finish = None
        self.busy_to = None
        self.job_id = None
        self.reason = ""
