from scrimp import ProvisionerConfig
from scrimp import SimpleStringifiable
import datetime


class SimRequest(SimpleStringifiable):
    """
    A class to manage AWS instance types.
    """

    def __init__(self, price, subnet, ins_type, reqid, sleep_time, job):
        self.type = ins_type
        self.price = price
        self.subnet = subnet
        self.sleep_time = sleep_time
        self.job_runner_id = job
        self.reqid = reqid
        self.request_time = ProvisionerConfig().simulate_time
        self.ready_time = self.request_time + \
            datetime.timedelta(seconds=int(sleep_time))
