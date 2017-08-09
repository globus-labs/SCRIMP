import sys
from scrimp import SimpleStringifiable
from scrimp import logger, ProvisionerConfig
import boto
from sim_request import SimRequest
from sim_resource import SimResource
import datetime
import threading
import random
import numpy as np
from scipy import stats

from scrimp.scheduler.simfile.sim_scheduler import SimScheduler


class AWSSimulator(SimpleStringifiable):
    """
    A class to simulate AWS resources etc.
    """

    def __init__(self):
        self.kill_time = 1400000
        self.already_terminated = []
        self.requests = []

        self.resources = []

        self.finished_jobs = []
        self.executing_jobs = []
        self.tenants = None
        self.reqid = 1
        self.insid = 1

        self.sched = SimScheduler()

        self.make_distributions()

        # read the resource fulfillment times into a list
        self.fulfill_time = [0]

        self.lock = threading.Lock()
        self.turn = 0

    def make_distributions(self):
        cmd = ("select extract(epoch from(exec_start_time - join_time)) from"
               " launch_stats where exec_start_time is not null and "
               "extract(epoch from(exec_start_time - join_time)) < 300;")
        data = ProvisionerConfig().dbconn.execute(cmd)
        neg_list = []
        for r in data:
            neg_list.append(r['date_part'])
        mu, sigma = stats.norm.fit(np.log(neg_list))
        shape, loc, scale = stats.lognorm.fit(neg_list, floc=0)
        dist = stats.lognorm(shape, loc, scale)
        self.negotiate_time_dist = list(dist.rvs(size=10000))

        mu, sigma = 7.118134, 0.895632  # mean and standard deviation
        self.fulfilled_time_dist = list(np.random.normal(mu, sigma, 10000))

        cmd = ("select extract(epoch from(join_time - fulfilled_time)) from "
               "launch_stats where exec_start_time is not null and "
               "extract(epoch from(join_time - fulfilled_time)) < 300;")
        data = ProvisionerConfig().dbconn.execute(cmd)
        context_list = []
        for r in data:
            context_list.append(r['date_part'])
        mu, sigma = stats.norm.fit(np.log(context_list))
        shape, loc, scale = stats.lognorm.fit(context_list, floc=0)
        dist = stats.lognorm(shape, loc, scale)
        self.contextualise_time_dist = list(dist.rvs(size=10000))

    def check_finished(self):
        """
        Make sure there are no instances or jobs remaining.
        """
        len = 0
        for t in self.tenants:
            for job in t.jobs:
                len = len + 1
                if job.sim_status != 'FINISHED':
                    return False
        for res in self.resources:
            len = len + 1
            if res.state != 'TERMINATED':
                return False
        if len == 0:
            return False
        return True

    def run_condor(self, tenants):
        """
        Be the condor agent. This will manage putting jobs on
        the resources etc.
        """
        logger.debug("SIMULATION CONDOR: starting.")
        instance_types = ProvisionerConfig().instance_types

        current_time = ProvisionerConfig().simulate_time

        # logger.debug("SIMULATION CONDOR: loaded tenants.")
        # now i need to add status to each of the jobs
        # Run through the jobs and set their states so they
        # are ignored by other things
        for t in tenants:
            for job in list(t.jobs):
                if job.id in self.finished_jobs:
                    job.sim_status = "FINISHED"
                    if job in t.idle_jobs:
                        t.idle_jobs.remove(job)
                    t.jobs.remove(job)

                elif job.id in self.executing_jobs:
                    job.sim_status = "EXECUTING"
                    if job in t.idle_jobs:
                        t.idle_jobs.remove(job)
            for t in tenants:
                for job in t.jobs:
                    for resource in self.resources:
                        if (job.id == resource.job_id and
                            resource.job_finish is not None and
                                resource.job_finish < current_time):
                            # Mark it as all done
                            job.sim_status = "FINISHED"
                            resource.state = "IDLE"
                            if job.id in self.executing_jobs:
                                self.executing_jobs.remove(job.id)
                            if job.id not in self.finished_jobs:

                                self.finished_jobs = self.finished_jobs + \
                                    [job.id]
                                logger.debug(
                                    "SIMULATION CONDOR: Finished " +
                                    "job %s." % (job.id))
                                # resource.state = "IDLE"
                                ProvisionerConfig().dbconn.execute(
                                    ("update jobs set end_time = '%s' " +
                                     "where job_id = %s and test = '%s';") % (
                                        ProvisionerConfig().simulate_time,
                                        int(job.id),
                                        ProvisionerConfig().run_name))

            logger.debug("SIMULATION CONDOR: deploying new jobs.")
            for t in tenants:
                for job in t.jobs:
                    # check if it can fit on the instance
                    if job.sim_status == "IDLE":
                        self.deploy_job(job)

    def check_claim(self, resource):
        """
        Chekc if there is any idle job that should cause this
        to become unclaimed.
        """
        for t in self.tenants:
            for job in t.jobs:
                if job.sim_status == 'IDLE':
                    # check it meets the jobs reuirements:
                    if self.check_requirements(resource.type, job):
                        return True
        return False

    def run_aws(self):
        """
        This is the aws loop. Check if instances should be fulfilled etc.
        """
        logger.debug("SIMULATION AWS: starting.")

        current_time = ProvisionerConfig().simulate_time
        logger.debug("SIMULATION AWS: running.")

        with self.lock:
            self.turn = 0
            # check if any requests should be fulfilled
            for request in list(self.requests):
                if current_time >= request.ready_time:
                    # start a resource for this
                    insid = "%s-sim-ins-%s" % (
                        ProvisionerConfig().run_name, self.insid)
                    self.insid = self.insid + 1

                    logger.debug("SIMULATION AWS: creating a new resource " +
                                 "for request %s, has slept %s" % (
                                     request, request.sleep_time))
                    new_resource = SimResource(request.price, request.subnet,
                                               request.type,
                                               request.request_time,
                                               request.reqid, insid,
                                               random.choice(
                                                   self.contextualise_time_dist),
                                               request.job_runner_id)
                    self.resources = self.resources + [new_resource]
                    # and remove the request since it is done
                    self.instance_acquired(new_resource)
                    self.requests.remove(request)

            # Now check to see if any instances should have booted by now.
            # This handles working out when the instance joins the HTCondor
            # queue and when jobs get dispatched.
            for resource in self.resources:
                # First check if the resource is in the contextualzing state.
                if (resource.state == 'CONTEXTUALIZING' and
                        current_time >= resource.context_time):
                    # Switch it over to Starting with a neg time
                    resource.state = 'UNCLAIMED'
                    wait_time = int(random.choice(self.negotiate_time_dist))
                    resource.claimed_time = current_time + \
                        datetime.timedelta(seconds=wait_time)

                elif resource.state == 'UNCLAIMED':
                    # check if the timer has passed:
                    logger.debug('SIMULATION: trying to become ' +
                                 'unclaimed %s seconds remaining' % (
                                     resource.claimed_time - current_time).total_seconds())
                    if (resource.claimed_time -
                            current_time).total_seconds() <= 0:
                        # check if any jobs are in an idle state
                        new_claim = self.check_claim(resource)
                        if new_claim:
                            resource.state = 'IDLE'
                            logger.debug('Set resource to idle')
                            continue
                        else:
                            # otherwise, set it back to 'starting so it
                            # becomes unclaimed again'
                            resource.claimed_time = current_time + \
                                datetime.timedelta(seconds=int(
                                    random.choice(self.negotiate_time_dist)))
                            resource.state = 'UNCLAIMED'
                            logger.debug(
                                'SIMULATION no idle job found, setting ' +
                                'back to UNCLAIMED')

            # check if any instances should terminate due to time
            terminate_resources = {}
            for resource in self.resources:
                if ProvisionerConfig().terminate == "hourly":
                    if (resource.state != 'EXECUTING' and
                        (int((current_time -
                              resource.launch_time).total_seconds() % 3600) >
                         3480) and resource.state != 'TERMINATED'):
                        # now checking this when killing anything
                        # over 3480 secs...
                        logger.debug("SIMULATION AWS. Terminating resource "
                                     "due to time: %s" % resource)
                        # terminate the job
                        resource.reason = "time related"
                        resource.state = "TERMINATED"
                        resource.terminate_time = self.get_fake_time()
                elif ProvisionerConfig().terminate == "1hour":
                    # now checking this when killing anything over 3480 secs...
                    if (resource.state != 'EXECUTING' and
                        (current_time - resource.launch_time).total_seconds()
                            > 3480 and resource.state != 'TERMINATED'):
                        logger.debug("SIMULATION AWS. Terminating resource "
                                     "due to time: %s" % resource)
                        # terminate the job
                        resource.reason = "time related"
                        resource.state = "TERMINATED"
                        resource.terminate_time = self.get_fake_time()
                elif ProvisionerConfig().terminate == "idle":
                    if (resource.state == 'IDLE' and
                        resource.state != 'TERMINATED' and
                        (int((current_time -
                              resource.launch_time).total_seconds()) > 600)):
                        # now checking this when killing anything over
                        # 3480 secs...
                        logger.debug(
                            "SIMULATION AWS. Terminating resource due "
                            "to time: %s" % resource)
                        # terminate the job
                        resource.reason = "time related"
                        resource.state = "TERMINATED"
                        resource.terminate_time = self.get_fake_time()

                # Sort out the job that was running on this instance,
                # put it back to idle.
                if resource.state != "TERMINATED":
                    # only do this every 10 seconds
                    if (ProvisionerConfig().simulate_time -
                            ProvisionerConfig().sim_time).total_seconds() % 60 == 0:
                        for t in self.tenants:
                            if resource.type in terminate_resources:
                                if terminate_resources[resource.type] is False:
                                    continue
                            if (float(resource.price) <
                                    float(self.get_spot_prices(resource, t))):
                                logger.debug(
                                    "SIMULATION: terminating resource "
                                    "due to price %s" % resource)

                                for job in t.jobs:
                                    if job.id == resource.job_id:

                                        job.sim_status = 'IDLE'
                                        if job.id in self.executing_jobs:
                                            self.executing_jobs.remove(job.id)
                                        if job not in t.idle_jobs:
                                            t.idle_jobs = t.idle_jobs + [job]
                                # now terminate the instance
                                logger.debug('terminating instance due '
                                             'to price: %s %s' % (
                                                 resource.price,
                                                 self.get_spot_prices(resource, t)))
                                resource.state = "TERMINATED"
                                resource.reason = ("spot instance termination "
                                                   "due to spot price")
                                resource.terminate_time = self.get_fake_time()
                                # self.resources.remove(resource)
                            else:
                                terminate_resources[resource.type] = False

    def simulate(self, _tenants):
        """
        check the state of the simulation.
        """
        self.tenants = _tenants
        jobs_list = []
        idle_jobs = []

        for t in self.tenants:
            for job in t.jobs:
                jobs_list = jobs_list + [job.id]
                if (job.sim_status == "IDLE" and
                    job.id not in self.finished_jobs and
                        job.id not in self.executing_jobs):
                    idle_jobs = idle_jobs + [job.id]

        # get some counts to print out
        terminated_time_instances = []
        terminated_price_instances = []
        starting_instances = []
        unclaimed_instances = []
        idle_instances = []
        executing_instances = []

        for res in self.resources:
            if res.state == "IDLE":
                idle_instances = idle_instances + [res.id]
            elif res.state == "EXECUTING":
                executing_instances = executing_instances + [res.id]
            elif res.state == "STARTING" or res.state == "CONTEXTUALIZING":
                starting_instances = starting_instances + [res.id]
            elif res.state == "UNCLAIMED":
                unclaimed_instances = unclaimed_instances + [res.id]
            elif res.state == "TERMINATED":
                if 'time' in res.reason:
                    terminated_time_instances = terminated_time_instances + \
                        [res.id]
                if 'price' in res.reason:
                    terminated_price_instances = terminated_price_instances + \
                        [res.id]

        logger.debug("\nSIMULATION OVERVIEW: requests (cur: %s -- total: %s), "
                     "resources (%s), jobs (%s)\n" %
                     (len(self.requests), self.reqid - 1, len(self.resources),
                      len(jobs_list)))

        logger.debug("\nSIMULATION JOB OVERVIEW: idle (%s), executing (%s), "
                     "finished (%s)\n" % (
                         len(idle_jobs), len(self.executing_jobs),
                         len(self.finished_jobs)))

        logger.debug("\nSIMULATION RESOURCE OVERVIEW: starting (%s), "
                     "idle (%s), unclaimed (%s), executing (%s), "
                     "terminated-time (%s), terminated-price (%s)\n" % (
                         len(starting_instances), len(idle_instances),
                         len(unclaimed_instances), len(executing_instances),
                         len(terminated_time_instances),
                         len(terminated_price_instances)))
        total_run_seconds = (ProvisionerConfig().simulate_time -
                             ProvisionerConfig().sim_time).total_seconds()
        logger.debug("\nSIMULATION TIME OVERVIEW: start time (%s), "
                     "current time (%s), seconds simulated (%s)" % (
                         ProvisionerConfig().sim_time,
                         ProvisionerConfig().simulate_time,
                         total_run_seconds))
        if total_run_seconds > self.kill_time:
            sys.exit()
        # Run through the jobs and set their states so they are
        # ignored by other things
        for t in self.tenants:
            for job in t.jobs:
                if job.id in self.finished_jobs:
                    job.sim_status = "FINISHED"
                    if job in t.idle_jobs:
                        t.idle_jobs.remove(job)
                    if job.id in self.executing_jobs:
                        self.executing_jobs.remove(job.id)

                    t.jobs.remove(job)
                elif job.id in self.executing_jobs:
                    job.sim_status = "EXECUTING"
                    if job in t.idle_jobs:
                        t.idle_jobs.remove(job)

        # try cleaning up the instance state too
        for res in self.resources:
            if (res.state == "EXECUTING" and
                    res.job_id not in self.executing_jobs):
                # somehow this one should have finished...
                # try to just wrap it up now
                res.job_id = None
                res.job_finish = None
                res.state = "IDLE"
                logger.debug("SIMULATION: Found an executing resource " +
                             "that should be idle.")

    def get_fake_time(self, time=None):
        """
        Get the time difference between the passed in time and
        the relative_time,
        then add that difference to the simulate time
        """
        offset = datetime.timedelta(seconds=time)
        if time is None:
            return ProvisionerConfig().simulate_time
        else:
            return ProvisionerConfig().simulate_time + offset

    def deploy_job(self, job):
        current_time = ProvisionerConfig().simulate_time
        instance_types = ProvisionerConfig().instance_types
        for resource in self.resources:
            if resource.state == "IDLE":
                for instance in instance_types:
                    # check that it fits this instance
                    if (resource.type == instance.type and
                            self.check_requirements(instance, job)):

                        # this is now good, so lets put it on there.
                        resource.job_id = job.id
                        # set the time for the job to finish
                        # first convert the exec time to the instance
                        exec_seconds = self.exec_time(job, resource.type)

                        logger.debug("SIMULATION CONDOR: Deploying " +
                                     "job %s to resource %s for %s" % (
                                         job.id, resource.id, exec_seconds))
                        # convert the jobs request time into a timestamp

                        req_time = job.req_time
                        ProvisionerConfig().dbconn.execute(
                            ("insert into jobs (test, job_id, start_time, "
                             "req_time) values ('%s', %s, '%s', '%s');" % (
                                 ProvisionerConfig().run_name,  int(job.id),
                                 self.get_fake_time(), req_time)))

                        resource.job_finish = current_time + \
                            datetime.timedelta(seconds=exec_seconds)
                        resource.state = "EXECUTING"
                        job.sim_status = "EXECUTING"
                        self.executing_jobs = self.executing_jobs + [job.id]
                        return

    def exec_time(self, job, res_type):
        """
        convert the exec time to a exec time of this instance type
        """
        if job.instype == "r3.8xlarge":
            if res_type == "c3.2xlarge":
                return job.duration * 2.022
            if res_type == "c3.4xlarge":
                return job.duration * 1.167
            if res_type == "c3.8xlarge":
                return job.duration * 0.944
            if res_type == "g2.2xlarge":
                return job.duration * 2.226
            if res_type == "g2.8xlarge":
                return job.duration * 1.039
            if res_type == "m3.2xlarge":
                return job.duration * 2.064
            if res_type == "r3.2xlarge":
                return job.duration * 2.106
            if res_type == "r3.4xlarge":
                return job.duration * 1.257
            if res_type == "r3.8xlarge":
                return job.duration * 1
        else:
            if res_type == "c3.2xlarge":
                return job.duration * 0.980
            if res_type == "c3.4xlarge":
                return job.duration * 0.565
            if res_type == "c3.8xlarge":
                return job.duration * 0.457
            if res_type == "g2.2xlarge":
                return job.duration * 1.078
            if res_type == "g2.8xlarge":
                return job.duration * 0.503
            if res_type == "m3.2xlarge":
                return job.duration * 1
            if res_type == "r3.2xlarge":
                return job.duration * 1.02
            if res_type == "r3.4xlarge":
                return job.duration * 0.609
            if res_type == "r3.8xlarge":
                return job.duration * 0.484

    def instance_acquired(self, resource):
        launch_time = ProvisionerConfig().simulator.get_fake_time()

        data = ProvisionerConfig().dbconn.execute(
            'select id from instance_request where ' +
            'request_id = \'' + resource.reqid + '\';')
        reqid = 0
        for r in data:
            reqid = r['id']
        # insert it into the database
        ProvisionerConfig().dbconn.execute(
            ("insert into instance (request_id, instance_id, " +
             "fulfilled_time, " +
             "public_dns, private_dns) values " +
             "('%s', '%s', '%s', '%s', '%s')") %
            (reqid, resource.id, resource.launch_time,
             'pubdns', 'privdns'))

    def request_spot_instances(self, price, image_id, subnet_id,
                               count, key_name,
                               security_group_ids, instance_type, user_data,
                               block_device_map, job):
        # this needs to make a request, not an instance. then somehow i
        # need to translate requests to instances after a little while.
        simid = "%s-sim-req-%s" % (ProvisionerConfig().run_name, self.reqid)

        sleep_time = float(random.choice(self.fulfilled_time_dist))
        self.reqid = self.reqid + 1
        new_request = SimRequest(
            price, subnet_id, instance_type, simid, int(sleep_time), job.id)
        self.requests.append(new_request)
        # moved this sleep to a different spot so now the requests are
        # done as a batch too
        # time.sleep(ProvisionerConfig().overhead_time)
        logger.debug("SIMULATION: creating new request %s - sleep for %s" %
                     (new_request, sleep_time))

        return [simid]

    def cancel_spot_instance_requests(self, to_kill):
        """
        remove requests in this list.
        """
        for kill in to_kill:
            logger.debug("SIMULATION: Killing requests %s" % kill)
            self.requests = [r for r in self.requests if r.reqid != kill]

    def get_all_instances(self):
        """
        return the set of reservations from aws
        """
        res = []
        for ins in self.resources:
            res = res + [ins]
        return res

    def get_spot_instances(self):
        """
        return the set of spot instances that are fulfilled from aws
        """
        res = []
        for ins in self.resources:
            res = res + ["'%s'" % ins.reqid]
        return res

    def get_open_requests(self):
        """
        return the set of spot instances that are fulfilled from aws
        """
        res = []
        for ins in self.requests:
            res = res + [ins.reqid]
        return res

    def get_spot_prices(self, resource, tenant):
        """
        Get the current spot price for each instance type.
        """

        new_time = ProvisionerConfig().simulate_time
        now = new_time.strftime('%Y-%m-%d %H:%M:%S')
        start_time = (ProvisionerConfig().simulate_time -
                      datetime.timedelta(seconds=60))
        start_time_z = start_time.strftime('%Y-%m-%d %H:%M:%S')

        conn = boto.connect_ec2(tenant.access_key, tenant.secret_key)
        jobCost = 0
        timeStr = str(now).replace(" ", "T") + "Z"
        startTimeStr = str(start_time_z).replace(" ", "T") + "Z"
        prices = conn.get_spot_price_history(
            instance_type=resource.type,
            product_description="Linux/UNIX (Amazon VPC)",
            end_time=timeStr, start_time=startTimeStr)
        lowest_price = 1000000
        for price in prices:
            for key, val in tenant.subnets.iteritems():
                if (price.availability_zone == key and
                    val == resource.subnet and
                        float(price.price) < lowest_price):
                    lowest_price = float(price.price)
        return lowest_price

    def check_requirements(self, instance, job):
        """
        Check to see if an instance can fulfil a jobs requirements.
        I came across a problem here where I wanted to use instance_requests
        from the database, and I didn't have an instance type. So it now
        accepts both an Instance or str (aws instance type name
        e.g. m2.4xlarge)
        """
        if isinstance(instance, basestring):
            for ins in ProvisionerConfig().instance_types:
                if ins.type == instance:
                    instance = ins
                    break

        # Check it meets cpu requirements
        if int(instance.cpus) < int(job.req_cpus):
            return False
        # Check it meets memory requirements
        if int(instance.memory) < int(job.req_mem):
            return False

        # It seems to meet the requirements
        return True
