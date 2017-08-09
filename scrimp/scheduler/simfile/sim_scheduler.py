import datetime
import json

from scrimp import logger, ProvisionerConfig
from scrimp.scheduler.base_scheduler import BaseScheduler
from scrimp.scheduler import Job


class SimScheduler(BaseScheduler):

    def __init__(self):
        self.jobs = []
        self.job_data = None

    def get_global_queue(self):
        """
        Read in the jobs that should have started prior to the
        current sim time.
        Create a new job object for each then return a list of them.
        """

        if self.job_data is None:
            with open(ProvisionerConfig().jobs_file) as data_file:
                logger.debug("SIMULATION: READING DATA")
                self.job_data = json.load(data_file)

        # NOTE: this now doesn't work for multiple tenants as this
        # is self.jobs. change it back
        # to read the full file over and over if i want tenants.

        # Work out how many seconds have passed since starting the test
        rel_time = (ProvisionerConfig().simulate_time -
                    ProvisionerConfig().sim_time).total_seconds()
        to_delete = []
        for j in self.job_data:

            if int(j['relative_time']) < rel_time:
                to_delete.append(j)
                description = {}
                description['instype'] = j['instance_type']
                description['duration'] = float(j['duration'])
                req_time = ProvisionerConfig().sim_time + \
                    datetime.timedelta(seconds=int(j['relative_time']))
                newjob = Job('tenant_addr', "%s%s" % (j['id'],
                             ProvisionerConfig().run_id), 1, req_time,
                             1, 1, 1, description)

                self.jobs.append(newjob)
            else:
                break
        for j in to_delete:
            self.job_data.remove(j)

        return self.jobs

    def process_job_description(self, desc):
        """
        Convert the job description in to a dict that will be
        passed to the job.
        """
        return ""

    def get_condor_status(self, pool):
        """
        Poll the collector of a pool to get the condor_status,
        describing the
        resources in the pool.
        """
        return ""

    def process_global_queue(self, jobs, tenants):
        """
        Associate each job with a tenant and add them to their local list of
        jobs.
        """
        for tenant in tenants:
            tenant.jobs = []
            tenant.idle_jobs = []
            # Get the necessary time a job must be idle as a timestamp for
            # each tenant

            # Go through the jobs and only add those that are old enough and
            # are in the idle state
            for job in jobs:
                tenant.jobs.append(job)

                job_idle_at = job.req_time + \
                    datetime.timedelta(seconds=tenant.idle_time)
                if (int(job.status) == 1 and
                    job_idle_at < ProvisionerConfig().simulate_time):
                    tenant.idle_jobs.append(job)
            logger.debug("SIMULATION: job len = %s" % len(tenant.jobs))
