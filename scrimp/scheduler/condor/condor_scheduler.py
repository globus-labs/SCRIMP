import subprocess
import calendar
import datetime
from pytz import timezone

from scrimp import logger, ProvisionerConfig
from scrimp.scheduler.base_scheduler import BaseScheduler
from scrimp.scheduler import Job


class CondorScheduler(BaseScheduler):

    def get_global_queue(self):
        """
        Poll condor_q -global and return a set of Jobs.
        """
        cmd = ['condor_q', '-global',
               '-format', '%s:', 'GlobalJobId',
               '-format', '%s:', 'ClusterId',
               '-format', '%s:', 'JobStatus',
               '-format', '%s:', 'QDate',
               '-format', '%s:', 'RequestCpus',
               '-format', '%s:', 'RequestMemory',
               '-format', '%s:', 'RequestDisk',
               '-format', '%s', 'JobDescription',
               '-format', '%s\n', 'ExitStatus']

        #output = subprocess.check_output(cmd)
        output = subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()[0]
        queue = output.split("\n")
        queue = filter(None, queue)

        jobs = []
        if len(queue) > 0:
            # set the time of the first job if this is it
            if ProvisionerConfig().first_job_time is None:
                logger.debug("Simulation: first job time set")
                utc = timezone('UTC')
                ProvisionerConfig().first_job_time = datetime.datetime.now(utc)
            for line in queue:
                if "All queues are empty" in line:
                    break
                try:
                    split = line.split(":")
                    tenant_addr = ""
                    # Grab the address of the tenant from the global id
                    if "#" in split[0]:
                        tenant_addr = split[0].split("#")[0]
                    # Req memory is either a number or a string talking about
                    # requested memory, so check if it is a number
                    req_memory = 0
                    try:
                        req_memory = int(split[5])
                        if req_memory > 1024:
                            # change it to use GB like instance types.
                            req_memory = req_memory / 1024
                    except Exception, e:
                        pass
                    # Req disk is the same as memory. Again it is
                    # in mb I believe
                    req_disk = 0
                    try:
                        req_disk = int(split[6])
                        if req_disk > 1024:
                            # change it to use GB like instance types.
                            req_disk = req_disk / 1024
                    except Exception, e:
                        pass
                    # Decipher the description of the job as well (name, etc.)
                    description = {}
                    if "=" in split[7]:
                        description = self.process_job_description(split[7])
                    # Create the job: tenant address, job id, queue time,
                    # requested cpus, requested memory
                    j = Job(tenant_addr, split[1], split[2], split[3],
                            split[4], req_memory, req_disk, description)
                    jobs.append(j)
                except Exception, e:
                    logger.exception("Something has gone wrong while"
                                     " processing "
                                     "the job queue.")
                    raise e

        return jobs

    def process_job_description(self, desc):
        """
        Convert the job description in to a dict that will be
        passed to the job.
        """
        # Split the values in the string by comma and the
        # key/value pair by equals.
        desc = desc.strip('"')
        description = dict(item.split("=") for item in desc.split(","))
        # Now convert and true's to a bool True
        for key, value in description.iteritems():
            value = value.strip()
            if "true" == value.lower():
                description[key] = True
        return description

    def get_condor_status(self, pool):
        """
        Poll the collector of a pool to get the condor_status, describing the
        resources in the pool.
        """
        return ""

    def process_global_queue(self, jobs, tenants):
        """
        Associate each job with a tenant and add them to their local list of
        jobs.
        """
        for tenant in tenants:
            # Get the necessary time a job must be idle as a timestamp for
            # each tenant
            idle_time = (datetime.datetime.now() -
                         datetime.timedelta(seconds=tenant.idle_time))
            idle_time = calendar.timegm(idle_time.timetuple())

            # Go through the jobs and only add those that are old enough and
            # are in the idle state
            for job in jobs:
                if job.tenant_address == tenant.condor_address:
                    tenant.jobs.append(job)

                    # Check if the job is a candidate for resource provisioning
                    if int(job.status) == 1 and int(job.req_time) <= idle_time:
                        tenant.idle_jobs.append(job)
