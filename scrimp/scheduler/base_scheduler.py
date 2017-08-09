import datetime
import psycopg2
from pytz import timezone
from scrimp import logger, ProvisionerConfig


class BaseScheduler():

    def only_load_jobs(self, tenants):
        """
        Only do the job load. This is so the ignore stuff can be run after
        the simulator
        has ordered things into executing/finished etc.
        """
        # Clear out the lists then reload them.
        for t in tenants:
            t.idle_jobs = []
            t.jobs = []
        all_jobs = self.get_global_queue()
        if ProvisionerConfig().simulate:
            if ProvisionerConfig().relative_time is None:

                self.job_data = None
                utc = timezone('UTC')
                ProvisionerConfig().relative_time = datetime.datetime.now(utc)

        # Assoicate the jobs from the global queue with each of the tenants
        self.process_global_queue(all_jobs, tenants)

    def process_idle_jobs(self, tenants):
        """
        Go through and ignore fulfilled jobs etc.
        """

        t1 = datetime.datetime.now()
        ignore_fulfilled_jobs(tenants)
        t2 = datetime.datetime.now()
        # Stop resources being requested too frequently
        stop_over_requesting(tenants)

        t3 = datetime.datetime.now()

        ig_time = (t2 - t1).total_seconds()
        over_time = (t3 - t2).total_seconds()
        logger.debug("SIMULATION load times: ignore (%s), over req (%s)" %
                     (ig_time, over_time))

    def load_jobs(self, tenants):
        """
        Read in the condor queue and manage the removal of jobs that should
        not be processed.
        """
        # Assess the global queue

        # Clear out the lists then reload them.
        for t in tenants:
            t.idle_jobs = []
            t.jobs = []
        t1 = datetime.datetime.now()
        all_jobs = self.get_global_queue()
        t2 = datetime.datetime.now()
        if ProvisionerConfig().simulate:
            if ProvisionerConfig().relative_time is None:

                self.job_data = None
                utc = timezone('UTC')
                ProvisionerConfig().relative_time = datetime.datetime.now(utc)

        # Assoicate the jobs from the global queue with each of the tenants
        self.process_global_queue(all_jobs, tenants)
        t3 = datetime.datetime.now()

        ignore_fulfilled_jobs(tenants)
        t4 = datetime.datetime.now()
        # Stop resources being requested too frequently
        stop_over_requesting(tenants)
        t5 = datetime.datetime.now()

        queue_time = (t2 - t1).total_seconds()
        process_time = (t3 - t2).total_seconds()
        ignore_time = (t4 - t3).total_seconds()
        stop_time = (t5 - t4).total_seconds()
        logger.debug("SIMULATION load times: queue (%s), process (%s), "
                     "ignore (%s), stop (%s)" % (
            queue_time, process_time, ignore_time, stop_time))

    def get_global_queue(self):
        """
        Poll all queues and return a set of Jobs.
        """
        pass

    def get_status(self, pool):
        """
        Poll the collector of a pool to get the status, describing the
        resources in the pool.
        """
        return ""

    def process_global_queue(self, jobs, tenants):
        """
        Associate each job with a tenant and add them to their local list of
        jobs.
        """
        pass


def ignore_fulfilled_jobs(tenants):
    """
    Check whether a job's spot requests have been fulfilled yet. If so,
    remove the job from the idle_jobs list. Also check whether any
    outstanding, but still valid, request exists. If there are other
    requests for the job, migrate or cancel them.
    """
    # Add in a variable of whether or not to acquire more instances
    # after a job has been fulfilled. This catches jobs that have instances
    # revoked and return to the idle queue.
    fulfill_revoked = True
    revoked_time = 600
    for tenant in tenants:
        # Check to see if any entries have been made in the instance table
        # this indicates an instance has been fulfilled for a request.
        # Restrict the query to only looking at requests for a specific job
        for job in list(tenant.idle_jobs):

            if ProvisionerConfig().simulate:
                if job.sim_status != "IDLE":
                    continue
                new_ins = ProvisionerConfig().simulator.resources
                for i in new_ins:
                    if i.job_runner_id == job.id:
                        job.fulfilled = True
                        diff = (ProvisionerConfig().simulate_time -
                                i.launch_time).total_seconds()
                        if diff >= revoked_time:
                            job.fulfilled = False
                        continue
            else:

                rows = ProvisionerConfig().dbconn.execute(
                    ("select instance_request.job_runner_id, "
                        "instance_type.cpus, EXTRACT(EPOCH FROM (Now() - "
                        "instance_request.request_time)) as seconds from "
                        "instance_request, "
                        "instance_type, instance where "
                        "instance_type.id = instance_request.instance_type "
                        "and instance.request_id = instance_request.id "
                        "and instance_request.job_runner_id = '%s' "
                        "and tenant = %s order by instance_request.id "
                        "desc") % (job.id, tenant.db_id))

                fulfilled_cpus = 0
                set_false = False
                # Iterate over the fulfilled instance requests for this job
                for row in rows:
                    # Set fulfilled back to False if the revoked time
                    # has passed.
                    # This should only happen if the instance is
                    # terminated early.
                    # This works as the job won't be in the idle queue
                    # (we are iterating)
                    # if the job hasn't been kicked off an instance.
                    if fulfilled_cpus == 0 and fulfill_revoked:
                        if int(row['seconds']) > revoked_time:
                            job.fulfilled = False
                            set_false = True
                            # logger.debug(("Found a revoked job, setting " +
                            # "fulfilled back to false."))

                    # Work out how many cpus this instance type has
                    fulfilled_cpus = fulfilled_cpus + int(row['cpus'])

                    # If this is being simulated I need to work out if an
                    # instance would have been terminated?
                    # maybe the easiest way is to just do it through the db?

                # If enough cpus have been acquired, flag the job as fulfilled
                if fulfilled_cpus >= int(job.req_cpus) and set_false is False:
                    job.fulfilled = True
                    continue

                # Also remove any that have an ondemand instance fulfilled
                rows = ProvisionerConfig().dbconn.execute(
                    ("select instance_request.job_runner_id "
                        "from instance_request, instance where "
                        "instance.request_id = instance_request.id "
                        "and instance_request.job_runner_id = '%s' "
                        "and tenant = %s and instance_request.request_type = "
                        "'ondemand'") % (job.id, tenant.db_id))
                for row in rows:
                    job.fulfilled = True
        # Remove any jobs that have been set as fulfilled from the idle
        # queue
        for job in list(tenant.idle_jobs):
            if job.fulfilled:
                tenant.idle_jobs.remove(job)
                continue


def stop_over_requesting(tenants):
    """
    Stop too many requests being made for an individual job. This is the
    frequency of new requests being made for an individual job.
    Future work would be to look at launching many requests instantly, and
    then cancelling requests once one is fulfilled.
    """
    for tenant in tenants:
        # Stop excess instances being requested in a five minute round
        logger.debug("Tenant: %s. Request rate: %s" % (tenant.name,
            tenant.request_rate))

        if ProvisionerConfig().simulate:
            open_reqs = ProvisionerConfig().simulator.requests

        for job in list(tenant.idle_jobs):
            if ProvisionerConfig().simulate:
                if job.sim_status != "IDLE":
                    continue
            # check to see if we are requesting too frequently
            count = 0
            try:
                if ProvisionerConfig().simulate:
                    open_reqs = ProvisionerConfig().simulator.requests
                    for openreq in open_reqs:
                        if openreq.job_runner_id == job.id:
                            # found an existing job
                            diff = (ProvisionerConfig().simulate_time -
                                    openreq.request_time).total_seconds()
                            if diff <= tenant.request_rate:

                                count = 1
                else:
                    rows = ProvisionerConfig().dbconn.execute(
                        ("select count(*) from instance_request " +
                         "where job_runner_id = '%s' and " +
                         "request_time >= Now() - " +
                         "'%s second'::interval and tenant = %s;") %
                        (job.id, tenant.request_rate, tenant.db_id))
                    for row in rows:
                        count = row['count']
            except psycopg2.Error:
                logger.exception("Error getting number of outstanding "
                                 "requests within time frame.")
            if count > 0:
                tenant.idle_jobs.remove(job)
                logger.debug("Too many requests. Removed job %s" % job.id)
                continue

            # now check to see if we already have too many requests for
            # this job
            try:
                if ProvisionerConfig().simulate:
                    count = 0
                    open_reqs = ProvisionerConfig().simulator.requests
                    for openreq in open_reqs:
                        if openreq.job_runner_id == job.id:
                            count = count + 1
                    if count > ProvisionerConfig().max_requests:
                        logger.warn("Too many outstanding requests, " +
                                    "removing idle job: %s" % repr(job))
                        tenant.idle_jobs.remove(job)
                else:
                    rows = ProvisionerConfig().dbconn.execute(
                        ("select count(*) from instance_request "
                         "where job_runner_id = '%s' and tenant = %s;") %
                        (job.id, tenant.db_id))
                    count = 0
                    for row in rows:
                        count = row['count']
                    if count > ProvisionerConfig().max_requests:
                        logger.warn("Too many outstanding requests, "
                                    "removing idle job: %s" % repr(job))
                        tenant.idle_jobs.remove(job)
            except psycopg2.Error:
                logger.exception("Error getting number of outstanding "
                                 "requests.")
