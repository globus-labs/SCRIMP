import psycopg2
import datetime
import calendar
import time
# import sys
from decimal import *
import requests

from scrimp import logger, ProvisionerConfig, tenant, scheduler
from scrimp.cloud import aws
from scrimp.cloud import simaws
from scrimp.scheduler.condor.condor_scheduler import CondorScheduler
from scrimp.scheduler.simfile.sim_scheduler import SimScheduler


class Provisioner(object):
    """
    A provisioner for cloud resources.
    Cost effectively acquires and manages instances.
    """

    def __init__(self):
        self.tenants = []

        self.drafts_mapping = {'us-east-1a': 'us-east-1e',
                               'us-east-1b': 'us-east-1d',
                               'us-east-1c': 'us-east-1a',
                               'us-east-1d': 'us-east-1b',
                               'us-east-1e': 'us-east-1c', }

        # Read in any config data and set up the database connection
        ProvisionerConfig()

    def run(self):
        """
        Run the provisioner. This should execute periodically and
        determine what actions need to be taken.
        """
        self.run_iterations = 0
        # self.simulate = False
        if ProvisionerConfig().simulate:
            self.sched = SimScheduler()
            ProvisionerConfig().load_instance_types()
            self.load_drafts_data()
            while True:
                self.run_iterations = self.run_iterations + 1
                # Load jobs
                t1 = datetime.datetime.now()
                start_time = datetime.datetime.now()
                self.load_tenants_and_jobs()

                t2 = datetime.datetime.now()
                # Simulate the world (mostly tidy things up and print stats)
                ProvisionerConfig().simulator.simulate(self.tenants)
                t3 = datetime.datetime.now()

                self.sched.process_idle_jobs(self.tenants)
                tx = datetime.datetime.now()
                # Simulate Condor
                ProvisionerConfig().simulator.run_condor(self.tenants)
                t4 = datetime.datetime.now()
                # Simulate AWS
                ProvisionerConfig().simulator.run_aws()
                t5 = datetime.datetime.now()
                # Check if it should finish executing (e.g. jobs and
                # resources all terminated)
                if ProvisionerConfig().simulator.check_finished():
                    break

                self.manage_resources()
                t6 = datetime.datetime.now()
                if ((ProvisionerConfig().simulate_time -
                    ProvisionerConfig().sim_time).total_seconds() %
                    ProvisionerConfig().run_rate == 0):

                    self.provision_resources()
                t7 = datetime.datetime.now()

                load_time = (t2 - t1).total_seconds()
                sim_time = (t3 - t2).total_seconds()
                proc_idle_time = (tx - t3).total_seconds()
                condor_time = (t4 - tx).total_seconds()
                aws_time = (t5 - t4).total_seconds()
                manage_time = (t6 - t5).total_seconds()
                prov_time = (t7 - t6).total_seconds()

                # Otherwise, step through time
                ProvisionerConfig().simulate_time = ProvisionerConfig(
                ).simulate_time + datetime.timedelta(seconds=2)
                logger.debug("RUN ID: %s. SIMULATION: advancing time "
                             "2 second" % ProvisionerConfig().run_id)

                logger.debug("SIMULATION times: load (%s), sim (%s),"
                             " proc_idle (%s), condor (%s), aws (%s),"
                             " manage (%s), prov (%s)" % (
                                 load_time, sim_time, proc_idle_time,
                                 condor_time,
                                 aws_time, manage_time, prov_time))

        else:
            self.sched = CondorScheduler()
            while True:
                self.run_iterations = self.run_iterations + 1
                # Get the tenants from the database and process the current
                # condor_q. Also assign those jobs to each tenant.
                start_time = datetime.datetime.now()
                self.load_tenants_and_jobs()
                # provisioning will fail if there are no tenants
                if len(self.tenants) > 0:
                    # Handle all of the existing requests. This will cancel
                    # or migrate excess requests and update the database to
                    # reflect the state of the environment
                    self.manage_resources()

                    # Work out the price for each instance type and acquire
                    # resources for jobs
                    self.provision_resources()

                # wait "run_rate" seconds before trying again
                end_time = datetime.datetime.now()
                diff = (end_time - start_time).total_seconds()
                logger.debug("SCRIMP (SIMULATION) run loop: "
                             "%s seconds. Now sleeping %s seconds." % (
                                 diff, ProvisionerConfig().run_rate))
                if diff < ProvisionerConfig().run_rate:
                    time.sleep(ProvisionerConfig().run_rate - diff)

    def load_tenants_and_jobs(self):
        """
        Get all of the tenants from the database and then read the condor
        queue to get their respective jobs.
        """
        # Load all of the tenants

        # Load all of the jobs from condor and associate them with the tenants.
        # This will also remove jobs that should not be processed (e.g. an
        # instance has been fulfilled for them already).
        if ProvisionerConfig().simulate:
            # lets only do this once.
            if ProvisionerConfig().relative_time is None:
                self.tenants = tenant.load_from_db()
            self.sched.only_load_jobs(self.tenants)

        else:
            self.tenants = tenant.load_from_db()
            self.sched.load_jobs(self.tenants)

    def manage_resources(self):
        """
        Use the resource manager to keep the database up to date and manage
        aws requests and resources.
        """
        # Build a set of instances and their current spot prices so we don't
        # need to keep revisiting the AWS API

        if ProvisionerConfig().simulate:
            simaws.manager.process_resources(self.tenants)
        else:
            aws.manager.process_resources(self.tenants)

            scheduler.base_scheduler.ignore_fulfilled_jobs(self.tenants)

    def load_drafts_data(self):
        """
        To speed this up, load in all the drafts data once per
        provisioning cycle
        """
        cur_time = datetime.datetime.utcnow()
        if ProvisionerConfig().simulate:
            cur_time = ProvisionerConfig().simulator.get_fake_time()

        minus_ten = cur_time - datetime.timedelta(seconds=600)
        query = ("select * from drafts_price where timestamp < "
                 "'%s'::TIMESTAMP and timestamp > '%s'::TIMESTAMP") % (
            cur_time.strftime("%Y-%m-%d %H:%M"),
            minus_ten.strftime("%Y-%m-%d %H:%M"))
        self.drafts_data = []
        logger.debug('getting drafts data: ' + query)
        rows = ProvisionerConfig().dbconn.execute(query)
        for row in rows:
            data = {'time': row['time'], 'price': row['price'],
                    'zone': row['zone'], 'type': row['type']}
            self.drafts_data.append(data)

    def provision_resources(self):
        # This passes tenant[0] (a test tenant with my credentials) to use its
        # credentials to query the AWS API for price data
        # price data is stored in the Instance objects
        for t in self.tenants:
            if len(t.idle_jobs) == 0:
                continue
            if (ProvisionerConfig().DrAFTS or
                    ProvisionerConfig().DrAFTSProfiles):
                if ProvisionerConfig().simulate:
                    # when simulating only load it every 5 mins.
                    if ((ProvisionerConfig().simulate_time -
                         ProvisionerConfig().sim_time).total_seconds() %
                            300 == 0):
                        self.load_drafts_data()
                else:
                    if self.run_iterations % 300 == 0:
                        self.load_drafts_data()
            # Get the spot prices for this tenant's AZ's
            if ProvisionerConfig().simulate:
                simaws.api.get_spot_prices(
                    ProvisionerConfig().instance_types, t)
            else:
                aws.api.get_spot_prices(ProvisionerConfig().instance_types,
                                        t)
            # Select a request to make for each job
            self.select_instance_type(ProvisionerConfig().instance_types)
            # Make the requests for the resources
            if ProvisionerConfig().simulate:
                simaws.api.request_resources(t)
            else:
                aws.api.request_resources(t)

    def get_potential_instances(self, eligible_instances, job, tenant):
        """
        Make a list of all <type,zone> and <type,ondemand> pairs then order
        them.
        """
        # Putting this here so it isn't called every run
        # commented out to stop it checking drafts prices

        unsorted_instances = []
        # Add an entry for each instance type as ondemand, or each spot
        # price so we can sort everything and pick the cheapest.
        for ins in eligible_instances:
            unsorted_instances.append(aws.Request(
                ins, ins.type, "", ins.ami, 1, 0, True,
                ins.ondemand, ins.ondemand, ins.ondemand, ins.ondemand,
                ins.ondemand))
            # Don't bother adding spot prices if it is an ondemand request:
            if not job.ondemand:
                DrAFTS = None
                AvgPrice = None
                OraclePrice = None
                for zone, price in ins.spot.iteritems():
                    # if zone == 'us-east-1c':
                    if (ProvisionerConfig().DrAFTS or
                            ProvisionerConfig().DrAFTSProfiles):
                        DrAFTS, OraclePrice = self.get_DrAFTS_bid(
                            ins.type, zone, job, price)
                        if DrAFTS is None or OraclePrice is None:
                            # try it again, if it doesn't find them its
                            # because the price doesn't exist. so add a big
                            # value to skip it
                            DrAFTS, OraclePrice = self.get_DrAFTS_bid(
                                ins.type, zone, job, price)
                            if DrAFTS is None:
                                DrAFTS = 1000
                            if OraclePrice is None:
                                OraclePrice = 1000
                        if ProvisionerConfig().DrAFTS:
                            unsorted_instances.append(aws.Request(
                                ins, ins.type, zone, ins.ami, 1, 0, False,
                                ins.ondemand, DrAFTS, 0, 0, 0))
                        elif ProvisionerConfig().DrAFTSProfiles:
                            unsorted_instances.append(aws.Request(
                                ins, ins.type, zone, ins.ami, 1, 0, False,
                                ins.ondemand, OraclePrice, 0, 0, 0))
                    else:
                        unsorted_instances.append(aws.Request(
                            ins, ins.type, zone, ins.ami, 1, 0, False,
                            ins.ondemand, price, 0, 0, 0))
                    logger.debug('%s, %s spot: %s drafts: %s profile: %s' % (
                        ins.type, zone, price, DrAFTS, OraclePrice))

        # Now sort all of these instances by price
        sorted_instances = []
        # Adding and false here to force it to use the cheapest price for now.
        if ProvisionerConfig().DrAFTS:
            # This should sort by the drafts price and then by the current
            # spot price that way we will get the cheapest AZ at the top of
            # the list.
            sorted_instances = sorted(unsorted_instances,
                                      key=lambda k: (k.DrAFTS, k.price))
        if ProvisionerConfig().DrAFTSProfiles:
            sorted_instances = sorted(unsorted_instances,
                                      key=lambda k: (k.OraclePrice, k.price))

        else:
            sorted_instances = sorted(
                unsorted_instances, key=lambda k: k.price)
        return sorted_instances

    def get_DrAFTS_bid(self, ins, zone, job, cur_price):
        """
        Pull the DrAFTS price for this instance type.
        This will get the nearest value greater than 1 hour.
        """
        # example: http://128.111.84.183/vpc/us-east-1a-c3.2xlarge.pgraph
        try:
            ret_drafts = None
            ret_oracle = None

            if ProvisionerConfig().drafts_stored_db:
                # clear the tenant's current avg prices
                mapped_zone = self.drafts_mapping[zone]
                logger.debug('drafts zone: %s' % mapped_zone)
                for row in self.drafts_data:
                    if (row['type'] == ins and mapped_zone == row['zone'] and
                            float(row['price']) > float(cur_price)):
                        time = row['time']
                        cost = row['price']
                        if ret_drafts is None and float(time) > 1:
                            ret_drafts = Decimal(str(cost))
                        if (ret_oracle is None and float(time) >
                                (float(job.duration) / 3600)):
                            ret_oracle = Decimal(str(cost))

                return ret_drafts, ret_oracle
            else:
                # use the mapping between AZs to pick a zone name
                mapped_zone = self.drafts_mapping[zone]

                addr = 'http://128.111.84.183/vpc/%s-%s.pgraph' % (
                    mapped_zone, ins)
                req = requests.get(addr)
                output = req.text
                # Split the result by line
                lines = output.split("\n")
                ret_drafts = None
                # define these out here so if it goes over the line,
                # when the request length is too long, it can use the
                # previous ones.
                cost = None
                time = None
                for line in lines:
                    # Extract the time and cost
                    try:
                        time = line.split(" ")[0]
                        cost = line.split(" ")[1]
                    except Exception, y:
                        logger.error("drafts: Failed here: %s %s" % (y, line))
                   # Split the line in half to get the time and cost
                    if float(time) > 1:
                        # this is the one we want to use
                        ret_drafts = Decimal(str(cost))
                        break
                # now do the oracle ones
                ret_oracle = None
                last = False
                for line in lines:
                    # Extract the time and cost
                    try:
                        if len(line) > 5:
                            time = line.split(" ")[0]
                            cost = line.split(" ")[1]
                        else:
                            last = True
                            logger.debug("No prediction long enough in "
                                         "%s, using last one. %s %s" % (addr,
                                                                        time,
                                                                        cost))
                    except Exception, z:
                        logger.error("oracle: failed here: %s %s" % (z, line))
                    # Split the line in half to get the time and cost
                    if last or float(time) > (float(job.duration) / 3600):
                        # this is the one we want to use
                        ret_oracle = Decimal(str(cost))
                        break
                return ret_drafts, ret_oracle
        except Exception, e:
            logger.debug("Failed to find DrAFTS price for %s. %s" % (ins, e))
        return None, None

    def print_cheapest_options(self, sorted_instances):
        # Print out the top three
        logger.info("Top three to select from:")
        top_three = 3
        for ins in sorted_instances:
            if top_three == 0:
                break
            if ProvisionerConfig().DrAFTS:
                logger.info("DrAFTS:  %s %s %s %s" % (ins.instance_type,
                    ins.zone, ins.price, ins.DrAFTS))
            if ProvisionerConfig().DrAFTSAvgPrice:
                logger.info("DrAFTS Oracle Price: %s %s %s %s" % (ins.instance_type,
                    ins.zone, ins.price, ins.OraclePrice))
            else:
                logger.info("    %s %s %s" %
                            (ins.instance_type, ins.zone, ins.price))
            top_three = top_three - 1

    def get_timeout_ondemand(self, job, tenant, instances):
        """
        Check to see if the job now requires an ondemand instance due to
        timing out.
        """
        cur_time = datetime.datetime.now()
        cur_time = calendar.timegm(cur_time.timetuple())
        time_idle = 0
        if ProvisionerConfig().simulate:
            cur_time = ProvisionerConfig().simulate_time
            time_idle = (ProvisionerConfig().simulate_time -
                         job.req_time).total_seconds()
        else:
            time_idle = cur_time - int(job.req_time)

        res_instance = None
        # if the tenant has set a timeout and the job has been idle longer than
        # this
        if tenant.timeout > 0 and time_idle > tenant.timeout:
            # sort the eligibile instances by their ondemand price (odp)
            sorted_instances = sorted(instances, key=lambda k: k.odp)
            logger.debug("Selecting ondemand instance: %s" % str(job.launch))
            res_instance = sorted_instances[0]
        return res_instance

    def check_ondemand_needed(self, tenant, sorted_instances, job):
        # Check to see if an ondemand instance is required due to timeout
        needed = False
        launch_instance = self.get_timeout_ondemand(job, tenant,
                                                    sorted_instances)
        cheapest = sorted_instances[0]

        # check to see if it timed out
        if (launch_instance is not None and
                launch_instance.odp < tenant.max_bid_price):
            job.launch = aws.Request(
                launch_instance, launch_instance.type, "", launch_instance.ami,
                1, launch_instance.odp, True)
            logger.debug("Selected to launch on demand due to timeout: %s" %
                         str(job.launch))
            needed = True

        # check if the job is flagged as needing on-demand
        elif job.ondemand:
            needed = True

        # if the cheapest option is ondemand
        elif cheapest.ondemand and cheapest.odp < tenant.max_bid_price:
            job.launch = cheapest
            logger.debug("Selected to launch on demand due to ondemand "
                         "being cheapest: %s" % repr(cheapest))
            needed = True

        # or if the cheapest option close in price to ondemand, then use
        # ondemand.
        elif (cheapest.price >
                (ProvisionerConfig().ondemand_price_threshold *
                    float(cheapest.odp)) and
                cheapest.price < tenant.max_bid_price):
            job.launch = cheapest
            logger.debug("Selected to launch on demand due to spot price "
                         "being close to ondemand price: %s" %
                         repr(cheapest))
            needed = True

        return needed

    def select_instance_type(self, instances):
        """
        Select the instance to launch for each idle job.
        """

        for tenant in self.tenants:
            for job in list(tenant.idle_jobs):
                if ProvisionerConfig().simulate:
                    time.sleep(ProvisionerConfig().overhead_time)
                # Get the set of instance types that can be used for this job
                eligible_instances = self.restrict_instances(job)
                if len(eligible_instances) == 0:
                    logger.error("Failed to find any eligible instances "
                                 "for job %s" % job)
                    continue
                # get all potential pairs and sort them
                sorted_instances = self.get_potential_instances(
                    eligible_instances, job, tenant)
                if len(sorted_instances) == 0:
                    logger.error("Failed to find any sorted instances "
                                 "for job %s" % job)
                    continue

                # work out if an ondemand instance is needed
                job.ondemand = self.check_ondemand_needed(tenant,
                                                          sorted_instances,
                                                          job)

                # If ondemand is required, redo the sorted list with only
                # ondemand requests and set that to be the launched instance
                if job.ondemand:
                    sorted_instances = self.get_potential_instances(
                        eligible_instances, job, tenant)

                    job.launch = sorted_instances[0]
                    logger.debug("Launching ondemand for this job. %s" %
                                 str(job.launch))
                    continue

                # otherwise we are now looking at launching a spot request
                # print out the options we are looking at
                self.print_cheapest_options(sorted_instances)
                # filter out a job if it has had too many requests made
                existing_requests = self.get_existing_requests(tenant, job)
                if len(existing_requests) >= ProvisionerConfig().max_requests:
                    tenant.idle_jobs.remove(job)
                    continue

                # Find the top request that hasn't already been requested
                # (e.g. zone+type pair is not in existing_requests)
                for req in sorted_instances:
                    if len(existing_requests) > 0:
                        # Skip this type if a matching request already
                        # exists
                        exists = False
                        for existing in existing_requests:
                            if (req.instance_type == existing.instance_type and
                                    req.zone == existing.zone):
                                exists = True
                        if exists:
                            continue
                    # Launch this type.
                    # Hmm, this is getting more complciated with
                    # multuiple provisioning models.
                    if req.price < tenant.max_bid_price:
                        req.bid = self.get_bid_price(job, tenant, req)
                        job.launch = req
                        job.cost_aware = req
                        break
                    else:
                        logger.error(("Unable to launch request %s as "
                                      "the price is higher than max bid "
                                      "%s.") % (str(req),
                                                tenant.max_bid_price))

    def get_existing_requests(self, tenant, job):
        # Get all of the outstanding requests from the db for this instance
        existing_requests = []
        try:
            rows = ProvisionerConfig().dbconn.execute(
                ("select instance_request.instance_type, "
                 "instance_request.request_type, "
                 "instance_type.type, "
                 "instance_request.subnet, subnet_mapping.zone "
                 "from instance_request, subnet_mapping, instance_type "
                 "where job_runner_id = '%s' and "
                 "instance_request.tenant = %s and "
                 "instance_request.instance_type = instance_type.id and "
                 "subnet_mapping.id = instance_request.subnet") %
                (job.id, tenant.db_id))
            for row in rows:
                existing_requests.append(aws.Request(
                    None, row['type'],
                    row['zone'], None, None))
        except psycopg2.Error:
            logger.exception("Error getting number of outstanding")

        return existing_requests

    def restrict_instances(self, job):
        """
        Filter out instances that do not meet the requirements of a job then
        return a list of the eligible instances.
        """
        eligible_instances = []

        # Check if the instance is viable for the job
        instance_types = ProvisionerConfig().instance_types
        for instance in instance_types:
            if aws.manager.check_requirements(instance, job):
                eligible_instances.append(instance)

        return eligible_instances

    def get_bid_price(self, job, tenant, req):
        """
        This function is not totally necessary at the moment, but it could be
        expanded to include more complex logic when placing a bid.
        Currently it just does bid percent * ondemand price of the resource
        and checks it is less than the maximum bid.
        """
        if ProvisionerConfig().DrAFTS or ProvisionerConfig().DrAFTSProfiles:
            return req.price

        bid = float(tenant.bid_percent) / 100 * float(req.odp)
        if bid <= tenant.max_bid_price:
            return bid
        else:
            return 0.40
