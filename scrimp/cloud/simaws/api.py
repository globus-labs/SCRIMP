import boto
from scrimp import ProvisionerConfig, logger


def get_spot_prices(instances, tenant):
    """
    Get the current spot price for each instance type.
    """
    new_time = ProvisionerConfig().simulate_time
    now = new_time.strftime('%Y-%m-%d %H:%M:%S')
    conn = boto.connect_ec2(tenant.access_key, tenant.secret_key)
    timeStr = str(now).replace(" ", "T") + "Z"
    for ins in instances:
        prices = conn.get_spot_price_history(
            instance_type=ins.type,
            product_description="Linux/UNIX (Amazon VPC)",
            end_time=timeStr, start_time=timeStr)
        for price in prices:
            for key, val in tenant.subnets.iteritems():
                if price.availability_zone == key:
                    ins.spot.update({price.availability_zone: price.price})


def tag_requests(req, tag, conn):
    """
    Tag any requests that have just been made with the tenant name
    """
    pass


def launch_ondemand_request(conn, request, tenant, job):
    pass


def launch_spot_request(conn, request, tenant, job):
    try:

        cost_aware_req = job.cost_aware
        drafts_req = job.DrAFTS
        drafts_avg = job.DrAFTSAvg

        cost_aware_req = job.cost_aware
        drafts_req = job.cost_aware
        drafts_avg = job.cost_aware

        mapping = None

        my_req_ids = ProvisionerConfig().simulator.request_spot_instances(
            price=request.bid, image_id=request.ami,
            subnet_id=tenant.subnets[request.zone],
            count=request.count,
            key_name=tenant.key_pair,
            security_group_ids=[tenant.security_group],
            instance_type=request.instance_type,
            user_data=customise_cloudinit(tenant, job),
            block_device_map=mapping,
            job=job)
        for req in my_req_ids:
            # tag each request
            tag_requests(req, tenant.name, conn)

            ProvisionerConfig().dbconn.execute(
                ("insert into instance_request (tenant, instance_type, " +
                 "price, job_runner_id, request_type, request_id, " +
                 "subnet, cost_aware_ins, cost_aware_bid, " +
                 "cost_aware_subnet, " +
                 "drafts_ins, drafts_bid, drafts_subnet, selected_avg_price, "
                 "cost_aware_avg_price, drafts_avg_price, drafts_avg_ins, " +
                 "drafts_avg_bid, drafts_avg_subnet, drafts_avg_avg_price) " +
                 "values ('%s', '%s', %s, '%s', '%s', '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)") %
                (tenant.db_id, request.instance.db_id, request.OraclePrice,
                 job.id,
                 "spot", req, tenant.subnets_db_id[request.zone],
                 cost_aware_req.instance.db_id, cost_aware_req.bid,
                 tenant.subnets_db_id[cost_aware_req.zone],
                 drafts_req.instance.db_id,
                 drafts_req.DrAFTS, tenant.subnets_db_id[drafts_req.zone],
                 request.AvgPrice, cost_aware_req.AvgPrice,
                 drafts_req.AvgPrice,
                 drafts_avg.instance.db_id, drafts_avg.DrAFTS,
                 tenant.subnets_db_id[
                     drafts_avg.zone],
                 drafts_avg.AvgPrice))

        return my_req_ids
    except boto.exception.EC2ResponseError:
        logger.exception("There was an error communicating with EC2.")


def request_resources(tenant):
    """
    Request the resources that have been selected for each job
    """
    conn = None
    output_string = "Name: %s\n" % tenant.name
    output_string = "%sTenant: %s\n" % (output_string, tenant.name)
    instance_req_string = ""
    req_cpus = 0
    req_instances = 0

    for job in tenant.idle_jobs:
        if job.fulfilled is False:
            request = job.launch
            if request is None:
                logger.debug("Failed to find request object for job %s" % job)
                continue
            # increment some counters
            req_instances += int(request.count)
            req_cpus += int(job.req_cpus)
            # Launch any on-demand requests
            if request.ondemand:
                # launch the ondemand request
                launch_ondemand_request(conn, request, tenant, job)
                instance_req_string = (
                    ("%sONDEMAND_INSTANCE_REQUEST" +
                     "\t%s\t%s\t%s\t%s\t%s\n") %
                    (instance_req_string, tenant.name,
                     request.instance_type, request.bid, job.id,
                     "ondemand"))
            else:
                # launch the spot request
                # TODO batch request instances of the same type
                req_ids = launch_spot_request(conn, request, tenant, job)
                for req in req_ids:
                    instance_req_string = (
                        ("%sSPOT_INSTANCE_REQUEST" +
                         "\t%s\t%s\t%s\t%s\t%s\tDrAFTS: %s\t%s\n") %
                        (instance_req_string, tenant.name,
                         request.instance_type, request.bid, job.id,
                         "spot", request.DrAFTS, req))


def customise_cloudinit(tenant, job):
    """
    Use a string template to construct an appropriate cloudinit script to
    pass as userdata to the aws request.
    """
    return None
