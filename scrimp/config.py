import ConfigParser
import sqlalchemy
import psycopg2
import pytz
import datetime
import ggprovisioner
from scrimp import Singleton, logger
import random


class ProvisionerConfig(object):
    """
    Stateful storage of loaded configuration values in an object. A
    ProvisionerConfig instantiated with its initializer loads config from
    disk and the DB.
    Because this class is a Singleton, multiple attempts to construct it will
    only return one instance of the class
    """
    __metaclass__ = Singleton

    def __init__(self, *args, **kwargs):
        """
        Load provisioner configuration based on the settings in a config file.
        """
        # override defaults with kwargs
        config_file = 'ggprovisioner/provisioner.ini'
        cloudinit_file = "cloudinit.cfg"
        if 'config_file' in kwargs:
            config_file = kwargs['config_file']
        if 'cloudinit_file' in kwargs:
            cloudinit_file = kwargs['cloudinit_file']

        # we need to pull cloudinit from the DB in the future
        self.cloudinit_file = cloudinit_file

        # read config from a file
        config = ConfigParser.ConfigParser()
        config.read(config_file)

        # get DB connection info
        user = config.get('Database', 'user')
        password = config.get('Database', 'password')
        host = config.get('Database', 'host')
        port = config.get('Database', 'port')
        database = config.get('Database', 'database')

        # create a connection and keep it as a config attribute
        try:
            engine = sqlalchemy.create_engine(
                'postgresql://%s:%s@%s:%s/%s' %
                (user, password, host, port, database),
                isolation_level="AUTOCOMMIT")
            self.dbconn = engine.connect()
        except psycopg2.Error:
            logger.exception("Failed to connect to database.")

        # Get some provisioner specific config settings
        self.ondemand_price_threshold = float(
            config.get('Provision', 'ondemand_price_threshold'))
        self.max_requests = int(config.get('Provision', 'max_requests'))
        self.run_rate = int(config.get('Provision', 'run_rate'))

        self.DrAFTS = config.get('Provision', 'DrAFTS')
        self.DrAFTSAvgPrice = config.get('Provision', 'DrAFTSAvgPrice')
        self.DrAFTSProfiles = config.get('Provision', 'DrAFTSProfiles')
        self.instance_types = []
        if self.DrAFTS == 'True':
            self.DrAFTS = True
        else:
            self.DrAFTS = False
        if self.DrAFTSProfiles == 'True':
            self.DrAFTSProfiles = True
        else:
            self.DrAFTSProfiles = False
        if self.DrAFTSAvgPrice == 'True':
            self.DrAFTSAvgPrice = True
        else:
            self.DrAFTSAvgPrice = False

        self.simulate = config.get('Simulation', 'Simulate')
        if self.simulate == 'True':
            self.simulate = True
        else:
            self.simulate = False

        self.neg_time = int(config.get('Simulation', 'NegTime'))
        self.job_number = int(config.get('Simulation', 'JobNumber'))
        self.idle_time = int(config.get('Simulation', 'IdleTime'))
        self.terminate = config.get('Simulation', 'Terminate')
        self.overhead_time = int(config.get('Simulation', 'OverheadTime'))
        self.simulate_jobs = (config.get('Simulation', 'JobFile'))
        self.run_name = config.get('Simulation', 'RunName')

        # things for the simulator
        self.first_job_time = None

        # drafts_stored_db = True

        # self.simulate = True
        self.sim_time = datetime.datetime.strptime('2017-03-25T03:14:00Z',
                                                   '%Y-%m-%dT%H:%M:%SZ')
        self.jobs_file = self.simulate_jobs.split("/")[-1]
        if self.job_number == 100:
            # 100 jobs
            self.jobs_file = '100jobs.json'
            self.simulate_jobs = '/home/ubuntu/100jobs.json'
            self.sim_time = datetime.datetime.strptime('2017-03-23T21:35:00Z',
                                                       '%Y-%m-%dT%H:%M:%SZ')
        elif self.job_number == 200:
            # 200 jobs
            self.jobs_file = '200jobs.json'
            self.simulate_jobs = '/home/ubuntu/200jobs.json'
            self.sim_time = datetime.datetime.strptime('2017-03-27T20:45:00Z',
                                                       '%Y-%m-%dT%H:%M:%SZ')
        elif self.job_number == 500:
            # 500 jobs
            self.jobs_file = '500jobs.json'
            self.simulate_jobs = '/home/ubuntu/500jobs.json'
            self.sim_time = datetime.datetime.strptime('2017-03-28T21:55:00Z',
                                                       '%Y-%m-%dT%H:%M:%SZ')
        elif self.job_number == 1000:
            # 1000 jobs
            self.jobs_file = '1000jobs.json'
            self.simulate_jobs = '/home/ubuntu/1000jobs.json'
            self.sim_time = datetime.datetime.strptime('2017-03-25T03:14:00Z',
                                                       '%Y-%m-%dT%H:%M:%SZ')
            if self.idle_time == 120:
                self.idle_time = 128

        self.sim_time = self.sim_time.replace(tzinfo=pytz.utc)

        self.run_id = random.randint(500, 10000)
        self.relative_time = None
        self.simulate_time = self.sim_time

        if self.simulate:
            self.simulator = ggprovisioner.cloud.simaws.aws_simulator.AWSSimulator()

    def load_instance_types(self):
        """
        Load instance types from database into config object
        """
        # this must be imported here to avoid a circular import
        from ggprovisioner.cloud import aws

        def get_instance_types():
            """
            Get the set of instances from the database
            """
            instances = []
            try:
                rows = self.dbconn.execute(
                    "select * from instance_type where available = True")
                for row in rows:
                    instances.append(aws.Instance(
                        row['id'], row['type'], row['ondemand_price'],
                        row['cpus'], row['memory'], row['disk'],
                        row['ami']))
            except psycopg2.Error:
                logger.exception("Error getting instance types from database.")
            return instances

        self.instance_types = get_instance_types()
