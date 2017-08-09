import logging

from .singleton import Singleton
from .simplestringifiable import SimpleStringifiable

logger = logging.getLogger('scrimp')

from .config import ProvisionerConfig

from .provisioner import Provisioner
