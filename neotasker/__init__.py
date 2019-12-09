__author__ = 'Altertech, https://www.altertech.com/'
__copyright__ = 'Copyright (c) 2019 Altertech'
__license__ = 'MIT'
__version__ = '0.0.2'

from neotasker.supervisor import TaskSupervisor

task_supervisor = TaskSupervisor(supervisor_id='default')

from neotasker.f import FunctionCollection

from neotasker.localproxy import LocalProxy

from neotasker.workers import background_worker
from neotasker.workers import BackgroundWorker, BackgroundAsyncWorker
from neotasker.workers import BackgroundEventWorker, BackgroundQueueWorker

import neotasker.supervisor
import aiosched

g = LocalProxy()


def set_debug(mode=True):
    neotasker.supervisor.debug = mode
    neotasker.workers.debug = mode
    aiosched.set_debug(mode)
