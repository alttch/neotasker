__author__ = 'Altertech, https://www.altertech.com/'
__copyright__ = 'Copyright (c) 2019 Altertech'
__license__ = 'MIT'
__version__ = '0.0.36'

from neotasker.supervisor import TaskSupervisor

task_supervisor = TaskSupervisor(supervisor_id='default')

spawn = task_supervisor.spawn
spawn_ajob = task_supervisor.create_async_job

from neotasker.f import FunctionCollection

from neotasker.localproxy import LocalProxy

from neotasker.workers import background_worker
from neotasker.workers import BackgroundWorker, BackgroundAsyncWorker
from neotasker.workers import BackgroundEventWorker, BackgroundQueueWorker
from neotasker.workers import BackgroundIntervalWorker

import neotasker.supervisor
import neotasker.embed
import aiosched

g = LocalProxy()


def set_debug(mode=True):
    neotasker.supervisor.debug = mode
    neotasker.workers.debug = mode
    neotasker.embed.debug = mode
    aiosched.set_debug(mode)
