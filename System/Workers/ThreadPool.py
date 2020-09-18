import threading
from queue import Queue
import logging

# Inspired by https://www.metachris.com/2016/04/python-threadpool/


class PoolWorker(threading.Thread):
    """ Thread executing tasks from a given tasks queue """
    def __init__(self, task_queue, **kwargs):
        super(PoolWorker, self).__init__()
        self.task_queue = task_queue
        self.daemon = True
        self.start()

    def run(self):

        while True:

            # Obtain the arguments of the task
            try:
                args, kargs = self.task_queue.get()
            except Exception as e:
                logging.error("%s failed! Could not obtain task arguments from the task queue!" % self.__class__.__name__)
                if str(e) != "":
                    logging.error("Received the following error message:\n%s" % e)
                return

            # Initialize the number or retries
            num_retries = 0

            # Initialize flag of failure
            has_failed = True

            while has_failed and num_retries < 3:

                try:
                    # Run task
                    self.task(*args, **kargs)

                    # Set task as not failed
                    has_failed = False

                except Exception as e:

                    # Set task as failed task
                    has_failed = True

                    # Raise abstract function for handling thread
                    logging.error("%s failed!" % self.__class__.__name__)
                    if str(e) != "":
                        logging.error("Received the following error message:\n%s" % e)

                    # Let user know if we will retry
                    if num_retries < 3:
                        logging.info("We will retry the command. Already retried {0}/2 times.".format(num_retries))

                finally:

                    # If task has failed retry it
                    if has_failed:
                        num_retries += 1
                    else:
                        self.task_queue.task_done()

    def task(self, *args, **kargs):
        pass


class ThreadPool:
    """ Pool of threads consuming tasks from a queue """
    def __init__(self, num_threads, worker_class=None, **worker_kwargs):
        # Create task queue
        self.tasks = Queue(num_threads)

        # Set class of Worker in thread pool
        self.worker_class = PoolWorker if worker_class is None else worker_class

        # Initialize thread pool workers
        self.workers = []
        for _ in range(num_threads):
            self.workers.append(self.worker_class(self.tasks, **worker_kwargs))

    def add_task(self, *args, **kargs):
        """ Add a task to the queue """
        self.tasks.put((args, kargs))

    def wait_completion(self):
        """ Wait for completion of all the tasks in the queue """
        self.tasks.join()
