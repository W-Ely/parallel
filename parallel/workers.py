import logging
from queue import Empty, Queue
from threading import Event, Thread

_LOGGER = logging.getLogger(__name__)


def chain(iterable, **jobs):
    finished_q, stop_event, exception_q = Queue(), Event(), Queue()
    work_q, work_pool = finished_q, []
    try:
        for job in reversed(list(jobs)):
            work_q, threads = _start_workers(work_q, stop_event, exception_q, **jobs[job])
            work_pool.append({"work_q": work_q, "threads": threads})
        _start_work(iterable, work_q, jobs[job]["num_workers"])
    except Exception as e:
        exception_q.put(e)
        stop_event.set()
    _join_workers(work_pool)
    _propagate_exceptions(exception_q)
    return finished_q


def _start_work(iterable, work_q, num_workers):
    for item in iterable:
        work_q.put(item)


def _start_workers(finished_q, stop_event, exception_q, **kwargs):
    work_q = Queue()
    threads = []
    for i in range(kwargs["num_workers"]):
        t = Thread(target=_worker, args=(work_q, finished_q, stop_event, exception_q), kwargs=kwargs)
        t.start()
        threads.append(t)
    return work_q, threads


def _worker(
    work_q,
    finished_q,
    stop_event,
    exception_q,
    target=None,
    uninstantiated_client=None,
    client_args=None,
    client_kwargs=None,
    **kwargs,
):
    _client = uninstantiated_client(*client_args, **client_kwargs)
    while not stop_event.is_set():
        try:
            arg = work_q.get()
            if arg is None:
                _LOGGER.debug("Worker finished")
                break
            complete = target(arg, client=_client)
            _LOGGER.debug(f"Arg complete: {complete}")
            finished_q.put(complete)
        except Exception as e:
            exception_q.put(e)
            stop_event.set()


def _join_workers(work_pool):
    _LOGGER.debug("Main thread waiting to joining queues and workers.")
    for i, job in enumerate(reversed(work_pool)):
        _LOGGER.debug(f"Sending end signals to queue number: {i + 1}")
        for i in range(len(job["threads"])):  # Send end signals
            job["work_q"].put(None)
        _LOGGER.debug(f"Joining threads from target number: {i + 1}")
        for t in job["threads"]:  # Join worker threads
            t.join()


def _propagate_exceptions(exception_q):
    _LOGGER.debug("Propagating exceptions if any.")
    try:
        e = exception_q.get(block=False)
        raise e
    except Empty:
        pass


if __name__ == "__main__":
    import time
    from random import randint

    term = logging.StreamHandler()
    _LOGGER.addHandler(term)
    _LOGGER.setLevel(logging.DEBUG)
    _THROW_EXCEPTION = False

    class FakeClient:
        def __init__(self, *args, **kwargs):
            for arg in args:
                _LOGGER.info(f"Client initalized: {arg}")
                self.kind = arg
            _LOGGER.info(f"Client region: {kwargs.get('Region')}")
            self.region = kwargs.get("Region")

        def send(self, str):
            _LOGGER.info(f"Sending with {self.kind}_client: {str}")
            time.sleep(1)  # emulate i/o

    ###  ---- required user code ---- ###

    def func_1(arg, client):
        if randint(0, 100) <= 10 and _THROW_EXCEPTION:
            raise Exception("Foobar:  This exception has stopped all workers in all threads.")
        client.send(arg)
        return arg + "_1"

    def func_2(arg, client):
        client.send(arg)
        return arg + "_2"

    def func_3(arg, client):
        client.send(arg)
        return arg + "_3"

    def test_iterable_initial_args():
        s3_get_objects = [
            "a",
            "b",
            "c",
            "d",
            "e",
            "f",
            "g",
            "h",
            "i",
            "j",
            "k",
            "l",
            "m",
            "n",
            "o",
            "p",
            "q",
            "r",
            "s",
            "t",
            "u",
            "v",
            "w",
            "x",
            "y",
            "z",
        ]
        for item in s3_get_objects:
            yield item

    start_time = time.time()
    finished_q = chain(
        test_iterable_initial_args(),
        first_job=dict(
            target=func_1,
            uninstantiated_client=FakeClient,
            client_args=("s3",),
            client_kwargs={},
            num_workers=10,
        ),
        second_task=dict(
            target=func_2,
            uninstantiated_client=FakeClient,
            client_args=("sqs",),
            client_kwargs={"Region": "us-west-2"},
            num_workers=10,
        ),
        last_job=dict(
            target=func_3,
            uninstantiated_client=FakeClient,
            client_args=("s3",),
            client_kwargs={},
            num_workers=10,
        ),
    )
    _LOGGER.info(f"Finished: {finished_q.qsize()}")
    _LOGGER.info(f"Took: {time.time() - start_time}s")
