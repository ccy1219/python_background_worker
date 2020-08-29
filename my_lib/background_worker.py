import time
from datetime import datetime
import threading, queue
import functools
import logging

"""
一個簡單概念的 back ground worker，
可以把job放到背景thread執行，並可以等待完成、取得結果等功能。
此module只是一個單純練習範例，也還在測試階段，尚有一些情況不考慮，如Queue空間大小等等。
為避免其他問題，請避免直接new物件出來，用以下範例來操作。

流程:
0. import work_runner
1. do()
2. (optional) wait()
3. (optional) get_job_result()
4. (optional) offline()

範例:
from my_lib.background_worker import work_runner as bg_worker

bg_worker.online()

# 把job放入queue中，等待執行(非同步操作)，返回 job_id
job_id = bg_worker.do(job_function,'p1','p2'))

# 等待job完成 (類似 thread join)
bg_worker.wait(job_id)

# 透過 job_id 取得job執行結果
result = bg_worker.get_job_result(job_id)

# 印出工作結果
logger.info('Result is job_id:{}, ,is_error:{}, result:{}'.format(
    result['job_id'], result['is_error'], result['result']))

# 通常希望背景的worker持續執行，所以通常不需要做此 offline
bg_worker.offline()
"""

logger = logging.getLogger(__file__)

class BackgroundWorker(object):
    _instance = None
    _lock = threading.Lock()
    JOB_RESULT_KEY_JOB_ID='job_id'
    JOB_RESULT_KEY_RESULT='result'
    JOB_RESULT_KEY_IS_ERROR='is_error'
    JOB_RESULT_NOT_FOUND = 'JOB_NOT_FOUND'

    @staticmethod
    def get_instance(error_exit_first):
        # Double-checked locking
        if BackgroundWorker._instance is None:
            with BackgroundWorker._lock:
                if BackgroundWorker._instance is None:
                    BackgroundWorker._instance = BackgroundWorker(error_exit_first)
                return BackgroundWorker._instance

    def __init__(self, error_exit_first):
        '''
        參數:
          若 error_exit_first 為 True 時，若發生例外，thread會直接停止
        '''
        self._error_exit_first = error_exit_first
        self._job_queue = queue.Queue()
        self._job_ids = set()        
        self._job_results = queue.Queue(maxsize=10)
        self._condition_lock = threading.Condition()
        self._stop_event = threading.Event()

        background_worker_thread = threading.Thread(target=self._background_worker, name='background_worker_thread', args=(self._stop_event,))
        background_worker_thread.start()

    def stop_bg_daemon(self):
        self._stop_event.set()
        logger.info('BackgroundWorker stop event is sent')

    def _background_worker(self, stop_event):
        '''
        此method將被一個在背景執行的thread所運行
        也是實際執行job的地方
        '''
        # time.sleep(3)
        logger.info('BackgroundWorker queue worker is ready')
        while True:
            is_error = False
            result = None            
            try:
                # 從_job_queue取出job，並執行該job
                job_id, job_func = self._job_queue.get(timeout=1)
            except queue.Empty as e:
                # 若被要求offline，且當_job_queue都已經做完時，跳出迴圈讓此thread結束
                if self._stop_event.isSet():
                    break
                else:
                    continue
            
            try:
                logger.debug('BackgroundWorker starts doing job. job_func: {} job_id: {}'.format(job_func.__name__, job_id))
                result = job_func()
                logger.debug('BackgroundWorker finished doing job. job_func: {} job_id: {}'.format(job_func.__name__, job_id))
            except Exception as e:
                if self._error_exit_first:
                    raise                
                is_error = True
                logger.error('BackgroundWorker job error. Job job_id: {} Error: {}'.format(job_id, str(e)))
                result = e

            self._job_queue.task_done()

            with self._condition_lock:
                self._job_ids.remove(job_id)
                if self._job_results.full():
                    discard_result = self._job_results.get()
                try:
                    self._job_results.put({
                        BackgroundWorker.JOB_RESULT_KEY_JOB_ID : job_id, 
                        BackgroundWorker.JOB_RESULT_KEY_RESULT : result, 
                        BackgroundWorker.JOB_RESULT_KEY_IS_ERROR : is_error
                        }, block=False)
                except queue.Full as e:
                    logger.error('Unexpected result! _job_results should never be FILL! job id: {}'.format(job_id))
                self._condition_lock.notify_all()
            # time.sleep(0.5)
        with BackgroundWorker._lock:
            logger.info('BackgroundWorker is being offline')
            BackgroundWorker._instance = None

    @property
    def pending_job_cnt(self):
        return self._job_queue.qsize()

    @property
    def is_online(self):
        with BackgroundWorker._lock:
            if BackgroundWorker._instance is None:
                return False
            else:
                return True


    def do(self, job_func, *args, **kwargs):
        '''
        1. 呼叫此方法將job_func放入_job_queue中
        '''
        # 先將job與參數合成一個物件
        job_func_partial = functools.partial(job_func, *args, **kwargs)
        try:
            functools.update_wrapper(job_func_partial, job_func)
        except AttributeError:
            # job_funcs already wrapped by functools.partial won't have
            # __name__, __module__ or __doc__ and the update_wrapper()
            # call will fail.
            pass
        
        # sleep for a while in case job_id collision
        time.sleep(0.1)
        job_id = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
        logger.debug('BackgroundWorker enqueue job {} args {} kwargs {} job_id {}'.format(job_func, args, kwargs, job_id))
        # 將job讓入_job_queue，準備執行
        with self._condition_lock:
            self._job_queue.put((job_id, job_func_partial))
            self._job_ids.add(job_id)
        return job_id

    def wait(self, job_id):
        '''
        2. (optional)一直等待直到該job執行結束
        Wait job done by job_id you get from do()
        '''
        if job_id not in self._job_ids:
            return            
        while True:
            with self._condition_lock:
                self._condition_lock.wait()
                if job_id not in self._job_ids:
                    return

    def get_job_result(self, job_id): 
        '''
        3. (optional)當job執行結束後，可透過此方法拿到job執行結果
        可透過key JOB_RESULT_KEY_IS_ERROR 來判斷job是否成功
        可透過key JOB_RESULT_KEY_RESULT 來拿到job結果
        '''       
        result_list = list(self._job_results.queue)
        job = [j for j in result_list if j['job_id']==job_id]
        if len(job)>0:
            return job[0]
        else:
            logger.warning('BackgroundWorker cannot find job result: {}'.format(job_id))
            err_result = {
                BackgroundWorker.JOB_RESULT_KEY_JOB_ID : job_id,
                BackgroundWorker.JOB_RESULT_KEY_RESULT : BackgroundWorker.JOB_RESULT_NOT_FOUND,
                BackgroundWorker.JOB_RESULT_KEY_IS_ERROR : False
                }
            return err_result


class IBackgroundWorker:
    def __init__(self, error_exit_first=True):
        '''
        此 decorator 用來標示/限制 操作只能在online狀態下執行
        '''
        self._online = False
        self._bg_worker = None
        self._error_exit_first = error_exit_first

    def online_only(f):
        '''
        此 decorator 用來標示/限制 操作只能在online狀態下執行
        '''
        @functools.wraps(f)
        def inner_func(*args, **kwargs):
            bg_instance = args[0]
            if bg_instance._online:
                return f(*args, **kwargs)
            else:
                logger.warning('Warning! This operation is running on online status only, so do nothing here.')
        return inner_func

    def online(self):
        '''
        online這個動作會在背景起一個BackgroundWorker thread，
        隨時等待工作指派並執行
        '''
        self._bg_worker = BackgroundWorker.get_instance(self._error_exit_first)
        self._online = True
    
    @online_only
    def offline(self):
        '''
        通常希望背景的worker持續執行，所以通常不需要做此 offline，
        除非真的確定不需要，此動作會把back ground thread 安全的停止
        '''
        self._bg_worker.stop_bg_daemon()
        while self._bg_worker.is_online:
            # 暫時的作法，busy waiting
            time.sleep(1)            
        self._online = False

    @property
    def is_online(self):
        return self._online

    @online_only
    def do(self, job_func, *args, **kwargs):
        return self._bg_worker.do(job_func, *args, **kwargs)

    @online_only
    def wait(self, job_func, *args, **kwargs):
        return self._bg_worker.wait(job_func, *args, **kwargs)

    @online_only
    def get_job_result(self, job_func, *args, **kwargs):
        return self._bg_worker.get_job_result(job_func, *args, **kwargs)

# 外部只需要使用 work_runner 這個變數，請勿直接使用本module之外的物件，
work_runner = IBackgroundWorker()
