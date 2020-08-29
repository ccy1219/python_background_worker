import logging, time
from my_lib.background_worker import work_runner as bg_worker

log_level = logging.DEBUG
# log_level = logging.INFO

log_format = '[%(levelname)-8s][%(asctime)s][%(filename)s %(funcName)s Line:%(lineno)d][%(message).3000s]'
logging.basicConfig(level=log_level, format=log_format)
logger = logging.getLogger(__file__)

def job_function(p1, p2):
    logger.info('job start with params {} {}'.format(p1,p2))
    for i in range(3):
        logger.info('job is processing...')
        time.sleep(1)
    logger.info('job end')
    return p1+p2
    

def dummy_waiting():
    for i in range(1000):
        logger.debug('dummy_waiting...')
        time.sleep(10)

if __name__ == "__main__":
    bg_worker.online()
    for i in range(2):
        job_id1 = bg_worker.do(job_function,'p1','p2')
        logger.info('Wait job until finished')
        bg_worker.wait(job_id1)
        result = bg_worker.get_job_result(job_id1)
        logger.info('job finished.')
        logger.info('Result is job_id:{}, ,is_error:{}, result:{}'.format(
            result['job_id'], result['is_error'], result['result']))
    bg_worker.offline()
    # dummy_waiting()
