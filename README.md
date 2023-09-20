# redis-debug-job
Debug Redis TimeSeries caching performance

### Steps to install and run:

1. pip install -r requirements.txt
2. setup redis-stack-server (locally or using docker)
3. cd csv & unzip files.zip
4. run_celery.sh (to start celery worker)
5. python main.py (to start ingestion tasks)


### write_df performance is bad:

```
[2023-09-19 21:38:00,844: INFO/ForkPoolWorker-3] write_df took 1498.5411 ms
[2023-09-19 21:38:00,844: INFO/ForkPoolWorker-2] write_df took 1500.9809 ms
[2023-09-19 21:38:00,972: INFO/ForkPoolWorker-10] cached [MTDR] (stock)
[2023-09-19 21:38:00,972: INFO/ForkPoolWorker-10] write_df took 640.5663 ms
[2023-09-19 21:38:01,127: INFO/ForkPoolWorker-4] cached [PPL] (stock)
[2023-09-19 21:38:01,128: INFO/ForkPoolWorker-4] write_df took 1527.7519 ms
[2023-09-19 21:38:01,273: INFO/ForkPoolWorker-1] cached [ROKU] (stock)
[2023-09-19 21:38:01,273: INFO/ForkPoolWorker-1] write_df took 943.9244 ms
[2023-09-19 21:38:01,382: INFO/ForkPoolWorker-8] cached [NXT] (stock)
[2023-09-19 21:38:01,382: INFO/ForkPoolWorker-8] write_df took 814.1732 ms
```
