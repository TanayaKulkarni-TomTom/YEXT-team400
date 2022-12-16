# Database-USA-CAN Pipeline
## Deployment
1. Install python 3.8.2
2. Install curl 
```
sudo apt install curl
```
3. install python packages from requirements.txt
4. Create .env file in ./src/database_usa_can/ folder based on .env_template
5. Fill .env file
6. cd to ./src/database_usa_can/
7. Setup crontab: 
```
echo "00 14 16 * * cd /home/azureuser/prod/sourcing-source-pre-processing/src/database_usa_can/ && python3 run_process.py 2>&1 out.log" >> cronfile && crontab cronfile
```
*Please edit path to folder with run_process.py if needed.

Above command will add a new cron expression to crontab. To edit ctontab use:
```
crontab -e
```
to see if process runing and get PID:
```
ps ax | grep python
```
to kill the process:
```
kill -9 <PID>



## Process description
Scheduler triggers [run_process.py](./src/database_usa_can/run_process.py) at the given time. 

First Script downloads the files using tomtom's sftp nlftp.tomtomtgroup.com/ site.

When all files downloaded pre-processing is triggered via `ProcessPoolExecutor` - this will create a process per file (multiprocessing). Number of processes is limited by a number of CPUs, it can be controlled in [definitions.py](./src/database_usa_can/definitions.py). In each process a `pipeline_runner` will be triggered. The `pipeline_runner` will create and start pipeline for given file. The pipeline is a chain of generator functions, the order by which filters are executed is controlled in [filters_list.py](./src/database_usa_can/filters_list.py).

When pre-processing for all files is completed files will be sent to POI loader via `poi_dl_upload()` one by one. In `poi_dl_upload()` it first sends a file via ftp to POI Loader machine, then hit poi loader's automatic api to trigger POI loader delivery process.