# YEXT pipeline
## Deployment
1. Install python 3.8.2
2. Install curl 
```
sudo apt install curl
```
2. install python packages from requirements.txt
3. Create `.env` file in `./src/YEXT/` folder based on `.env_template`
4. Fill `.env` file
5. cd to `./src./YEXT/`
6. Setup crontab: 
```
echo "00 10 12 * * cd /home/azureuser/prod/sourcing-source-pre-processing/src/YEXT/ && python3 run_process.py 2>&1 out.log" >> cron_YEXT && crontab cron_YEXT
```
*Please edit path to folder with run_process.py if needed.

Above commad will add a new cron expression to crontab. To edit ctontab use:
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
```
log is stored in file `./src/YEXT/nohup.out`, to print it use:
```
cat nohup.out
```

## Process description
Scheduler triggers [run_process.py](./src/run_process.py) at the given time. First, script gets list of all the files on supplier's FTP via `get_list_of_files()`, then it filters only YEXT source files via `filter_yext_files()`, if no files found - error will be thrown. 

When all files downloaded pre-processing is triggered via `ProcessPoolExecutor` - this will create a process per file (multiprocessing). Number of processes is limited by a number of CPUs, it can be controlled in [definitions.py](./src/YEXT/definitions.py). In each process a `pipeline_runner` will be triggered. The `pipeline_runner` will create and start pipeline for given file. The pipeline is a chain of generator functions, the order by which filters are executed is controlled in [filters_list.py](./src/YEXT/filters_list.py).

When pre-processing for all files is completed files will be sent to POI loader via `poi_dl_upload()` one by one. In `poi_dl_upload()` it first sends a file via ftp to POI Loader machine, then hit poi loader's automatic api to trigger POI loader delivery process.