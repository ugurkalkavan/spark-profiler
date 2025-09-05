import os
import pandas as pd 
import requests
import re

os.environ['http_proxy']=""

class Analyzer:

    def __init__(self, tasks=None, stages=None, sql=None):
        self.stages = stages
        self.tasks = tasks
        self.sql = sql

    # Analysis for Task related metrics
    def task_analyze(self):
        task_data = self.stages[['jobId', 'description', 'stageId', 'numTasks', 'numActiveTasks', 'numCompleteTasks', 'numFailedTasks', 'numKilledTasks', 'numCompletedIndices']]
        return task_data

    # Analysis for Spill related metrics (Memory and Disk)
    def spill_analyze(self):
        spill_data = self.stages[['jobId', 'description', 'stageId', 'memoryBytesSpilled', 'diskBytesSpilled']]
        return spill_data

    # Analysis for Time-related metrics 
    def time_analyze(self):
        time_data = self.stages[['jobId', 'description', 'stageId', 'executorDeserializeTime','executorDeserializeCpuTime', 'executorRunTime', 'jvmGcTime', 'resultSerializationTime']]
        return time_data

    # Analysis for Shuffle read metrics
    def shuffle_read_analyze(self):
        shuffle_read_data = self.stages[['jobId', 'description', 'stageId', 'shuffleRemoteBlocksFetched', 'shuffleLocalBlocksFetched', 'shuffleFetchWaitTime', 'shuffleRemoteBytesRead', 'shuffleRemoteBytesReadToDisk', 'shuffleLocalBytesRead']]
        return shuffle_read_data

    # Analysis for Shuffle write metrics
    def shuffle_write_analyze(self):
        shuffle_write_data = self.stages[['jobId', 'description', 'stageId', 'shuffleWriteBytes', 'shuffleWriteTime', 'shuffleWriteRecords']]
        return shuffle_write_data

    # Analysis for Input and Output metrics
    def inout_analyze(self):
        inout_data = self.stages[['jobId', 'description', 'stageId', 'inputBytes', 'inputRecords', 'outputBytes', 'outputRecords']]
        return inout_data


    def critical_path(self):
        critical_path_time = 0
        
        return critical_path_time

    
    def sql_analyze(self):
        # Your SQL analyze code here 
        # Return statement as above
        pass


class Stage():
    def __init__(self, stages):
        self.stages = stages
        self.analyzer = Analyzer(self.stages)
        
    def __repr__(self):
        return "display() will give you the stage info"

    @staticmethod
    def is_positive(v):
        return 'color: red' if v > 0 else None

    @staticmethod   
    def style_dataframe(df, subset):
        df = df.style.format(precision=3, thousands=".", decimal=",") \
               .map(Stage.is_positive, subset=subset)
        return df
    
    
    def analyze(self):
        print('\nTask Analysis:')
        task_df = self.analyzer.task_analyze()
        styled_df = Stage.style_dataframe(task_df, ['numFailedTasks'])
        display(styled_df)
     
        print('\nSpill Analysis:')
        spill_df = self.analyzer.spill_analyze()
        styled_df = Stage.style_dataframe(spill_df, ['memoryBytesSpilled', 'diskBytesSpilled'])
        display(styled_df)  

    
        print('\nTime-related Analysis:')
        display(self.analyzer.time_analyze())
    
        print('\nShuffle Read Analysis:')
        display(self.analyzer.shuffle_read_analyze())
    
        print('\nShuffle Write Analysis:')
        display(self.analyzer.shuffle_write_analyze())
    
        print('\nInput and Output Analysis:')
        display(self.analyzer.inout_analyze())
    
    def display(self): 
        return self.stages

    
class Job():
    def __init__(self, jobs):
        self.jobs = jobs
        
    def __repr__(self):
        return "display() will give you the job info"
    
    def display(self): 
        return self.jobs

class SQL(Analyzer):
    def __init__(self, sql):
        self.sql = sql
        self.analyzer = Analyzer(self.sql)

    def analyze(self):
        print('\nSQL Analysis:')
        display(self.analyzer.sql_analyze())
        
    def __repr__(self):
        return "display() will give you the job info"
    
    def display(self): 
        return self.sql
    
    
class SparkPyRest(object): 
    """Basic class for handling Spark REST API queries"""
    
    def __repr__(self):
        return """
        -- app will give you the app id
        -- Stage will give you the stage info
        -- Job will give you the job info
        -- Tasks(stageId) will give you the task info
        -- sql will give you the sql info"""
        
    def __init__(self, host, port=4040): 
        self.host = host
        self.port = port
        self.base_url = base_url = 'http://{host}:{port}/api/v1'.format(host=host, port=port)

    @property
    def app(self): 
        return get_app(self.base_url)

    @property
    def jobs(self): 
        jobs_data = get_jobs(self.base_url)
        return Job(jobs_data)

    @property
    def stages(self): 
        stages_data = merge_jobs_stages(self.base_url)
        return Stage(stages_data) 

    @property
    def tasks(self):
        """Return all the tasks"""
        df_all_tasks = get_all_tasks(self.base_url)

        return df_all_tasks

    
    @property
    def sql(self): 
        sql_data = get_sql(self.base_url)
        return SQL(sql_data)
    
    @property
    def executors(self):
        response = requests.get(self.base_url+'/applications/' + self.app + '/executors')
        executors = response.json()
        return executors


    


    
    def examine(self):
        df_all_tasks = get_all_tasks(self.base_url)
        analyzer = Analyzer(tasks = df_all_tasks)
        crt = analyzer.critical_path()
        return crt

    #def tasks(self, stageid):
    #    """Return the tasks for a given stageid"""
    #    return get_tasks(self.base_url, stageid)


    def executor_log_bytes(self, executor_id):
        """Find out how big the executor log file is"""
        executors = self.executors

        for executor in executors: 
            if executor['id'] == str(executor_id):
               break

        log_url = executors[executor_id]
        response = requests.get(executor['executorLogs']['stderr'].replace('logPage', 'log')+'&offset=0')
        total_bytes = int(re.findall('\d+\sof\s(\d+)\sof', response.text)[0])
        return total_bytes        


    def executor_log(self, executor_id, nbytes='all'):
        """Fetch the executors log; by default, the entire log is returned."""
        total_bytes = self.executor_log_bytes(executor_id)

        executors = self.executors

        for executor in executors: 
            if executor['id'] == str(executor_id):
               break

        if nbytes=='all':
            nbytes = total_bytes

        else: 
            if not isinstance(nbytes,int): 
                raise RuntimeError('nbytes must be an integer')

        query = executor['executorLogs']['stderr'].replace('logPage', 'log')
        query += '&offset=0'
        query += '&byteLength={}'.format(nbytes)
        response = requests.get(query)

        return response.text


######## base public functions

def get_app(base_url: str) -> str:
    """Get the app ID from the REST server"""
    response = requests.get(base_url+'/applications')
    return response.json()[0]['id']


def get_all_tasks(base_url: str) -> pd.DataFrame:
    stages_data = merge_jobs_stages(base_url)

    df_all_tasks = pd.DataFrame()
    for stageId in stages_data["stageId"]:

        df_tasks = get_tasks(base_url, stageid=stageId)
        df_all_tasks = pd.concat([df_all_tasks, df_tasks], ignore_index=True)

    #display(stages_data)
    #display(df_all_tasks)

    # Select only the necessary columns
    stages_data = stages_data[["jobId", "description", "stageId"]]
    # merge jobs and stages on stageId
    merged_df = pd.merge(stages_data, df_all_tasks, left_on='stageId', right_on='stageId', how='inner')
    
    
    return merged_df
        
def get_sql(base_url: str) -> pd.DataFrame:
    """Get SQL data"""
    #response = requests.get(base_url+'/applications/'+get_app(base_url)+'/sql')
    #sql_data = response.json()
    #print(base_url+'/applications/'+get_app(base_url)+'/sql')
    print()
    #print(response.text)
    #df_sql = pd.DataFrame(sql_data, columns=['nodeName', 'nodeMetrics'])
    

    fields = ['id', 'submissionTime', 'duration', 'runningJobIds', 'successJobIds', 'failedJobIds']
                  
    nested_fields = [ 'nodeId', 'nodeName' ]
    
    
    response = requests.get(base_url+'/applications/'+get_app(base_url)+'/sql/')
    print(base_url+'/applications/'+get_app(base_url)+'/sql/')
    j = response.json()
    
    data = []
    for item in j:
        if 'nodes' in item:
            for node in item['nodes']:
                node_data = _recurse_dict(node, nested_fields)
                if 'metrics' in node:
                    for metric in node['metrics']:
                        res = _recurse_dict(item, fields)
                        res.update(node_data)
                        res.update(metric)
                        data.append(res)
                else:
                    # Handles the case if there is no 'metrics' key in the 'node'
                    res = _recurse_dict(item, fields)
                    res.update(node_data)
                    data.append(res)
        else:
            # Handles the case if there is no 'nodes' key in 'item'
            res = _recurse_dict(item, fields)
            data.append(res)

    df = pd.DataFrame(data)

    return df
    
    
    
    
    #return df_sql



def get_jobs(base_url: str) -> pd.DataFrame:
    """Get job IDs"""
    response = requests.get(base_url+'/applications/'+get_app(base_url)+'/jobs')
    jobs = response.json()
    df_jobs = pd.DataFrame(jobs, columns=['jobId', 'name', 'description', 'stageIds'])
    #return [(job['jobId'], job['name'], job['description'], job['stageIds']) for job in jobs]
    return df_jobs


def merge_jobs_stages(base_url: str) -> pd.DataFrame:
    """Merge jobs and stages"""
    stages_df = get_stages(base_url)
    jobs_df = get_jobs(base_url)
    
    #drop name column from jobs_df
    jobs_df = jobs_df.drop('name', axis=1)

    # explode the stageIds list into separate rows
    jobs_df_exploded = jobs_df.explode('stageIds')

    # merge jobs and stages on stageId
    merged_df = pd.merge(jobs_df_exploded, stages_df, left_on='stageIds', right_on='stageId', how='inner')
    
    # drop the columns 'name_y' and 'stageId'
    merged_df = merged_df.drop('stageIds', axis=1)
    

    return merged_df



def get_stages_v1(base_url: str) -> pd.DataFrame:
    """Get stage IDs"""
    
    fields = ['status', 'stageId', 'attemptId', 
              'numTasks', 'numActiveTasks', 'numCompleteTasks', 'numFailedTasks', 'numKilledTasks', 'numCompletedIndices',
              'executorDeserializeTime', 'executorDeserializeCpuTime', 'executorRunTime', 'executorCpuTime',
              'resultSize', 'jvmGcTime', 'resultSerializationTime', 
              'memoryBytesSpilled', 'diskBytesSpilled', 'peakExecutionMemory', 
              'inputBytes', 'inputRecords', 'outputBytes', 'outputRecords', 
              'shuffleRemoteBlocksFetched', 'shuffleLocalBlocksFetched', 'shuffleFetchWaitTime', 'shuffleRemoteBytesRead', 'shuffleRemoteBytesReadToDisk', 'shuffleLocalBytesRead', 'shuffleReadBytes', 'shuffleReadRecords', 'shuffleCorruptMergedBlockChunks', 'shuffleMergedFetchFallbackCount', 'shuffleMergedRemoteBlocksFetched', 'shuffleMergedLocalBlocksFetched', 'shuffleMergedRemoteChunksFetched', 'shuffleMergedLocalChunksFetched', 'shuffleMergedRemoteBytesRead', 'shuffleMergedLocalBytesRead', 'shuffleRemoteReqsDuration', 'shuffleMergedRemoteReqsDuration', 'shuffleWriteBytes', 'shuffleWriteTime', 'shuffleWriteRecords']
    
    response = requests.get(base_url+'/applications/'+get_app(base_url)+'/stages?withSummaries=true')
    stages = response.json()
    
    #return [(stage['stageId'], stage['name']) for stage in stages] # return list
    df_stages = pd.DataFrame(stages, columns=fields)
    return df_stages



def get_stages(base_url: str) -> pd.DataFrame:
    """Get stage IDs"""
    
    fields = ['status', 'stageId', 'attemptId', 'numTasks', 'numActiveTasks', 'numCompleteTasks', 'numFailedTasks', 'numKilledTasks', 'numCompletedIndices',
             'executorDeserializeTime', 'executorDeserializeCpuTime', 'executorRunTime', 'executorCpuTime',
             'resultSize', 'jvmGcTime', 'resultSerializationTime', 
             'memoryBytesSpilled', 'diskBytesSpilled', 'peakExecutionMemory', 
             'inputBytes', 'inputRecords', 'outputBytes', 'outputRecords', ]

    nested_fields = ['quantiles', 'duration', 'executorDeserializeTime', 'executorDeserializeCpuTime', 'executorRunTime', 'executorCpuTime', 'resultSize', 'jvmGcTime', 'resultSerializationTime', 'gettingResultTime', 'schedulerDelay', 'peakExecutionMemory', 'memoryBytesSpilled', 'diskBytesSpilled', 'bytesRead', 'recordsRead', 'bytesWritten', 'recordsWritten', 
                     'readBytes', 'readRecords','writeBytes','writeRecords','writeTime']

    response = requests.get(base_url+'/applications/'+get_app(base_url)+'/stages?withSummaries=true')
    stages = response.json()

    data = []
    for item in stages:
        res = _recurse_dict(item, fields)
        res.update(_recurse_dict(item, nested_fields))
        data.append(res)

    df_stages = pd.DataFrame(data)
    return df_stages


def get_tasks_v1(base_url: str, stageid: int) -> list:
    """Produce a DataFrame of task metrics for a stage or list of stages"""

    fields = ['taskId', 'host', 'executorId', 'executorRunTime', 
                  'localBytesRead', 'remoteBytesRead', 'bytesWritten']

    fields = [
    "status","stageId","numTasks","taskId", "index", "attempt", "partitionId", "launchTime", "duration",
    "executorId", "host", "status", "taskLocality", "speculative", "accumulatorUpdates",
    "executorDeserializeTime", "executorDeserializeCpuTime",
    "executorRunTime", "executorCpuTime", "resultSize", "jvmGcTime", "resultSerializationTime",
    "memoryBytesSpilled", "diskBytesSpilled", "peakExecutionMemory", "bytesRead", "recordsRead", "bytesWritten",
    "recordsWritten", "remoteBlocksFetched", "localBlocksFetched", "fetchWaitTime",
    "remoteBytesRead", "localBytesRead", "remoteMergedBytesRead", "localMergedBytesRead", 
    "remoteReqsDuration", "remoteMergedBlocksFetched","localMergedBlocksFetched", "remoteMergedChunksFetched", "localMergedChunksFetched", "remoteMergedBytesRead",
    "remoteMergedReqsDuration", "schedulerDelay", "gettingResultTime"
]


    if isinstance(stageid,int): 
        response = requests.get(base_url+'/applications/'+get_app(base_url)+'/stages/'+str(stageid))
        print(base_url+'/applications/'+get_app(base_url)+'/stages/'+str(stageid))
        j = response.json()

        res = [_recurse_dict(task,fields) for task in j[0]['tasks'].values()]
        data = _recurse_dict(j[0], fields)
        res = pd.DataFrame(data)

    return res



def get_tasks(base_url: str, stageid: int) -> pd.DataFrame:
    """Produce a DataFrame of task metrics for a stage or list of stages"""

    fields = ["stageId", "firstTaskLaunchedTime", "completionTime"]

    nested_fields = [
        "taskId", "index", "attempt", "partitionId", "launchTime", "duration",
        "executorId", "host", "status", "taskLocality", "speculative", "accumulatorUpdates",
        "executorDeserializeTime", "executorDeserializeCpuTime",
        "executorRunTime", "executorCpuTime", "resultSize", "jvmGcTime", "resultSerializationTime",
        "memoryBytesSpilled", "diskBytesSpilled", "peakExecutionMemory", "bytesRead", "recordsRead", "bytesWritten",
        "recordsWritten", "remoteBlocksFetched", "localBlocksFetched", "fetchWaitTime",
        "remoteBytesRead", "localBytesRead", "remoteMergedBytesRead", "localMergedBytesRead", 
        "remoteReqsDuration", "remoteMergedBlocksFetched","localMergedBlocksFetched", "remoteMergedChunksFetched", "localMergedChunksFetched", "remoteMergedBytesRead",
        "remoteMergedReqsDuration", "schedulerDelay", "gettingResultTime"
    ]

    if isinstance(stageid,int): 
        
        response = requests.get(f'{base_url}/applications/{get_app(base_url)}/stages/{stageid}')
        
        stage = response.json()
        
        
        initial_res = _recurse_dict(stage[0], fields) 

        data = []
        for task in stage[0]['tasks'].values():
            res = initial_res.copy()  # Create a new copy of the initial result for each task
            task_info = _recurse_dict(task, nested_fields)

            res.update(task_info)
            data.append(res)  # Append the result for this task to the data list

        df = pd.DataFrame(data)  # Create the DataFrame from the list of task data
    return df



#def _recurse_dict(d, fields):
#    """Traverse a dictionary recursively looking for keys matching the list of keys in 'fields'"""
#    task_metrics = {}
#    for k,v in d.items():
#        if k in fields: task_metrics[k] = v
#        if isinstance(v,dict):
#            new_dict = _recurse_dict(v,fields)
#            if new_dict is not None: 
#                task_metrics.update(new_dict)
#    return task_metrics



def _recurse_dict(d, fields, parent_key=""):
    """Traverse a dictionary recursively looking for keys matching the list of keys in 'fields'"""
    task_metrics = {}
    for k, v in d.items():
        new_key = f"{parent_key}.{k}" if parent_key else k
        if k in fields: 
            task_metrics[new_key] = v
        if isinstance(v, dict):
            new_dict = _recurse_dict(v, fields, new_key)
            if new_dict is not None: 
                task_metrics.update(new_dict)
    return task_metrics


