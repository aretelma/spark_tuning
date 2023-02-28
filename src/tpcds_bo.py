from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import re
import subprocess

from bayes_opt import BayesianOptimization
import time
'''
def load_data(df_path, table_name, file_list, hdfs_path):
    print(table_name)
    p = re.compile(f'{hdfs_path}/{table_name}_[0-9]_4.dat')
    # 지정 폴더의 파일들 중 특정 제목 형식 파일들 찾기
    n_file = 0
    for f in file_list:
        f = f.decode('utf-8')
        if p.match(f):
            print(f)
            n_file += 1
    df = spark_ds.read.options(header="True", delimiter="|").csv(f"{df_path}/header/{table_name}_header.dat") #header부터 로드하기
    for i in range(1, n_file+1):
        df_add = spark_ds.read.options(header="False", delimiter="|").csv(f"{df_path}/{table_name}_{i}_4.dat")
        df = df.union(df_add) #추가 데이터 append
        # print(i, df.count()) #@
    df = df.drop(df.schema[-1].name) #마지막 column은 NULL로 빈 헛 column이기 때문 (끝에 delimiter가 붙어있어서 생기는 문제)
    # df.printSchema() #@
    df.createOrReplaceTempView(f'{table_name}')
    print()
'''
def change_error_string(qfile_content):
    error_string = re.compile("[\"]{1}[\w|\>|\-|\s]+[\"]{1}") # "order count" -> order_count
    found_list = re.findall(error_string, qfile_content)
    revised_found_list = []
    if found_list is not None:
        for f in found_list:
            f = f.replace("\"", "").replace(" ", "_").replace("-", "_").replace(">", "above_")
            revised_found_list.append(f)
        for i in range(len(found_list)):
            qfile_content = qfile_content.replace(found_list[i], revised_found_list[i])
    return qfile_content

def change_date_add(qfile_content):
    date_add = re.compile("\(cast[\s]*\([\']{1}[0-9]+-[0-9]+-[0-9]+[\']{1}[\s]*as[\s]*date\)[\s]*[\+]{1}[\s]*[0-9]+[\s]*days[\)]{1}")
    found_list = re.findall(date_add, qfile_content)
    revised_found_list = []
    if found_list is not None:
        for f in found_list:
            f = f.replace(r"(cast", "date_add(cast").replace("+", ",").replace("days", "")
            revised_found_list.append(f)
        for i in range(len(found_list)):
            qfile_content = qfile_content.replace(found_list[i], revised_found_list[i])
    return qfile_content

def change_date_sub(qfile_content):
    date_sub = re.compile("\(cast[\s]*\([\']{1}[0-9]+-[0-9]+-[0-9]+[\']{1}[\s]*as[\s]*date\)[\s]*[\-]{1}[\s]*[0-9]+[\s]*days[\)]{1}")
    found_list = re.findall(date_sub, qfile_content)
    revised_found_list = []
    if found_list is not None:
        for f in found_list:
            f = f.replace("(cast", "date_sub(cast").replace("-", ",").replace("days", "")
            revised_found_list.append(f)
        for i in range(len(found_list)):
            qfile_content = qfile_content.replace(found_list[i], revised_found_list[i])
    return qfile_content

# memory fraction & memory storage fraction 소수점 1 자리까지 round 

config = { 'bbs' : (1,16), 'd_p' : (100,1000), 'e_c' : (1,8), 'e_i': (2,32), 'e_m' : (2,16), \
            'e_mo' : (0, 8192), 'iczbs' : (16,96), 'iczl' : (1,5), 'k_b' : (32,128), \
            'kbm': (32,128), 'l_w' : (1,6), 'm_f' : (0.5, 0.9), 'msf' : (0.5,0.9), 'mohs' : (0, 4096),\
            'rmsif' : (24, 144), 's_ri' : (1,5), 'ssp' : (100, 1000)
    }


def sparkapp(bbs, d_p, e_c, e_i, e_m, e_mo, iczbs,iczl, k_b, kbm, l_w, m_f, msf, mohs, rmsif, s_ri, ssp):

    conf = SparkConf()
    conf.set("spark.broadcast.blockSize", str(bbs)+'m')
    conf.set('spark.default.parallelism', d_p)
    conf.set('spark.executor.cores', e_c)
    conf.set('spark.executor.instances', e_i)
    conf.set('spark.executor.memory', str(e_m)+'g')
    conf.set('spark.sql.crossJoin.enabled', 'true')
    conf.set('spark.executor.memoryOverhead', str(e_mo)+'m')
    conf.set('spark.io.compression.zstd.bufferSize', str(iczbs)+ 'k')
    conf.set('spark.io.compression.zstd.level', iczl)
    conf.set('spark.kryoserializer.buffer', str(k_b)+'k')
    conf.set('spark.kryoserializer.buffer.max', str(kbm)+'m')
    conf.set('spark.locality.wait', str(l_w)+ 's')
    conf.set('spark.memory.fraction', m_f)
    conf.set('spark.memory.storageFraction', msf)
    conf.set('spark.memory.offHeap.size', str(mohs)+'m')
    conf.set('spark.reducer.maxSizeInFlight', str(rmsif)+'m')
    conf.set('spark.scheduler.revive.interval', str(s_ri)+'s')
    # conf.set('spark.shuffle.file.buffer', conf_param['s_f_b'])
    # conf.set('spark.shuffle.io.numConnectionsPerPeer', conf_param['s_i_nCPP'])
    # conf.set('spark.shuffle.sort.bypassMergeThreshold', conf_param['s_s_bMT'])
    # conf.set('spark.sql.autoBroadcastJoinThreshold', conf_param['s_aBJT'])
    # conf.set('spark.sql.cartesianProductExec.buffer.in.memory.threshold', conf_param['s_cPE_b_i_m_t'])
    # conf.set('spark.sql.codegen.maxFields', conf_param['s_c_mF'])
    # conf.set('spark.sql.inMemoryColumnarStorage.batchSize', conf_param['s_iMCS_b'])
    conf.set('spark.sql.shuffle.partitions', ssp)
    # conf.set('spark.storage.memoryMapThreshold', conf_param['s_mMT'])
    # conf.set('spark.broadcast.compress', conf_param['b_c'])
    # conf.set('spark.memory.offHeap.enabled', conf_param['m_o_e'])
    # conf.set('spark.rdd.compress', conf_param['r_c'])
    # conf.set('spark.shuffle.compress', conf_param['s_c'])
    # conf.set('spark.shuffle.spill.compress', conf_param['s_s_c'])
    # conf.set('spark.sql.codegen.aggregate.map.twolevel.enable', conf_param['s_c_a_m_t_e'])
    # conf.set('spark.sql.inMemoryColumnarStorage.compressed', conf_param['s_iMCS_c'])
    # conf.set('spark.sql.inMemoryColumnarStorage.partitionPruning', conf_param['s_iMCS_pP'])
    # conf.set('spark.sql.join.preferSortMergeJoin', conf_param['s_j_pSMJ'])
    # conf.set('spark.sql.retainGroupColumns', conf_param['s_rGC'])
    # conf.set('spark.sql.sort.enableRadixSort', conf_param['s_s_eRS'])

    spark_ds = SparkSession.builder\
    .appName("Optimization Start")\
    .config(conf=conf)\
    .getOrCreate()
    table_names = ['call_center', 'catalog_page', 'catalog_returns', 'catalog_sales', 'customer',
                        'customer_address', 'customer_demographics', 'date_dim', 'dbgen_version', 'household_demographics',
                        'income_band', 'inventory', 'item', 'promotion', 'reason',
                        'ship_mode', 'store', 'store_returns', 'store_sales', 'time_dim',
                        'warehouse', 'web_page', 'web_returns', 'web_sales', 'web_site']

    df_path = 'hdfs://master2:8020/tpcds' # data file이 들어있는 폴더 경로
    hdfs_path = '/tpcds'

    args = "hdfs dfs -ls "+hdfs_path+" | awk '{print $8}'"

    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output, s_err = proc.communicate()
    file_list = s_output.split()

    # file_list = os.listdir(df_path)
    
    for table_name in table_names:
        p = re.compile(f'{hdfs_path}/{table_name}_[0-9]_4.dat')
        # 지정 폴더의 파일들 중 특정 제목 형식 파일들 찾기
        n_file = 0
        for f in file_list:
            f = f.decode('utf-8')
            if p.match(f):
                n_file += 1
        df = spark_ds.read.options(header="True", delimiter="|").csv(f"{df_path}/header/{table_name}_header.dat") #header부터 로드하기
        for i in range(1, n_file+1):
            df_add = spark_ds.read.options(header="False", delimiter="|").csv(f"{df_path}/{table_name}_{i}_4.dat")
            df = df.union(df_add) #추가 데이터 append
            # print(i, df.count()) #@
        df = df.drop(df.schema[-1].name) #마지막 column은 NULL로 빈 헛 column이기 때문 (끝에 delimiter가 붙어있어서 생기는 문제)
        # df.printSchema() #@
        df.createOrReplaceTempView(f'{table_name}')


    qf_path = 'hdfs://master2:8020/tpcds/query' #query file이 들어있는 폴더 경로
    query_files = spark_ds.sparkContext.wholeTextFiles(f"{qf_path}").collect() #폴더에 있는 모든 sql file 읽어오기

    error_string = re.compile("[\"]{1}[\w|\s]+[\"]{1}") # "order count"

    error_query = []

    querynum = 0
    for i in range(len(query_files)):
        querynum = querynum + 1
        if querynum not in [72,29,14,43,41,99,57,33,69,40,64,50,21,70,95,54,23,15,58,62,20] :
            continue
        try:
            # print(f"query{i+1}")
            qfile_content = query_files[i][1] # file content 가져오기
            # print(query_files[i][0])
                
            # [--dialect NETEZZA] replacing the part of .sql which causes ERROR
            qfile_content = change_error_string(qfile_content) #ex: query16.sql, "order count"->order_count
            qfile_content = change_date_add(qfile_content) #ex: query05.sql, (cast('' as date) +  14 days) -> date_add(cast('' as date), 14) 
            qfile_content = change_date_sub(qfile_content) #ex: query21.sql, (cast ('' as date) - 30 days) -> date_sub(cast('' as date), 30)
                
            queries = qfile_content.strip().split(';') # splitting (;)
            queries = list(filter(None, queries)) #removing empty strings
                
            # action: show() for each sql query
            for q in queries:
                spark_ds.sql(q)
            # print("ERROR QUERY LIST: ", error_query) # printing the name of query files which is not executing properly (for debugging)
            # print()
        except:
            error_query.append(query_files[i][0])
            print("\n Error Found! Moving On to Next Query")


    spark_ds.stop()

def objfunc(bbs, d_p, e_c, e_i, e_m, e_mo, iczbs,iczl, k_b, kbm, l_w, m_f, msf, mohs, rmsif, s_ri, ssp):
    #for x in config:
    #    config[x] = int(config[x])
    bbs = int(bbs)
    d_p = int(d_p)
    e_c = int(e_c)
    e_i = int(e_i)
    e_m = int(e_m)
    e_mo = int(e_mo)
    iczbs = int(iczbs)
    iczl = int(iczl)
    k_b = int(k_b)
    kbm = int(kbm)
    l_w = int(l_w)
    m_f = round(m_f, 1)
    msf = round(msf, 1)
    mohs = int(mohs)
    rmsif = int(rmsif)
    s_ri = int(s_ri)
    ssp = int(ssp)

    start_time = time.time()
    sparkapp(bbs, d_p, e_c, e_i, e_m, e_mo, iczbs,iczl, k_b, kbm, l_w, m_f, msf, mohs, rmsif, s_ri, ssp)
    exetime = time.time() - start_time

    return -1 * exetime

optimizer = BayesianOptimization(
    f = objfunc,
    pbounds = config,
    verbose = 2,
    random_state = 1
)

opt_start = time.time()
optimizer.maximize(
    init_points=2,
    n_iter=3,
)
for i, res in enumerate(optimizer.res):
    print(f'Iteration {i}: \n\t{res}')
print('Final result: ', optimizer.max)

opt_time = time.time()-opt_start
print('Optimization time: ', opt_time)

# print(res)
# {'target' : value, 'params' : {parameter dictionary}}

# import pandas as pd

# resdf = pd.DataFrame



