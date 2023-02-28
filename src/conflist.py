'''conf list'''
 # conf.set('spark.driver.cores', )
conf.set('spark.driver.memory', conf_param['d_m'])
conf.set('spark.executor.cores', conf_param['e_c'])
conf.set('spark.executor.instances', conf_param['e_i'])
conf.set('spark.executor.memory', conf_param['e_m'])
conf.set('spark.executor.memoryOverhead', conf_param['e_mO'])
conf.set('spark.io.compression.zstd.bufferSize', conf_param['i_c_z_bS'])
conf.set('spark.io.compression.zstd.level', conf_param['i_c_z_l'])
conf.set('spark.kryoserializer.buffer', conf_param['k_b'])
conf.set('spark.kryoserializer.buffer.max', conf_param['k_b_m'])
conf.set('spark.locality.wait', conf_param['l_w'])
conf.set('spark.memory.fraction', conf_param['m_f'])
conf.set('spark.memory.storageFraction', conf_param['m_sF'])
conf.set('spark.memory.offHeap.size', conf_param['m_oH_s'])
conf.set('spark.reducer.maxSizeInFlight', conf_param['r_mSIF'])
conf.set('spark.scheduler.revive.interval', conf_param['s_r_i'])
conf.set('spark.shuffle.file.buffer', conf_param['s_f_b'])
conf.set('spark.shuffle.io.numConnectionsPerPeer', conf_param['s_i_nCPP'])
conf.set('spark.shuffle.sort.bypassMergeThreshold', conf_param['s_s_bMT'])
conf.set('spark.sql.autoBroadcastJoinThreshold', conf_param['s_aBJT'])
conf.set('spark.sql.cartesianProductExec.buffer.in.memory.threshold', conf_param['s_cPE_b_i_m_t'])
conf.set('spark.sql.codegen.maxFields', conf_param['s_c_mF'])
conf.set('spark.sql.inMemoryColumnarStorage.batchSize', conf_param['s_iMCS_b'])
conf.set('spark.sql.shuffle.partitions', conf_param['s_s_p'])
conf.set('spark.storage.memoryMapThreshold', conf_param['s_mMT'])
conf.set('spark.broadcast.compress', conf_param['b_c'])
conf.set('spark.memory.offHeap.enabled', conf_param['m_o_e'])
conf.set('spark.rdd.compress', conf_param['r_c'])
conf.set('spark.shuffle.compress', conf_param['s_c'])
conf.set('spark.shuffle.spill.compress', conf_param['s_s_c'])
conf.set('spark.sql.codegen.aggregate.map.twolevel.enable', conf_param['s_c_a_m_t_e'])
conf.set('spark.sql.inMemoryColumnarStorage.compressed', conf_param['s_iMCS_c'])
conf.set('spark.sql.inMemoryColumnarStorage.partitionPruning', conf_param['s_iMCS_pP'])
conf.set('spark.sql.join.preferSortMergeJoin', conf_param['s_j_pSMJ'])
conf.set('spark.sql.retainGroupColumns', conf_param['s_rGC'])
conf.set('spark.sql.sort.enableRadixSort', conf_param['s_s_eRS'])

'''
conf_param['b_bS'] = str(r.randint(1, 16))+'m'
conf_param['d_p'] = r.randint(100, 1000)
conf_param['d_c'] = r.randint(1, 16)
conf_param['d_m'] = str(r.randint(4, 48))+'g'
conf_param['e_c'] = r.randint(1, 16)
conf_param['e_i'] = r.randint(9, 112)
conf_param['e_m'] = str(r.randint(4, 48))+'g'
conf_param['e_mO'] = str(r.randint(0, 49152))+'m'
conf_param['i_c_z_bS'] = str(r.randint(16, 96))+'k'
conf_param['i_c_z_l'] = r.randint(1, 5)

conf_param['k_b'] = str(r.randint(32, 128))+'k'
conf_param['k_b_m'] = str(r.randint(32, 128))+'m'
conf_param['l_w'] = str(r.randint(1, 6))+'s'
conf_param['m_f'] = round(r.uniform(0.5, 0.9),  1)
conf_param['m_sF'] = round(r.uniform(0.5, 0.9), 1)
conf_param['m_oH_s'] = str(r.randint(0, 49152))+'m'
conf_param['r_mSIF'] = str(r.randint(24, 144))+'m'
conf_param['s_r_i'] = str(r.randint(1, 5))+'s'
conf_param['s_f_b'] = str(r.randint(16, 96))+'k'
conf_param['s_i_nCPP'] = r.randint(1, 5)

conf_param['s_s_bMT'] = r.randint(100, 400) 
conf_param['s_aBJT'] = 10485760
conf_param['s_cPE_b_i_m_t'] = r.randint(1024, 8192)
conf_param['s_c_mF'] = r.randint(50, 200)
conf_param['s_iMCS_b'] = r.randint(5000, 20000)
conf_param['s_s_p'] = r.randint(100, 1000)
conf_param['s_mMT'] = str(r.randint(1, 10))+'m'
conf_param['b_c'] = r.choice(true_false)

conf_param['m_o_e'] = r.choice(true_false) 
conf_param['r_c'] = r.choice(true_false) 
conf_param['s_c'] = r.choice(true_false) 
conf_param['s_s_c'] = r.choice(true_false) 
conf_param['s_c_a_m_t_e'] = r.choice(true_false) 
conf_param['s_iMCS_c'] = r.choice(true_false) 
conf_param['s_iMCS_pP'] = r.choice(true_false)
conf_param['s_j_pSMJ'] = r.choice(true_false)
conf_param['s_rGC'] = r.choice(true_false) 
conf_param['s_s_eRS'] = r.choice(true_false)    
'''
