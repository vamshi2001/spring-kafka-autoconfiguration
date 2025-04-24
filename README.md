=================================================================================================================================================================
inputs: 
1. target processing time of records (pt_target) = 10,000 records/ min
2. processing time per record per one thread (pt_record) = 5 secs / record => 12 rec/(min * thread)
3. number of threads we can create (nt) ( depends on max number of records we can process parallel and max number of 3rd party connections we can create like DB)= 40
4. application startup time (ast) = 3 mins
5. max msg size (mms) = 1000 charcters
6. max msg in bytes in broker config (mmbbc) = 50MB

assumptions:
1. min application process time (mapt) = 5 mins
2. max time to start consumers (mttsc) = 30 secs
3. number of retries (nr) = 1
4. exponential back off time (ebt) = 5 secs
5. time to recover record (ttrr) = 30 sec
6. percentage of records may fail (prf) = 1/2
7. fetchMaxWaitTime = 10 secs
9. maxIdleConnectionTimeOut = 120000
10. requestTimeout = 30000
11. idleBetweenPolls = 0

calculated values:
1. number of pods/consumers required (np) = pt_target / (nt * pt_record) = 10000 / (40 * 12) => 21
2. min number of partitions required = np  + 2 = 21 +2 => 23
3. maxPollRecords (mpr) = pt_record * mapt * nt = 25 * 5 * 40 = 2400 records
4. maxPollInterval = (mapt * prf * (nr + 1)) + (calculate total exponential backoff time for given number of retries (nr) ) + ttrr = (300 * 1/2 * (1+1)) + (5+ 1) + 30 = 5.6 ~ 6 minutes
5. maxSessionTimeout = ast + mttsc = 3.3 mins
6. heartBeatInterval = maxSessionTimeout/3 = 1.1 min
7. fetchMaxBytes = mpr * 4 = 9,600 bytes
8. fetchMinBytes = mms * 4 = 4000 bytes
9. fetchMaxBytesPerPartition = fetchMaxBytes
10. sendBuffer = if(mmbbc > 131072) 131072 else mmbbc = 131072
11. receiveBuffer = if(mmbbc > 131072) 131072 else mmbbc = 131072
12. pollTimeout = requestTimeout + 10000 = 40000

exponential backof formulla = delay = min(initialInterval * multiplier^retryCount, maxInterval)