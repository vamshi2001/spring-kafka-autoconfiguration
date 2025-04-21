1. target processing time of records (pt_target) = 10,000 records/ min => 167 recs /sec
2. processing time per record (pt_record) = 5 secs / record => (1/5) rec/sec  /thread => 12 rec/min
3. number of threads we can create (nt) = 40
4. total records to process (tr) = pt_target / pt_record = 835 
5. number of records we can process per application (nr_application_sec) = nt * pt_record = 8 or 480 per min
6. number of pods (np)= pt_target / nr_application_sec = 21



max number of records to poll (mr_poll) = nt * 

worst case -> total n records failed and for retries and logging/recover failed records it took k secs

total poll interval = N * K <= session timeout
=> (N * (pt_record) * number retries ) + (ideal time between retries * number retries) + (time for logging/recover record) + delta 5 sec < session timeout