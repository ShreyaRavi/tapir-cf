// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/benchmark/retwisClient.cc:
 *   Retwis benchmarking client for a distributed transactional store.
 *
 **********************************************************************/

#include "store/common/truetime.h"
#include "store/common/frontend/client.h"
#include "store/tapirstore/client.h"
#include <algorithm>
#include <queue>
#include <unistd.h>

using namespace std;

// Function to pick a random key according to some distribution.
int rand_key();

bool ready = false;
double alpha = -1;
double *zipf;

vector<string> keys;
vector<string> values;
int nKeys = 100;

int
main(int argc, char **argv)
{
    const char *configPath = NULL;
    int duration = 10;
    int nShards = 1;
    int closestReplica = -1; // Closest replica id.
    int skew = 0; // difference between real clock and TrueTime
    int error = 0; // error bars
    unsigned int valueSize = 64;

    Client *client;
    enum {
        MODE_UNKNOWN,
        MODE_TAPIR,
    } mode = MODE_UNKNOWN;

    bool useCornflakes = false;
    

    int opt;
    while ((opt = getopt(argc, argv, "c:d:N:k:f:m:e:s:z:r:v:l:")) != -1) {
        switch (opt) {
        case 'c': // Configuration path
        { 
            configPath = optarg;
            break;
        }

        case 'f': // Generated keys path
        { 
            break;
        }

        case 'N': // Number of shards.
        { 
            char *strtolPtr;
            nShards = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (nShards <= 0)) {
                fprintf(stderr, "option -N requires a numeric arg\n");
            }
            break;
        }

        case 'd': // Duration in seconds to run.
        { 
            char *strtolPtr;
            duration = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (duration <= 0)) {
                fprintf(stderr, "option -d requires a numeric arg\n");
            }
            break;
        }

        case 'k': // Number of keys to operate on.
        {
            char *strtolPtr;
            nKeys = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (nKeys <= 0)) {
                fprintf(stderr, "option -k requires a numeric arg\n");
            }
            break;
        }

        case 's': // Simulated clock skew.
        {
            char *strtolPtr;
            skew = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || (skew < 0))
            {
                fprintf(stderr,
                        "option -s requires a numeric arg\n");
            }
            break;
        }

        case 'e': // Simulated clock error.
        {
            char *strtolPtr;
            error = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || (error < 0))
            {
                fprintf(stderr,
                        "option -e requires a numeric arg\n");
            }
            break;
        }

        case 'z': // Zipf coefficient for key selection.
        {
            char *strtolPtr;
            alpha = strtod(optarg, &strtolPtr);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr,
                        "option -z requires a numeric arg\n");
            }
            break;
        }

        case 'r': // Preferred closest replica.
        {
            char *strtolPtr;
            closestReplica = strtod(optarg, &strtolPtr);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr,
                        "option -r requires a numeric arg\n");
            }
            break;
        }

        case 'm': // Mode to run in [occ/lock/...]
        {
            if (strcasecmp(optarg, "txn-l") == 0) {
                mode = MODE_TAPIR;
            } else if (strcasecmp(optarg, "txn-s") == 0) {
                mode = MODE_TAPIR;
            } else {
                fprintf(stderr, "unknown mode '%s'\n", optarg);
                exit(0);
            }
            break;
        }
        case 'v':
        {
            useCornflakes = true;
            break;
        }
        case 'l':
        {
            char *strtolPtr;
            valueSize = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr, "option -l requires a numeric arg\n");
            }
            break;  
        }

        default:
            fprintf(stderr, "Unknown argument %s\n", argv[optind]);
            break;
        }
    }

    if (mode == MODE_TAPIR) {
        client = new tapirstore::Client(configPath, nShards,
                    closestReplica, TrueTime(skew, error), useCornflakes);
    } else {
        fprintf(stderr, "option -m is required\n");
        exit(0);
    }

    // Generate keys.
    size_t keySize = 64;

    for (int i = 0; i < nKeys; i++) {
        string keyPrefix = "key_";
        string valuePrefix = "value_";
        string indexStr = std::to_string(i);
        string keyFiller(keySize - keyPrefix.length() - indexStr.length(), 'a');
        string valueFiller(valueSize - valuePrefix.length() - indexStr.length(), 'a');
        keys.push_back(keyPrefix + indexStr + keyFiller);
        values.push_back(valuePrefix + indexStr + valueFiller);
    }

    struct timeval t0, t1, t2;
    int nTransactions = 0; // Number of transactions attempted.
    int ttype; // Transaction type.
    int ret;
    bool status;
    vector<int> keyIdx;

    queue<int> ids;

    gettimeofday(&t0, NULL);
    srand(t0.tv_sec + t0.tv_usec);

    string value;
    while (1) {
        keyIdx.clear();
            
        // Begin a transaction.
        client->Begin();
        gettimeofday(&t1, NULL);
        status = true;

        // Decide which type of retwis transaction it is going to be.
        ttype = rand() % 100;

        if (ttype < 100) {
            keyIdx.push_back(rand_key());
            uint64_t id = client->GetWithID(keys[keyIdx[0]]);
            //printf("get request sent with command id %lu\n", id);
            //string value;
            //int get_status = client->GetStatus(id, value);
            //printf("get status: %d for command id %lu\n", get_status, id);
            //if (get_status > 0) printf("get result: %s\n", value.c_str());
            usleep(10);
            ids.push(id);
            // return an index corresponding to the request id
            // then when the client gets a reply with that id, we know the transaction has finished.
        } else if (ttype < 5) {
            // 5% - Add user transaction. 1,3
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            sort(keyIdx.begin(), keyIdx.end());
            
            if ((ret = client->Get(keys[keyIdx[0]], value))) {
                Warning("Aborting due to %s %d", keys[keyIdx[0]].c_str(), ret);
                status = false;
            }
            
            for (int i = 0; i < 3 && status; i++) {
                client->Put(keys[keyIdx[i]], values[keyIdx[i]]);
            }
            ttype = 1;
        } else if (ttype < 20) {
            // 15% - Follow/Unfollow transaction. 2,2
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            sort(keyIdx.begin(), keyIdx.end());

            for (int i = 0; i < 2 && status; i++) {
                if ((ret = client->Get(keys[keyIdx[i]], value))) {
                    Warning("Aborting due to %s %d", keys[keyIdx[i]].c_str(), ret);
                    status = false;
                }
                client->Put(keys[keyIdx[i]], values[keyIdx[i]]);
            }
            ttype = 2;
        } else if (ttype < 50) {
            // 30% - Post tweet transaction. 3,5
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            sort(keyIdx.begin(), keyIdx.end());

            for (int i = 0; i < 3 && status; i++) {
                if ((ret = client->Get(keys[keyIdx[i]], value))) {
                    Warning("Aborting due to %s %d", keys[keyIdx[i]].c_str(), ret);
                    status = false;
                }
                client->Put(keys[keyIdx[i]], values[keyIdx[i]]);
            }
            for (int i = 0; i < 2; i++) {
                client->Put(keys[keyIdx[i+3]], values[keyIdx[i+3]]);
            }
            ttype = 3;
        } else {
            // 50% - Get followers/timeline transaction. rand(1,10),0
            int nGets = 1 + rand() % 10;
            for (int i = 0; i < nGets; i++) {
                keyIdx.push_back(rand_key());
            }

            sort(keyIdx.begin(), keyIdx.end());
            for (int i = 0; i < nGets && status; i++) {
                if ((ret = client->Get(keys[keyIdx[i]], value))) {
                    Warning("Aborting due to %s %d", keys[keyIdx[i]].c_str(), ret);
                    status = false;
                }
            }
            ttype = 4;
        }

        if (status) {
            // status = client->Commit();
        } else {
            Debug("Aborting transaction due to failed Read");
        }
        gettimeofday(&t2, NULL);
        
        long latency = (t2.tv_sec - t1.tv_sec) * 1000000 + (t2.tv_usec - t1.tv_usec);

        int retries = 0;
        if (!client->Stats().empty()) {
            retries = client->Stats()[0];
        }

        // fprintf(stderr, "%d %ld.%06ld %ld.%06ld %ld %d %d %d", ++nTransactions, t1.tv_sec,
        //         t1.tv_usec, t2.tv_sec, t2.tv_usec, latency, status?1:0, ttype, retries);
        // fprintf(stderr, "\n");
        nTransactions++;

        if (((t2.tv_sec-t0.tv_sec)*1000000 + (t2.tv_usec-t0.tv_usec)) > duration*1000000) {
            printf("number of completed transactions: %d\n", nTransactions);
            break;
        } 
    }

    uint64_t successfulGets = 0;
    while (ids.size() > 0) {
        // pop every queue entry and see what's completed.
        uint64_t command_id = ids.front();
        string value;
        if (client->GetStatus(command_id, value) > 0) {
            successfulGets++;
            printf("cmd_id: %lu, value: %s\n", command_id, value.c_str());
        }
        ids.pop();
    }
    printf("successful gets: %lu\n", successfulGets);

    fprintf(stderr, "# Client exiting..\n");
    return 0;
}

int rand_key()
{
    if (alpha < 0) {
        // Uniform selection of keys.
        return (rand() % nKeys);
    } else {
        // Zipf-like selection of keys.
        if (!ready) {
            zipf = new double[nKeys];

            double c = 0.0;
            for (int i = 1; i <= nKeys; i++) {
                c = c + (1.0 / pow((double) i, alpha));
            }
            c = 1.0 / c;

            double sum = 0.0;
            for (int i = 1; i <= nKeys; i++) {
                sum += (c / pow((double) i, alpha));
                zipf[i-1] = sum;
            }
            ready = true;
        }

        double random = 0.0;
        while (random == 0.0 || random == 1.0) {
            random = (1.0 + rand())/RAND_MAX;
        }

        // binary search to find key;
        int l = 0, r = nKeys, mid;
        while (l < r) {
            mid = (l + r) / 2;
            if (random > zipf[mid]) {
                l = mid + 1;
            } else if (random < zipf[mid]) {
                r = mid - 1;
            } else {
                break;
            }
        }
        return mid;
    } 
}
