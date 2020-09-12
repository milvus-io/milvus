import getopt
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
import numpy as np
import math

PROCESS_NUM = 12
GET_VEC = False
CSV = False
UINT8 = False

BASE_FOLDER_NAME = '/data/milvus/base'
NQ_FOLDER_NAME = '/data/milvus/query'

GT_ALL_FOLDER_NAME = 'ground_truth_all'
GT_FOLDER_NAME = 'ground_truth'
LOC_FILE_NAME = 'ground_truth.txt'
FLOC_FILE_NAME = 'file_ground_truth.txt'
VEC_FILE_NAME = 'vectors.npy'


# get vectors of the files
def load_query_vec(nq, vectors=[], length=0):
    filenames = os.listdir(NQ_FOLDER_NAME)
    filenames.sort()
    for filename in filenames:
        vec_list = load_vec_list(NQ_FOLDER_NAME + '/' + filename)
        length += len(vec_list)
        if nq!=0 and length>nq :
            num = nq % len(vec_list)
            vectors += vec_list[0:num]
            break
        vectors += vec_list
    return vectors


# load vectors from filr_name and num means nq's number
def load_vec_list(file_name):
    if CSV:
        import pandas as pd
        data = pd.read_csv(file_name, header=None)
        data = np.array(data)
    else:
        data = np.load(file_name)
    if UINT8:
        data = (data + 0.5) / 255
    vec_list = data.tolist()
    return vec_list


def hex_to_bin(fp):
    vec=[]
    length = len(fp) * 4
    bstr = str(bin(int(fp,16)))
    bstr = (length-(len(bstr)-2)) * '0' + bstr[2:]
    for f in bstr:
        vec.append(int(f))
    return vec


def calEuclideanDistance(vec1, vec2):
    vec1 = np.array(vec1)
    vec2 = np.array(vec2)
    dist = np.sqrt(np.sum(np.square(vec1 - vec2)))
    return dist


def calInnerDistance(vec1, vec2):
    vec1 = np.array(vec1)
    vec2 = np.array(vec2)
    dist = np.inner(vec1, vec2)
    return dist


def calTanimoto(vec1, vec2):
    vec1 = hex_to_bin(vec1)
    vec2 = hex_to_bin(vec2)
    # print(vec1,vec2)
    nc = float(np.inner(vec1, vec2))
    n1 = float(np.sum(vec1))
    n2 = float(np.sum(vec2))
    dist = nc/(n1+n2-nc)
    print(nc,n1,n2)
    return dist


def get_ground_truth_l2(topk, idx, vct_nq):
    filenames = os.listdir(BASE_FOLDER_NAME)
    filenames.sort()
    no_dist = {}
    k = 0
    for filename in filenames:
        vec_list = load_vec_list(BASE_FOLDER_NAME + '/' + filename)
        for j in range(len(vec_list)):
            dist = calEuclideanDistance(vct_nq, vec_list[j])
            num_j = "%01d%03d%06d" % (8, k, j)
            if k==0 and j<topk :
                no_dist[num_j] = dist
            else:
                # sorted by values
                max_key = max(no_dist, key=no_dist.get)
                max_value = no_dist[max_key]
                if dist < max_value:
                    m = no_dist.pop(max_key)
                    no_dist[num_j] = dist
        k += 1
    no_dist = sorted(no_dist.items(), key=lambda x: x[1])
    print(no_dist)
    save_gt_file(no_dist, idx)


def get_ground_truth_ip(topk, idx, vct_nq):
    filenames = os.listdir(BASE_FOLDER_NAME)  # get the whole file names
    filenames.sort()
    no_dist = {}
    k = 0
    for filename in filenames:
        vec_list = load_vec_list(BASE_FOLDER_NAME + '/' + filename)
        for j in range(len(vec_list)):
            dist = calInnerDistance(vct_nq, vec_list[j])
            num_j = "%03d%06d" % (k, j)
            if k==0 and j<topk :
                no_dist[num_j] = dist
            else:
                min_key = min(no_dist, key=no_dist.get)
                min_value = no_dist[min_key]
                if dist > min_value:
                    m = no_dist.pop(min_key)
                    no_dist[num_j] = dist
        k += 1
    no_dist = sorted(no_dist.items(), key=lambda x: x[1], reverse=True)
    print(no_dist)
    save_gt_file(no_dist, idx)


def get_ground_truth_tanimoto(topk, idx, vec_nq):
    filenames = os.listdir(BASE_FOLDER_NAME)  # get the whole file names
    filenames.sort()
    no_dist = {}
    k = 0
    for filename in filenames:
        vec_list = load_vec_list(BASE_FOLDER_NAME + '/' + filename)
        print(BASE_FOLDER_NAME + '/' + filename, len(vec_list))
        for j in range(len(vec_list)):
            dist = calTanimoto(vec_nq, vec_list[j])
            num_j = "%03d%06d" % (k, j)
            if k==0 and j<topk :
                no_dist[num_j] = dist
            else:
                min_key = min(no_dist, key=no_dist.get)
                min_value = no_dist[min_key]
                if dist > min_value:
                    m = no_dist.pop(min_key)
                    no_dist[num_j] = dist
        k += 1
    no_dist = sorted(no_dist.items(), key=lambda x: x[1], reverse=True)
    print(no_dist)
    save_gt_file(no_dist, idx)


def save_gt_file(no_dist, idx):
    filename = "%05d" % idx + 'results.txt'
    with open(GT_ALL_FOLDER_NAME+'/'+filename, 'w') as f:
        for re in no_dist:
            f.write(str(re[0]) + ' ' + str(re[1]) + '\n')

def get_loc_txt(file):
    filenames = os.listdir(GT_ALL_FOLDER_NAME)
    filenames.sort()
    write_file = open(GT_FOLDER_NAME + '/' + file, 'w+')
    for f in filenames:
        for line in open(GT_ALL_FOLDER_NAME+'/'+f, 'r'):
            write_file.write(line)
        write_file.write('\n')


def get_file_loc_txt(gt_file, fnames_file):
    filenames = os.listdir(BASE_FOLDER_NAME)
    filenames.sort()
    with open(GT_FOLDER_NAME+'/'+gt_file, 'r') as gt_f:
        with open(GT_FOLDER_NAME+'/'+fnames_file, 'w') as fnames_f:
            for line in gt_f:
                if line != '\n':
                    line = line.split()[0]
                    loca = int(line[1:4])
                    offset = int(line[4:10])
                    fnames_f.write(filenames[loca] + ' ' + str(offset + 1) + '\n')
                else:
                    fnames_f.write(line)


def load_gt_file_out():
    file_name = GT_FOLDER_NAME + '/' + FLOC_FILE_NAME
    base_filename = []
    num = []
    with open(file_name, 'r') as f:
        for line in f.readlines():
            data = line.split()
            if data:
                base_filename.append(data[0])
                num.append(data[1])
    return base_filename, num


def ground_truth_process(metric,nq_list, topk, num):
    thread_num = len(nq_list)
    with ProcessPoolExecutor(thread_num) as executor:
        for i in range(thread_num):
            # print("Process:",num+i)
            if metric == 'L2':
                executor.submit(get_ground_truth_l2, topk, num+i, nq_list[i])
            elif metric == 'IP':
                executor.submit(get_ground_truth_ip, topk, num+i, nq_list[i])
            elif metric == 'Tan':
                executor.submit(get_ground_truth_tanimoto, topk, num+i, nq_list[i])
    get_loc_txt(LOC_FILE_NAME)
    get_file_loc_txt(LOC_FILE_NAME, FLOC_FILE_NAME)
    if GET_VEC:
        vec = []
        file, num = load_gt_file_out()
        for i in range(len(file)):
            n = int(num[i]) - 1
            vectors = load_vec_list(BASE_FOLDER_NAME + '/' + file[i])
            vec.append(vectors[n])
        print("saved len of vec:", len(vec))
        np.save(GT_FOLDER_NAME + '/' + VEC_FILE_NAME, vec)


def main():
    try:
        opts, args = getopt.getopt(
            sys.argv[1:],
            "hlq:k:m:",
            ["help", "nq=", "topk=", "metric="],
        )
    except getopt.GetoptError:
        print("Usage: test.py [-q <nq>] -k <topk> -s")
        sys.exit(2)
    nq = 0
    for opt_name, opt_value in opts:
        if opt_name in ("-h", "--help"):
            print("test.py [-q <nq>] -k <topk> -l")
            sys.exit()
        elif opt_name in ("-q", "--nq"):
            nq = int(opt_value)
        elif opt_name in ("-k", "--topk"):
            topk = int(opt_value)
        elif opt_name in ("-m", "--metric"):
            metric = opt_value
        elif opt_name == "-l":    # test.py [-q <nq>] -k <topk> -m -l
            try:
                os.mkdir(GT_ALL_FOLDER_NAME)
            except:
                print('there already exits folder named ' + GT_ALL_FOLDER_NAME + ', please delete it first.')
                sys.exit()
            if not os.path.exists(GT_FOLDER_NAME):
                os.mkdir(GT_FOLDER_NAME)

            print("metric type is",metric)
            time_start = time.time()
            query_vectors = load_query_vec(nq)
            nq = len(query_vectors)
            print("query list:", len(query_vectors))
            num = math.ceil(nq/PROCESS_NUM)
            for i in range(num):
                print("start with round:",i+1)
                if i==num-1:
                    ground_truth_process(metric, query_vectors[i*PROCESS_NUM:nq], topk, i*PROCESS_NUM)
                else:
                    ground_truth_process(metric, query_vectors[i*PROCESS_NUM:i*PROCESS_NUM+PROCESS_NUM], topk, i*PROCESS_NUM)

            time_end = time.time()
            time_cost = time_end - time_start
            print("total_time = ", round(time_cost, 4), "\nGet the ground truth successfully!")


if __name__ == '__main__':
    main()
