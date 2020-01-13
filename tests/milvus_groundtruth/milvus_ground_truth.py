import getopt
import os
import sys
import time
from multiprocessing import Process
import numpy as np

GET_VEC = False
PROCESS_NUM = 5

IP = True
L2 = False
CSV = False
UINT8 = False

BASE_FOLDER_NAME = '/data/milvus/base'
NQ_FOLDER_NAME = '/data/milvus/query'

GT_ALL_FOLDER_NAME = 'ground_truth_all'
GT_FOLDER_NAME = 'ground_truth'
LOC_FILE_NAME = 'location.txt'
FLOC_FILE_NAME = 'file_location.txt'
VEC_FILE_NAME = 'vectors.npy'


# get vectors of the files
def load_nq_vec(nq):
    vectors = []
    length = 0
    filenames = os.listdir(NQ_FOLDER_NAME)
    filenames.sort()
    if nq == 0:
        for filename in filenames:
            vectors += load_vec_list(NQ_FOLDER_NAME + '/' + filename)
        return vectors
    for filename in filenames:
        vec_list = load_vec_list(NQ_FOLDER_NAME + '/' + filename)
        length += len(vec_list)
        if length > nq:
            num = nq % len(vec_list)
            # vec_list = load_vec_list(NQ_FOLDER_NAME + '/' + filename, num)
            vec_list = vec_list[0:num]
        vectors += vec_list
        if len(vectors) == nq:
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
    vec_list = []
    for i in range(len(data)):
        vec_list.append(data[i].tolist())
    return vec_list


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
            if j < topk and k == 0:
                no_dist[num_j] = dist
            else:
                # sorted by values
                max_key = max(no_dist, key=no_dist.get)
                max_value = no_dist[max_key]
                if dist < max_value:
                    m = no_dist.pop(max_key)
                    no_dist[num_j] = dist
        k = k + 1
    no_dist = sorted(no_dist.items(), key=lambda x: x[1])
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
            num_j = "%01d%03d%06d" % (8, k, j)
            if j < topk and k == 0:
                no_dist[num_j] = dist
            else:
                min_key = min(no_dist, key=no_dist.get)
                min_value = no_dist[min_key]
                if dist > min_value:
                    m = no_dist.pop(min_key)
                    no_dist[num_j] = dist
        k = k + 1
    no_dist = sorted(no_dist.items(), key=lambda x: x[1], reverse=True)
    save_gt_file(no_dist, idx)


def save_gt_file(no_dist, idx):
    s = "%05d" % idx
    idx_fname = GT_ALL_FOLDER_NAME + '/' + s + '_idx.txt'
    dis_fname = GT_ALL_FOLDER_NAME + '/' + s + '_dis.txt'
    with open(idx_fname, 'w') as f:
        for re in no_dist:
            f.write(str(re[0]) + '\n')
        f.write('\n')
    with open(dis_fname, 'w') as f:
        for re in no_dist:
            f.write(str(re[1]) + '\n')
        f.write('\n')


def get_loc_txt(file):
    filenames = os.listdir(GT_ALL_FOLDER_NAME)
    filenames.sort()
    write_file = open(GT_FOLDER_NAME + '/' + file, 'w+')
    for f in filenames:
        if f.endswith('_idx.txt'):
            f = GT_ALL_FOLDER_NAME + '/' + f
            for line in open(f, 'r'):
                write_file.write(line)


def get_file_loc_txt(gt_file, fnames_file):
    gt_file = GT_FOLDER_NAME + '/' + gt_file
    fnames_file = GT_FOLDER_NAME + '/' + fnames_file
    filenames = os.listdir(BASE_FOLDER_NAME)
    filenames.sort()
    with open(gt_file, 'r') as gt_f:
        with open(fnames_file, 'w') as fnames_f:
            for line in gt_f:
                if line != '\n':
                    line = line.strip()
                    loca = int(line[1:4])
                    offset = int(line[4:10])
                    fnames_f.write(filenames[loca] + ' ' + str(offset + 1) + '\n')
                else:
                    fnames_f.write('\n')


def load_gt_file_out():
    file_name = GT_FOLDER_NAME + '/' +FLOC_FILE_NAME
    base_filename = []
    num = []
    with open(file_name, 'r') as f:
        for line in f.readlines():
            data = line.split()
            if data:
                base_filename.append(data[0])
                num.append(data[1])
    return base_filename, num


def ground_truth_process(nq, topk):
    try:
        os.mkdir(GT_ALL_FOLDER_NAME)
    except:
        print('there already exits folder named ' + GT_ALL_FOLDER_NAME + ', please delete it first.')
    else:
        vectors = load_nq_vec(nq)
        print("query list:", len(vectors))
        processes = []
        process_num = PROCESS_NUM
        nq = len(vectors)
        loops = nq // process_num
        rest = nq % process_num
        if rest != 0:
            loops += 1
        time_start = time.time()
        for loop in range(loops):
            time1_start = time.time()
            base = loop * process_num
            if rest != 0 and loop == loops - 1:
                process_num = rest
            print('base:', loop)
            for i in range(process_num):
                print('nq_index:', base + i)
                if L2:
                    if base + i == 0:
                        print("get ground truth by L2.")
                    process = Process(target=get_ground_truth_l2,
                                      args=(topk, base + i, vectors[base + i]))
                elif IP:
                    if base + i == 0:
                        print("get ground truth by IP.")
                    process = Process(target=get_ground_truth_ip,
                                      args=(topk, base + i, vectors[base + i]))
                processes.append(process)
                process.start()
            for p in processes:
                p.join()
            time1_end = time.time()
            print("base", loop, "time_cost = ", round(time1_end - time1_start, 4))
        if not os.path.exists(GT_FOLDER_NAME):
            os.mkdir(GT_FOLDER_NAME)
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
        time_end = time.time()
        time_cost = time_end - time_start
        print("total_time = ", round(time_cost, 4), "\nGet the ground truth successfully!")


def main():
    try:
        opts, args = getopt.getopt(
            sys.argv[1:],
            "hlq:k:",
            ["help", "nq=", "topk="],
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
        elif opt_name == "-l":
            ground_truth_process(nq, topk)  # test.py [-q <nq>] -k <topk> -l
            sys.exit()


if __name__ == '__main__':
    main()
