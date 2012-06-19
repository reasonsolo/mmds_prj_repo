#!/usr/bin/env python
from collections import defaultdict

import os
import sys

RESULT_FILE_PREFIX = "part"

class GroundTruth:
    def __init__(self, filename):
        self.filename = filename
        self.data = defaultdict(int)
        self.cluster_num = 0
        try:
            instream = open(filename, 'r')
            for line in instream:
                datasegment = line.split()
                self.name = datasegment[0].split('.')[0]
                self.data[str(datasegment[1])] += 1
            self.cluster_num = len(self.data)
        except (IndexError):
            print('%s: input file format error' % self.filename)

    def find_max_label(self):
        """
        not used here
        """
        if not self.data:
            return None
        max_label = None
        max_count = 0
        for label, count in self.data.iteritems():
            if count > max_count:
                max_label = label
                max_count = count
        return max_label, max_count

    def get_size(self):
        if not self.data:
            return 0
        return len(self.data)

def list_multiply(data):
    result = 0
    for i, number in enumerate(data):
        for j in xrange(i + 1, len(data)):
            result += data[i] * data[j]
    return result

def get_precision_recall(groundtruths):
    precision = 0
    errors = defaultdict(list)
    total = 0
    total_recall = 0
    for groundtruth in groundtruths:
        for label, count in groundtruth.data.iteritems():
            total += count
            precision += count*(count-1)/2
            errors[label].append(count)
        recall_list = groundtruth.data.values()
        total_recall += list_multiply(recall_list)
    total_error = 0
    for error in errors.values():
        total_error += list_multiply(error)
    print("CORRECT:", precision)
    print("ERROR:  ", total_error)
    print("RECALL: ", total_recall)
    return float(precision) / (total_error + precision),\
                float(precision) / (total_recall + precision)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        folders = sys.argv[1:]
    else:
        sys.exit('Usage: %s [folder1] [folder2] ...' % sys.argv[0])

    for folder in folders:
        files = os.listdir(folder)
        groundtruths = []
        for f in files:
            if not f.startswith(RESULT_FILE_PREFIX):
                continue
            groundtruths.append(GroundTruth(os.path.join(folder,f)))
    pr = get_precision_recall(groundtruths)
    print(pr)
    f = 2/ (1/pr[0] + 1/pr[1])
    print(f)
    print('done')
