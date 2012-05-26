import os
import sys

if __name__ == '__main__':
    if len(sys.argv) > 1:
        path = sys.argv[1]
    else:
        path = "./"
    dir_list = os.listdir(path)
    dir_num = len(dir_list)
    if dir_num == 0:
        sys.exit("No directories available")
    file_list = os.listdir('/'.join([path, dir_list[0]]))
    file_num = len(file_list)
    if len(sys.argv) > 2:
        max_file = int(sys.argv[2])
    else:
        max_file = file_num
    line_num = 0
    for f in file_list[:max_file]:
        instreams = []
        outstream = open(f, 'w')
        for d in dir_list:
            instreams.append(open(path + d + '/' + f, 'r'))
        for line in instreams[0]:
            for index in xrange(len(instreams)):
                if index == 0:
                    pass
                else:
                    line = instreams[index].readline()
                outstream.write(line.replace('\n', '').replace('\r', ''))
                outstream.write('\t')
            outstream.write('\n')






