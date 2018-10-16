# -*- coding: utf-8 -*

import mrs
import glob
import sys
import re


class WordCount(mrs.MapReduce):

    def input_data(self, job):
        repo = self.args[0]
        print(repo)
        fileList = glob.glob(repo + '/*.csv')
        print(fileList)
        print('\n ----------------------------------------------------------------- \n')
        return job.file_data(fileList)

    def map(self, key, value):

        #print(value.encode('utf-8'))
        # print('\n ----------------------------------------------------------------- \n')
        enc = value.split(',')
        # print(enc[0])
        imc = 0
        id = '"id"'
        if enc[0] != id and int(enc[1]) >= 18:
            imc = (float(enc[3]) / float(enc[2]) ** 2) * 10000
            #print(imc)

        if imc != 0:
            yield enc[4], imc

    def reduce(self, key, value):
        minimc = 200
        maximc, buffer, count = 0, 0, 0
        for imc in tuple(value):
            # print(imc)
            if minimc > imc:
                minimc = imc
            if maximc < imc:
                maximc = imc
            count += 1
            buffer += imc

        avg = buffer / count
        imcstats = (avg, minimc, maximc)
        yield imcstats


if __name__ == '__main__':
    mrs.main(WordCount)

