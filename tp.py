# -*- coding: utf-8 -*

import mrs
import glob
import sys
import re


class WordCount(mrs.MapReduce):

    def input_data(self, job):
        repo = sys.argv[1]
        print(repo)
        fileList = glob.glob(repo + '/*.txt')
        print(fileList)
        print('\n ----------------------------------------------------------------- \n')
        return job.file_data(fileList)

    def map(self, key, value):

        print(value.encode('utf-8'))
        enc = value
        print('\n ----------------------------------------------------------------- \n')

        buffer = ''
        for char in enc:
            if char == ' ' or char.isalpha():
                buffer += char

        tab = buffer.split(' ')
        for word in tab:
            if word == '' or word == ' ' or word == ",":
                tab.remove(word)

        print(tab)
        print('\n ----------------------------------------------------------------- \n')
        print(len(tab))
        print('\n ----------------------------------------------------------------- \n')

        for word in tab:
            #print(word)
            yield word, 1

    def reduce(self, key, value):
        # count = len(tuple(value))
        # print(key + ' -> ' + str(count))
        # print(tuple(value))
        yield len(tuple(value))


if __name__ == '__main__':
    mrs.main(WordCount)

