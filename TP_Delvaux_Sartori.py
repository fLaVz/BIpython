import mrs
import glob
import re


class WordCount(mrs.MapReduce):

    def input_data(self, job):
        print(self.args[0])
        direc = self.args[0]
        fileList = glob.glob(direc + '/*.txt')
        print(fileList)
        print("----------------------------------")
        return job.file_data(fileList)

    def map(self, key, value):
        # print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        # print(value)
        splited = re.split('[\W0-9]', value, flags=re.UNICODE)
        for i in splited:
            if (i != ''):
                # print(i)
                yield i, 1

    def reduce(self, key, values):
        length = len(tuple(values))
        # print length
        yield length


if __name__ == '__main__':
    mrs.main(WordCount)

# j = mrs.job.Job
# wc = WordCount
# wc.input_data(j)
# wc.map("AAA 2.1 2 , bc2 3z bbb",1)
