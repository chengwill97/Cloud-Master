import json
import io

class DataIO:

    def __init__(self):
        return

    ############################################################
    #
    #   reads data file json
    #
    def readData(self, inputfile):
        try:
            with open(inputfile) as json_file:
                data_load = json.load(json_file)
            return data_load
        except (OSError, IOError) as e:
            print e
            return {}

    ############################################################
    #
    #   writes data into file outputfolder+data.json
    #
    def writeData(self, outputfolder, data):

        # writing to JSON needs to be in unicode
        try:
            to_unicode = unicode
        except NameError:
            to_unicode = str

        # try write except when file DNE
        try:
            with io.open(outputfolder+"results.json", 'w', encoding='utf8') as outfile:
                str_ = json.dumps(data,
                                  indent=4, sort_keys=True,
                                  separators=(',', ': '), ensure_ascii=False)
                outfile.write(to_unicode(str_))
        except (OSError, IOError) as e:
            print e