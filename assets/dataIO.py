import json
import io
import csv


############################################################
#
#   read_json function
#
#   reads data file json
#
def read_json(inputfile):
	try:
		with open(inputfile) as json_file:
			data_load = json.load(json_file)
		return data_load
	except (OSError, IOError) as e:
		print e
		return {}

############################################################
#
#	write_json function
#
#   writes data into file output_file
#
def write_json(output_file, data):

	# writing to JSON needs to be in unicode
	try:
		to_unicode = unicode
	except NameError:
		to_unicode = str

	# try write except when file DNE
	try:
		with io.open(output_file, 'w', encoding='utf8') as outfile:
			str_ = json.dumps(data,
							  indent=4, sort_keys=True,
							  separators=(',', ': '), ensure_ascii=False)
			outfile.write(to_unicode(str_))
	except (OSError, IOError) as e:
		print e

############################################################
#
#	append_csv function
#
#   appends data into file output_file
#
def append_csv(output_file, data):

	try:
		with open(output_file, 'a') as file:
			writer 	= csv.writer(file)
			writer.writerow(data)
	except (OSError, IOError) as e:
		print e

