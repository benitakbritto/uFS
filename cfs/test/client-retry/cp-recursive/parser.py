'''
    @brief: Transform all the output files -> csv
    @prereq: bash
    @usage: python parser.py -input-file <> -output-file <>
'''

import argparse
import csv 

parser = argparse.ArgumentParser()
parser.add_argument(
    "-input-file", 
    type=str, 
    help="Name of the input file(s). Accepts wildcards.",
    nargs='+'
)
parser.add_argument(
    "-output-file", 
    type=str, 
    help="Name of the output file",
)
args = parser.parse_args()

def get_content():
    input_time_dict = {}
    for input_file in args.input_file:
        print(f'input_file = {input_file}')
        time_ns_list = []

        with open(input_file, 'r') as f:
            for index, line in enumerate(f):
                line = line.split()
                if line[0] == 'Execution' and line[1] == 'time':
                    print(f'line = {line}')
                    time_ns_list.append(int(line[3]))
        
        input_time_dict[input_file] = time_ns_list
    
    return input_time_dict

def convert_time_to_seconds(input_time):
    return input_time / 1e9

def write_to_csv(data_dict):
    len_of_each_data_list = len(data_dict[args.input_file[0]])

    with open(args.output_file, 'w', encoding='UTF8') as f:
        writer = csv.writer(f)
        for index in range(len_of_each_data_list):
            row_data = []
            for key in data_dict:
                row_data.append(convert_time_to_seconds(data_dict[key][index]))
            writer.writerow(row_data)

def run_parser():
    data_dict = get_content()
    write_to_csv(data_dict)

if __name__ == "__main__":
    run_parser()
