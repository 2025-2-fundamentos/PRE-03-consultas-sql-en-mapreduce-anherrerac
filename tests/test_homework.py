"""Taller evaluable"""

# pylint: disable=broad-exception-raised
# pylint: disable=import-error

from homework.mapreduce import mapreduce  # type: ignore
import shutil
import os
#
# Columns:
# total_bill, tip, sex, smoker, day, time, size
#

#
# QUERY 1:
# SELECT *, tip/total_bill as tip_rate
# FROM tips;
#

def mapper_query_1(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append(
                (index, row.strip() + ",tip_rate")
            )
        else:
            row_values = row.strip().split(",")
            total_bill = float(row_values[0])
            tip = float(row_values[1])
            tip_rate = tip / total_bill
            result.append((index, row.strip() + "," + str(tip_rate)))
    return result


def reducer_query_1(sequence):
    """Reducer"""
    return sequence


#
# SELECT *
# FROM tips
# WHERE time = 'Dinner';
#
def mapper_query_2(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if row_values[5] == "Dinner":
                result.append((index, row.strip()))
    return result


def reducer_query_2(sequence):
    """Reducer"""
    return sequence

#
# SELECT *
# FROM tips
# WHERE time = 'Dinner' AND tip > 5.00;
#
def mapper_query_3(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if row_values[5] == "Dinner" and float(row_values[1]) > 5.00:
                result.append((index, row.strip()))
    return result


def reducer_query_3(sequence):
    """Reducer"""
    return sequence


#
# ORQUESTADOR:
#
def run():
    """Orquestador"""

    if os.path.exists("files/query_1"):
            shutil.rmtree("files/query_1")

    mapreduce(
        input_folder="files/input/", 
        output_folder="files/query_1", 
        mapper_fn=mapper_query_1, 
        reducer_fn=reducer_query_1,
    )


    if os.path.exists("files/query_2"):
        shutil.rmtree("files/query_2")

    mapreduce(
        input_folder="files/input/", 
        output_folder="files/query_2", 
        mapper_fn=mapper_query_2, 
        reducer_fn=reducer_query_2,
    )    

    if os.path.exists("files/query_3"):
        shutil.rmtree("files/query_3")

    mapreduce(
        input_folder="files/input/", 
        output_folder="files/query_3", 
        mapper_fn=mapper_query_3, 
        reducer_fn=reducer_query_3,
    )    


if __name__ == "__main__":

    run()

    
    
    
    