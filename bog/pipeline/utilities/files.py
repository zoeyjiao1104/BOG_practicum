""" Utility file for operations on files """
import json
import os
import pandas as pd

UTILITIES_PATH = os.path.dirname(os.path.abspath(__file__))
PIPELINE_PATH = os.path.dirname(UTILITIES_PATH)
DATA_PATH = os.path.join(PIPELINE_PATH, 'data')


def load_df(filename, delimiter = '\t'):
    """
    Reads in a file from the data folder
    Inputs
        filename: (str) filename to read in. Path to file assumed to
            be path/to/bog-anomaly-mapping/data/filename
        delimiter: (str) delimiter to use when reading in the delimiter
            separated values
    Outputs
        Returns a pandas df of the contents from the file.
     """
    return pd.read_csv(os.path.join(DATA_PATH, filename), sep=delimiter)


def load_json(file_name):
    """
    Loads a JSON file from the data folder.
    """
    path = os.path.join(DATA_PATH, file_name)
    with open(path) as f:
        return json.load(f)


def save_df(dataframe, filename, output_loc=None, delimiter='\t', index=True):
    """
    Saves dataframe in data folder as filename provided.
    Inputs
        dataframe: (pd.DataFrame) df to save
        filename: (str) filename to save to. Path to file assumed to
            be path/to/bog-anomaly-mapping/data/filename unless output_loc
            is supplied.
        output_loc: (str path) directory in which to save the output
        delimiter: (str) delimiter to use when saving the delimiter
            separated values
        index (bool): indicates whether the DataFrame's index should
            remain in the output file.
    Outputs
        Does not return anything. Will save DF to the respective folder.
    """
    if output_loc is None or not os.path.isdir(output_loc):
        output_loc = DATA_PATH
    filepath = os.path.join(output_loc, filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)   
    dataframe.to_csv(filepath, sep=delimiter, index=index)


if __name__ == "__main__":
    print(UTILITIES_PATH)
    print(PIPELINE_PATH) 
    print(DATA_PATH) 