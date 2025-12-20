import pandas as pd

def extract_csv(path):
    df = pd.read_csv(path,encoding="latin1")
    return df