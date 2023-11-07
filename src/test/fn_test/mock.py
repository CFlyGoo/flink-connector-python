import pandas as pd


def mock_pandas(name: str):
    print(name)
    return pd.DataFrame({"name": ['n1', 'n3'], "age": [1, 3]})
