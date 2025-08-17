import pandas as pd
from src.transform import transform

def test_transform_happy_path():
    df = pd.DataFrame({"id": [1,1,2], "value": [10,10,20], "ts": [1,1,2]})
    out = transform(df)
    assert set(out.columns) >= {"id","value","ts","value_x3"}
    assert len(out) == 2  # deduped
    assert out.loc[out['id']==1, 'value_x3'].iloc[0] == 30

def test_transform_missing_column():
    df = pd.DataFrame({"id": [1]})
    try:
        transform(df)
        assert False, "Expected failure"
    except Exception:
        assert True
