import ulmo

import pandas as pd

def test_get_water_district():
    # get division 1 districts
    wddf = ulmo.codwr.get_water_district(1)
    assert(type(wddf) is pd.DataFrame)
    print(wddf)